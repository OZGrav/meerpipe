#!/usr/bin/env python

"""
Code that runs the pipeline with the Slurm scheduler

__author__ = "Aditya Parthasarathy"
__copyright__ = "Copyright (C) 2018 Aditya Parthasarathy"
__license__ = "Public Domain"
__version__ = "0.1"
Modified version of MeerPipe
"""

#Basic imports
import os
import sys
import subprocess
import shlex
import argparse
import os.path
import numpy as np
import logging
import glob
import pickle
import json

# PSRDB paths 
PSRDB = "psrdb.py"

#Importing pipeline utilities
from initialize import parse_config, setup_logging

from archive_utils import decimate_data,mitigate_rfi,generate_toas,add_archives,calibrate_data, dynamic_spectra, fluxcalibrate, cleanup, generate_summary, check_summary

# PSRDB imports
from tables import *
from graphql_client import GraphQLClient
from db_utils import get_node_name,job_state_code,get_slurm_id,get_job_state,job_state_code, update_processing

#Argument parsing
parser = argparse.ArgumentParser(description="Run the MeerTime pipeline")
parser.add_argument("-obsname", dest="obsname", help="Process a specified observation")
parser.add_argument("-outputdir", dest="outputdir", help="Output directory for the pipeline")
parser.add_argument("-psrname", dest="psrname", help="Psrname")
args = parser.parse_args()

archive_list = list(np.load(str(args.obsname)))
output_dir = np.load(str(args.outputdir))
psrname = np.load(str(args.psrname))

with open(os.path.join(str(output_dir),"config_params.p"), 'rb') as fp:
    config_params = pickle.load(fp)
fp.close()

logger=setup_logging(config_params["output_path"],True,False)

#####

# update PSRDB entry now that job is running - but check to make sure there's no conflict first
if (config_params["db_flag"]):

    # set up the db client and required parameters

    db_client = GraphQLClient(config_params["db_url"], False)

    # now wait for run_pipe to finish, and then collect required info

    pendflag = True
    dest_state = json.loads(job_state_code(1))['job_state']
    while (pendflag):
        # query the job state
        state = get_job_state(config_params["db_proc_id"], db_client, config_params["db_url"], config_params["db_token"])
        if (state == dest_state):
            pendflag = False

    job_state = job_state_code(2)
    job_id = get_slurm_id(config_params["db_proc_id"], db_client, config_params["db_url"], config_params["db_token"])
    node_name = get_node_name()
    job_output = json.dumps({"job_id": job_id, "job_node": node_name})
    
    # now update job_state and job_output
    update_id = update_processing(config_params["db_proc_id"], None, None, None, None, None, job_state, job_output, None, db_client, config_params["db_url"], config_params["db_token"])
    if (str(update_id) != str(config_params["db_proc_id"])) or (update_id == None):
        logger.error("Failure to update 'processings' entry ID {0} - PSRDB cleanup may be required.".format(config_params["db_proc_id"]))
    else:
        logger.info("Updated PSRDB entry in 'processings' table, ID = {0}".format(config_params["db_proc_id"]))

#####

# Each of the following functions may make its own modifications to the relevant PSRDB processings entry, or may create linked entries in other tables

#####

#Add the archive files per observation directory into a single file
added_archives = add_archives(archive_list,output_dir,config_params,psrname,logger)
logger.info("Added archives: {0}".format(added_archives))

if not config_params["fluxcal"]:
    #Calibration
    calibrated_archives = calibrate_data(added_archives,output_dir,config_params,logger)
    logger.info("Calibrated archives: {0}".format(calibrated_archives))

if not config_params["fluxcal"]:
    #RFI zapping using coastguard on calibrated archives
    cleaned_archives = mitigate_rfi(calibrated_archives,output_dir,config_params,psrname,logger)
    logger.info("Cleaned archives: {0}".format(cleaned_archives))
elif config_params["fluxcal"]:
    #RFI zapping using coastguard on added archives
    cleaned_archives = mitigate_rfi(added_archives,output_dir,config_params,psrname,logger)
    logger.info("Cleaned archives: {0}".format(cleaned_archives))

if not config_params["fluxcal"]:
    #Checking flags and creating appropriate data products
    processed_archives = decimate_data(cleaned_archives,output_dir,config_params,logger)
    #logger.info("Processed archives: {0}".format(processed_archives))

    #Generating dynamic spectra from calibrated archives
    dynamic_spectra(output_dir,config_params,psrname,logger)

    #Flux calibrating the decimated products
    fluxcalibrate(output_dir,config_params,psrname,logger)

    #Cleaning
    cleanup(output_dir,config_params,psrname,logger)

    #Forming ToAs from the processed archives
    generate_toas(output_dir,config_params,psrname,logger)

    #Generate summary
    generate_summary(output_dir,config_params,psrname,logger)

    
    logger.info ("##############")

#####

# check for success condition to finalise the entry in the PSRDB processings table

if (config_params["db_flag"]):

    # specify custom success conditions for various pipeline states

    if (config_params["fluxcal"]):

        # TODO: COMPLETE THIS LATER IF REQUIRED
        job_state = job_state_code(5)

    elif not (config_params["fluxcal"]):

        # check the generated summary file for pass/fail status
        if (check_summary(output_dir, logger)):
            job_state = job_state_code(3)
        else:
            job_state = job_state_code(4)

    # and now update the entry in processings
    update_id = update_processing(config_params["db_proc_id"], None, None, None, None, None, job_state, None, None, db_client, config_params["db_url"], config_params["db_token"])
    if (str(update_id) != str(proc_id)) or (update_id == None):
        logger.error("Failure to update 'processings' entry ID {0} - PSRDB cleanup may be required.".format(config_params["db_proc_id"]))
    else:
        logger.info("Updated PSRDB entry in 'processings' table with final job state, ID = {0}".format(config_params["db_proc_id"]))
