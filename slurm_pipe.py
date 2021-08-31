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

# Important paths
PSRDB = "psrdb.py"

#Importing pipeline utilities
from initialize import parse_config, setup_logging

from archive_utils import decimate_data,mitigate_rfi,generate_toas,add_archives,calibrate_data, dynamic_spectra, fluxcalibrate, cleanup, generate_summary

from db_utils import get_node_name,psrdb_json_formatter,update_psrdb_query,job_state_code,get_slurm_id,get_job_state,job_state_code,list_psrdb_query

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

# update PSRDB entry now that job is running - but check to make sure there's no conflict first
if (config_params["db_proc_id"]):
    pendflag = True
    dest_state = json.loads(job_state_code(1))['job_state']
    while (pendflag):
        # query the job state
        state = get_job_state(config_params["db_proc_id"])
        if (state == dest_state):
            pendflag = False

    job_state = psrdb_json_formatter(job_state_code(2))
    job_id = get_slurm_id(config_params["db_proc_id"])
    node_name = get_node_name()
    job_output = psrdb_json_formatter(json.dumps({"job_id": job_id, "job_node": node_name}))
    
    # recall info from PSRDB and use it to seed the update
    proc_query = "%s -l processings list --id %s" % (PSRDB, config_params["db_proc_id"])
    proc_data = list_psrdb_query(proc_query)

    proc_query = "%s processings update %s %s %s %s %s %s %s %s %s" % (PSRDB, config_params["db_proc_id"], proc_data[1][1], proc_data[1][2], proc_data[1][3], proc_data[1][4], proc_data[1][5], job_state, job_output, proc_data[1][8])
    update_psrdb_query(proc_query)
    logger.info("Updated PSRDB entry in 'processings' table, ID = {0}".format(config_params["db_proc_id"]))

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
