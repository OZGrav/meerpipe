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
import argparse
import os.path
import numpy as np
import pickle

# PSRDB paths
PSRDB = "psrdb.py"

#Importing pipeline utilities
from initialize import setup_logging

from archive_utils import (decimate_data, mitigate_rfi, generate_toas, add_archives, calibrate_data,
                           dynamic_spectra, fluxcalibrate, cleanup, generate_summary, check_summary,
                           generate_images, secondary_cleanup, folding_resync)

# PSRDB imports
from tables import *
from graphql_client import GraphQLClient
from db_utils import (get_node_name, job_state_code, get_job_output, get_job_state, job_state_code,
                      update_processing)

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

logger = setup_logging(
    filedir=config_params["output_path"],
    console=True,
)

#####

# Update PSRDB entry now that the job is running
# Check to make sure there is no write-access conflict first
if (config_params["db_flag"]):

    # testing
    #logger.info("URL = {0}".format(config_params["db_url"]))
    #logger.info("Token = {0}".format(config_params["db_token"]))
    #logger.info("Proc ID = {0}".format(config_params["db_proc_id"]))

    # PSRDB client setup
    db_client = GraphQLClient(config_params["db_url"], False)

    # testing
    #logger.info("DB Client initialised.")

    # Wait for run_pipe.py to finish
    pendflag = True
    dest_state = job_state_code(1)
    while (pendflag):

        # testing
        #logger.info("About to query job state")

        state = get_job_state(
            config_params["db_proc_id"],
            db_client,
            config_params["db_url"],
            config_params["db_token"]
        )

        # testing
        #logger.info("Query complete")

        if (state == dest_state):
            pendflag = False

    # testing
    #logger.info("Pendflag loop escaped")

    # run_pipe.py has finished - prepare update parameters
    job_state = job_state_code(2)
    job_output = get_job_output(
        config_params["db_proc_id"],
        db_client,
        config_params["db_url"],
        config_params["db_token"]
    )
    node_name = get_node_name()
    job_output['job_node'] = node_name

    # testing
    # logger.info("About to update job state")

    # Complete the update and check for success
    update_id = update_processing(
        config_params["db_proc_id"],
        None,
        None,
        None,
        None,
        None,
        job_state,
        job_output,
        None,
        db_client,
        config_params["db_url"],
        config_params["db_token"]
    )
    if (update_id != config_params["db_proc_id"]) or (update_id == None):
        logger.error("Failure to update 'processings' entry ID {0} - PSRDB cleanup may be required.".format(config_params["db_proc_id"]))
    else:
        logger.info("Updated PSRDB entry in 'processings' table, ID = {0}".format(config_params["db_proc_id"]))

#####

# Each of the following functions may make its own modifications to the relevant PSRDB processings entry, or may create linked entries in other tables

#####

# bracket the pipeline code with an error-catch statement to check for success / failure
crash = False

try:

        # separate the code to be ignored if we want images only
    if not (config_params["image_flag"]):

        #Add the archive files per observation directory into a single file
        added_archives = add_archives(archive_list,output_dir,config_params,psrname,logger)
        logger.info("PIPE - Added archives: {0}".format(added_archives))

        if not config_params["fluxcal"]:
            #Calibration
            calibrated_archives = calibrate_data(added_archives,output_dir,config_params,logger)
            logger.info("PIPE - Calibrated archives: {0}".format(calibrated_archives))

        if not config_params["fluxcal"]:
            #RFI zapping using coastguard on calibrated archives
            cleaned_archives = mitigate_rfi(calibrated_archives,output_dir,config_params,psrname,logger)
            logger.info("PIPE - Cleaned archives: {0}".format(cleaned_archives))
        elif config_params["fluxcal"]:
            #RFI zapping using coastguard on added archives
            cleaned_archives = mitigate_rfi(added_archives,output_dir,config_params,psrname,logger)
            logger.info("PIPE - Cleaned archives: {0}".format(cleaned_archives))

        if not config_params["fluxcal"]:
            #Checking flags and creating appropriate data products
            processed_archives = decimate_data(cleaned_archives,output_dir,config_params,logger)
            #logger.info("Processed archives: {0}".format(processed_archives))
            logger.info("PIPE - Data decimation complete.")

            #Generating dynamic spectra from calibrated archives
            dynamic_spectra(output_dir,config_params,psrname,logger)
            logger.info("PIPE - Dynamic spectra complete.")

            #Flux calibrating the decimated products
            fluxcalibrate(output_dir,config_params,psrname,logger)
            logger.info("PIPE - Flux calibration complete.")

            #Cleaning
            cleanup(output_dir,config_params,psrname,logger)
            logger.info("PIPE - First-stage cleanup complete.")

            #Forming ToAs from the processed archives
            generate_toas(output_dir,config_params,psrname,logger)
            logger.info("PIPE - TOA generation complete.")

            #Generate summary
            generate_summary(output_dir,config_params,psrname,logger)
            logger.info("PIPE - Summary generation complete.")

    # code to be run in all cases
    if not config_params["fluxcal"]:

        # Produce images
        generate_images(output_dir,config_params,psrname,logger)
        logger.info("PIPE - Image generation complete.")

        # Secondary cleanup
        secondary_cleanup(output_dir,config_params,psrname,logger)
        logger.info("PIPE - Second-stage cleanup complete.")

        # Trigger PSRDB resync
        folding_resync(config_params,logger)
        logger.info("PIPE - Folding resync complete.")

        logger.info ("##############")

except Exception as e:
    crash = True
    logger.error("PIPELINE CRASH DETECTED")
    logger.error(e)

#####

# Check for success condition to finalise PSRDB Processings entry

if (config_params["db_flag"]):

    if (config_params["fluxcal"]):

        # TODO: COMPLETE THIS LATER IF REQUIRED
        job_state = job_state_code(5)

    elif not (config_params["fluxcal"]):

        # Check summary file for pass/fail status, in combination with crash status
        if (not crash):
            if (check_summary(output_dir, logger)):
                job_state = job_state_code(3)
            else:
                job_state = job_state_code(4)
        else:
            job_state = job_state_code(6)

    # Update and check for success
    update_id = update_processing(
        config_params["db_proc_id"],
        None,
        None,
        None,
        None,
        None,
        job_state,
        None,
        None,
        db_client,
        config_params["db_url"],
        config_params["db_token"]
    )
    if (update_id != config_params["db_proc_id"]) or (update_id == None):
        logger.error("Failure to update 'processings' entry ID {0} - PSRDB cleanup may be required.".format(config_params["db_proc_id"]))
    else:
        logger.info("Updated PSRDB entry in 'processings' table with final job state, ID = {0}".format(config_params["db_proc_id"]))
