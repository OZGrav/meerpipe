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

#Importing pipeline utilities
from initialize import parse_config, setup_logging

from archive_utils import decimate_data,mitigate_rfi,generate_toas,add_archives,calibrate_data, dynamic_spectra, fluxcalibrate, cleanup, generate_summary

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
