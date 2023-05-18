#!/usr/bin/env python
"""

Code to manually conduct cleanup of undesired prodiucts in individual pulsar directories

__author__ = "Andrew Cameron"
__copyright__ = "Copyright (C) 2022 Andrew Cameron"
__license__ = "Public Domain"
__version__ = "0.1"
__maintainer__ = "Andrew Cameron"
__email__ = "andrewcameron@swin.edu.au"
__status__ = "Development"
"""

# Import packages
import os,sys
import argparse
import glob

# Meerpipe imports
#sys.path.append('/fred/oz005/users/acameron/pipeline_stuff/andrew_meerpipe_dev/meerpipe/')
from meerpipe.initialize import (parse_config, setup_logging)
from archive_utils import (secondary_cleanup)

# Argument parsing
parser = argparse.ArgumentParser(description="Deletes undesired data products from all observations stored under a specific pulsar directory after processing via MeerPIPE. USE WITH EXTREME CAUTION.")
parser.add_argument("-psr_path", dest="psrpath", type=str, help="Processed pulsar directory under which to delete unwanted data products", default=None)
parser.add_argument("-config", dest="config", type=str, help="Path to the config file used in creating the processed results (must contain list of products to delete.", required=True, default=None)
parser.add_argument("-testmode", dest="testmode", action="store_true", help="Disables the script's deletion command for a test run.")
parser.add_argument("-safety_off", dest="nosafety", action="store_true", help="Disables the safety deletion check - script will not ask you to verify your input before deleting files. DANGEROUS - ONLY USE IF YOU ARE ABSOLUTELY SURE OF WHAT YOU ARE DOING")
args = parser.parse_args()

# -- MAIN PROGRAM --

# check valid input
if (args.config == None or not os.path.isfile(args.config)):
    raise Exception("Valid config file not provided - aborting.")
else:
    # ingest config file
    config_params = parse_config(str(args.config))

# check for valid paths
if (args.psrpath == None):
    raise Exception("No psr_path provided - aborting.")
else:
    psrpath = os.path.normpath(args.psrpath)
    split_psrpath = psrpath.split("/")
    if (config_params['output_path'] not in psrpath):
        raise Exception("Config file output path incomptible with psr_path - aborting.")
    elif ("J" not in split_psrpath[len(split_psrpath) - 1]):
        raise Excpetion("psr_path is not a a valid pulsar directory - aborting.")

# we're good to go - begin

# setup logger
logger = setup_logging(
    filedir=psrpath,
    console=True,
)
logger.info("Logger setup")
logger.info ("User:{0}".format(config_params["user"]))

# fetch all subdirectories
obsdirs = sorted(glob.glob(os.path.join(psrpath, "2*/*/*")))

# check
if len(obsdirs) == 0:
    raise Exception("No observation directories identified - aborting.")
else:
    if not (args.nosafety):
        # safety check
        safety_response = input("WARNING: Continuing with this script will delete all redundant files across {} processings. Continue? (Y/N)".format(len(obsdirs)))
        if not safety_response == "Y":
            print ("Program terminated.")
            exit()

# and clean
for obs in obsdirs:

    if not args.testmode:
        logger.info("Now cleaning unwanted files from {0}".format(obs))
        secondary_cleanup(obs,config_params,split_psrpath[len(split_psrpath) - 1],logger)
    else:
        logger.info("Now cleaning unwanted files from {0} (not really)".format(obs))

logger.info("Script complete.")
