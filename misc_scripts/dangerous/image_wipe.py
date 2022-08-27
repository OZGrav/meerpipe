#!/usr/bin/env python
"""

Code to wipe all images associated with the given list of processing IDs

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
import json
import subprocess
import numpy as np

# PSRDB imports
from tables import *
from graphql_client import GraphQLClient
sys.path.append('/fred/oz005/users/acameron/pipeline_stuff/andrew_meerpipe_dev/meerpipe/')
from db_utils import (check_response, get_folding_id, get_fold_parent_procid, update_processing)

# Important paths
PSRDB = "psrdb.py"

# Argument parsing
parser = argparse.ArgumentParser(description="Wipe all pipelineimage entries associated with a given list of processing IDs. Used in the event of changes to the image creation structure which would cause conflicts with existing entries, which will need to be regenerated. USE WITH EXTREME CAUTION.")
parser.add_argument("-infile", dest="infile", type=str, help="List of processing IDs to fix.", required=True)
parser.add_argument("-testmode", dest="testmode", action="store_true", help="Disables the script's deletion command for a test run.")
args = parser.parse_args()


# -- MAIN PROGRAM --

# PSRDB setup
env_query = 'echo $PSRDB_TOKEN'
token = str(subprocess.check_output(env_query, shell=True).decode("utf-8")).rstrip()
env_query = 'echo $PSRDB_URL'
url = str(subprocess.check_output(env_query, shell=True).decode("utf-8")).rstrip()
client = GraphQLClient(url, False)

# check valid input
if not (os.path.isfile(args.infile)):
    raise Exception("Input file does not exist - aborting.")

# PSRDB setup
pipelineimages = Pipelineimages(client, url, token)

# read list
proc_list = np.loadtxt(args.infile, dtype=int, ndmin=1)

# safety check
safety_response = input("WARNING: Continuing with this script will delete all images for {} processings. Continue? (Y/N)".format(len(proc_list)))
if not safety_response == "Y":
    print ("Program terminated.")
    exit()

# scroll list
count = 0
for x in range(0, len(proc_list)):

    print ("Deleting images for processing ID {0}".format(proc_list[x]))

    # quick and dirty
    response = pipelineimages.list(None, int(proc_list[x]))
    check_response(response)
    pimage_content = json.loads(response.content)
    pimage_data = pimage_content['data']['allPipelineimages']['edges']

    # loop through pipeline entries
    for y in range(0, len(pimage_data)):

        pimage_id = pipelineimages.decode_id(pimage_data[y]['node']['id'])

        # and now delete
        if not (args.testmode):
            pipelineimages.delete(pimage_id)
            print ("... Pipeline image ID {0} deleted.".format(pimage_id))
        else:
            print ("... Pipeline image ID {0} (not really) deleted.".format(pimage_id))
        count += 1


print ("{0} images deleted.".format(count))
