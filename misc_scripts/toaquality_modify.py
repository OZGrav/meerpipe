#!/usr/bin/env python
"""

Code to modify a given TOA quality field

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
from psrdb.tables import *
from psrdb.joins import *
from psrdb.graphql_client import GraphQLClient
#sys.path.append('/fred/oz005/users/acameron/pipeline_stuff/andrew_meerpipe_dev/meerpipe/')
from meerpipe.db_utils import check_response, update_toa_record

# Important paths
PSRDB = "psrdb.py"

# Argument parsing
parser = argparse.ArgumentParser(description="Modifies TOA quality fields to the specified status, based on the queryable parameters below.")
parser.add_argument("-toa_id", dest="toa_id", type=int, help="ID of a single TOA entry.", default=None)
parser.add_argument("-proc_id", dest="proc_id", type=int, help="ID of a single processing entry - all TOAs associated with this value will be modified (i.e. only one).", default=None)
parser.add_argument("-fold_id", dest="fold_id", type=int, help="ID of a single folding entry - all TOAs associated with this value will be modified (i.e. one per pipeline).", default=None)
parser.add_argument("-state", dest="state", type=str, help="State to set the TOA entries to. Select either 'NOMINAL' or 'BAD'.")
args = parser.parse_args()


# -- MAIN PROGRAM --

# PSRDB setup
env_query = 'echo $PSRDB_TOKEN'
token = str(subprocess.check_output(env_query, shell=True).decode("utf-8")).rstrip()
env_query = 'echo $PSRDB_URL'
url = str(subprocess.check_output(env_query, shell=True).decode("utf-8")).rstrip()
client = GraphQLClient(url, False)

# Input checks

# Check state
if not (args.state == "NOMINAL" or args.state == "BAD"):
    raise Exception ("WARNING: '-state' flag should be set only to 'NOMINAL' or 'BAD'.")

# Check IDs
if (args.toa_id == None and args.proc_id == None and args.fold_id == None):
    raise Exception ("WARNING: At least one query ID value must be provided, otherwise script will modify all TOAs. Code aborting to prevent modifying every entry in the entire database.")

# Make query and assess results
toas = Toas(client, url, token)
print ("Compiling TOA entries matching: TOA_ID = {0} | PROC_ID = {1} | FOLD_ID = {2}".format(args.toa_id, args.proc_id, args.fold_id))
response = toas.list(
    args.toa_id,
    args.proc_id,
    args.fold_id,
    None,
    None
)
check_response(response)
toa_content = json.loads(response.content)

# format output based on initial query parameters
if (args.toa_id == None):
    toa_data = toa_content['data']['allToas']['edges']
else:
    toa_inter = {}
    toa_inter['node'] = toa_content['data']['toa']
    toa_data = [toa_inter]

print ("Query complete - {0} TOA entries found".format(len(toa_data)))

# scroll through entries and modify quality status as required
for x in range(0, len(toa_data)):

    # isolate the entry
    toa_entry = toa_data[x]['node']

    # grab the entry's TOA ID
    toa_entry_id = int(toas.decode_id(toa_entry['id']))

    # sanitise the quality to be modified
    quality = args.state.lower()

    # update the entry
    update_response = update_toa_record(
        toa_entry_id,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        quality,
        None,
        client,
        url,
        token
    )

    if (update_response == None):
        print ("Update of TOA entry ID {0} failed - please investigate.".format(toa_entry_id))
    elif (int(update_response) == toa_entry_id):
        print ("Update of TOA entry ID {0} successful.".format(toa_entry_id))
    else:
        print ("Serious unknown error has occured with TOA entry ID {0} - please investigate.".format(toa_entry_id))
