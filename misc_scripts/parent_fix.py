#!/usr/bin/env python
"""

Code to fix parent associations in the processings table

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
parser = argparse.ArgumentParser(description="Fix parent associations in the processings table. USE WITH CAUTION.")
parser.add_argument("-infile", dest="infile", type=str, help="List of processing IDs to fix.", required=True)
args = parser.parse_args()


# -- MAIN PROGRAM --

# PSRDB setup
env_query = 'echo $PSRDB_TOKEN'
token = str(subprocess.check_output(env_query, shell=True).decode("utf-8")).rstrip()
env_query = 'echo $PSRDB_URL'
url = str(subprocess.check_output(env_query, shell=True).decode("utf-8")).rstrip()
client = GraphQLClient(url, False)

parent_pipe_id = 1 # PTUSE

# check valid input
if not (os.path.isfile(args.infile)):
    raise Exception("Input file does not exist - aborting.")

# PSRDB setup
processings = Processings(client, url, token)

# read list
proc_list = np.loadtxt(args.infile, dtype=int, ndmin=1)

# scroll list
for x in range(0, len(proc_list)):

    # quick and dirty
    response = processings.list(proc_list[x])
    check_response(response)
    proc_content = json.loads(response.content)
    proc_data = proc_content['data']['processing']

    # check for unique entry
    if not (proc_data == None):

        # get the obs id
        obs_id = processings.decode_id(proc_data['observation']['id'])

        # get the fold id
        fold_id = get_folding_id(obs_id, parent_pipe_id, client, url, token)

        # get the parent_id
        parent_id = get_fold_parent_procid(fold_id, client, url, token)

        # update the processing entry
        update_id = update_processing(
            int(proc_list[x]),
            None,
            None,
            parent_id,
            None,
            None,
            None,
            None,
            None,
            client,
            url,
            token
        )

        if (update_id != proc_list[x]) or (update_id == None):
            print("Failure to update 'processings' entry {0}.".format(str(proc_list[x])))
        else:
            print ("Updated proc {0}".format(proc_list[x]))

    else:
        print ("Invalid processing ID - {0}".format(proc_list[x]))
        continue
