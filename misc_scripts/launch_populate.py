#!/usr/bin/env python
"""

Code to populate the launch table given a list of pulsars to be assigned to a pipeline

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
from psrdb.graphql_client import GraphQLClient
#sys.path.append('/fred/oz005/users/acameron/pipeline_stuff/andrew_meerpipe_dev/meerpipe/')
from meerpipe.db_utils import (check_response, get_pulsar_id, check_pipeline, create_launch)

# Important paths
PSRDB = "psrdb.py"

# Argument parsing
parser = argparse.ArgumentParser(description="Specify a collection of pulsars to the PSRDB 'launches' table to be assigned to a specific pipeline")
parser.add_argument("-infile", dest="infile", help="Input file containing the list of pulsars.", required=True)
parser.add_argument("-pipe_id", dest="pipe_id", help="PSRDB ID of the pipeline the pulsars are to be assigned to.", required=True)
parser.add_argument("-parent_id", dest="parent_id", help="PSRDB ID of the *parent* pipeline (as required by the launches table).", required=True)
args = parser.parse_args()

# -- FUNCTIONS --

# nobody here but us chickens

# -- MAIN PROGRAM --

# PSRDB setup
env_query = 'echo $PSRDB_TOKEN'
token = str(subprocess.check_output(env_query, shell=True).decode("utf-8")).rstrip()
env_query = 'echo $PSRDB_URL'
url = str(subprocess.check_output(env_query, shell=True).decode("utf-8")).rstrip()
client = GraphQLClient(url, False)

# check for valid input
if not(check_pipeline(args.pipe_id, client, url, token)):
    raise Exception("Invalid pipeline ID specified - aborting.")
if not(check_pipeline(args.parent_id, client, url, token)):
    raise Exception("Invalid parent pipeline ID specified - aborting.")
if not (os.path.isfile(args.infile)):
    raise Exception("Input file does not exist - aborting.")


# ingest the file and begin scanning through
pulsar_list = np.loadtxt(args.infile, dtype="str")

for x in range(0, len(pulsar_list)):

    print ("Now checking {0}...".format(str(pulsar_list[x])))

    # get the pulsar_id and check that it exists
    pulsar_id = get_pulsar_id(str(pulsar_list[x]), client, url, token)
    if (pulsar_id == None):
        print ("Could not find {0} in the database - skipping...".format(pulsar_list[x]))
        continue

    # create the launch entry
    retval = create_launch(args.pipe_id, args.parent_id, pulsar_id, client, url, token)
    if not (retval == None):
        print ("{0} assigned to pipeline {1} - Launch ID {2}.".format(pulsar_list[x], args.pipe_id, retval))
