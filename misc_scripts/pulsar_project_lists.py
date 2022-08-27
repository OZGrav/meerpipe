#!/usr/bin/env python
"""

Code to compile lists of pulsars specific to each major project code as a starting point for large-scale job execution

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
from db_utils import check_response

# Important paths
PSRDB = "psrdb.py"

# Argument parsing
parser = argparse.ArgumentParser(description="Compile lists of pulsars specific to each PSRDB-defined project code")
parser.add_argument("-outdir", dest="outdir", help="Directory to store lists in.", required=True)
parser.add_argument("-outname", dest="outname", help="Root name for each list file.")
args = parser.parse_args()


# -- FUNCTIONS --

# ROLE   : Writes a 1D list to file
# INPUTS : File handle | array
# RETURNS: Nothing
def write_list(fh, arr, client, url, token):

    for x in range(0, len(arr)):

        fh.write("{0}\n".format(arr[x]))

    return

# -- MAIN PROGRAM --

# PSRDB setup
env_query = 'echo $PSRDB_TOKEN'
token = str(subprocess.check_output(env_query, shell=True).decode("utf-8")).rstrip()
env_query = 'echo $PSRDB_URL'
url = str(subprocess.check_output(env_query, shell=True).decode("utf-8")).rstrip()
client = GraphQLClient(url, False)

# check the outpath
if not (os.path.isdir(args.outdir)):
    os.makedirs(args.outdir)

# get the list of projects from PSRDB

projects = Projects(client, url, token)
response = projects.list()
check_response(response)
project_content = json.loads(response.content)
project_data = project_content['data']['allProjects']['edges']

print ("Compiling project code list")

project_codes = []
for x in range(0, len(project_data)):
    project_codes.append(project_data[x]['node']['code'])

print ("Project code list compiled - {0} entries".format(len(project_codes)))

# for each project code, query the list of observations and extract the pulsar names
observations = Observations(client, url, token)
observations.get_dicts = True
observations.set_use_pagination(True)

for x in range (0, len(project_codes)):

    print ("Collecting list of pulsars for project code {0}...".format(project_codes[x]))

    data = observations.list(None, None, None, None, None, None, project_codes[x], None, None, None, None)

    print ("Raw data compiled.")

    psr_list_raw = []

    # scroll through and collate pulsar names
    for y in range(0, len(data)):

        name = data[y]['node']['target']['name']
        #psr_name = get_pulsarname(name, client, url, token)
        psr_list_raw.append(name)

    print ("Raw pulsar list compiled - {0} entries.".format(len(psr_list_raw)))

    # remove duplicates from the list of names
    psr_list_cleaned = list(dict.fromkeys(psr_list_raw))

    print ("Unique pulsar list compiled - {0} entries".format(len(psr_list_cleaned)))

    # write the list to a file
    if (args.outname):
        outname = "{0}_{1}.list".format(args.outname, project_codes[x])
    else:
        outname = "{0}.list".format(project_codes[x])

    outpath = os.path.join(args.outdir, outname)

    outfile = open(outpath, "w")
    write_list(outfile, psr_list_cleaned, client, url, token)
    outfile.close()
    print("List of pulsars in project {0} written to {1}.".format(project_codes[x], outpath))
