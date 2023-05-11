#!/usr/bin/env python
"""

Code to collate toa entries reporting a certain toa quality status

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
from joins import *
from graphql_client import GraphQLClient
sys.path.append('/fred/oz005/users/acameron/pipeline_stuff/andrew_meerpipe_dev/meerpipe/')
from db_utils import check_response

# Important paths
PSRDB = "psrdb.py"

# Argument parsing
parser = argparse.ArgumentParser(description="Reports the TOA quality state of all TOA entries matching the specified criteria.")
parser.add_argument("-outdir", dest="outdir", help="Directory in which to store the output file.", default=None)
parser.add_argument("-outfile", dest="outfile", type=str, help="File in which to store the recalled TOA quality results.", default=None)
parser.add_argument("-state", dest="state", type=str, help="Return TOAs matching this quality state.", default = None)
parser.add_argument("-allStates", dest="allStates", action="store_true", help="Reports the quality of all TOAs. Overrides '-state' and requires an output file, output will not be displayed on screen.")
args = parser.parse_args()


# -- MAIN PROGRAM --

# PSRDB setup
env_query = 'echo $PSRDB_TOKEN'
token = str(subprocess.check_output(env_query, shell=True).decode("utf-8")).rstrip()
env_query = 'echo $PSRDB_URL'
url = str(subprocess.check_output(env_query, shell=True).decode("utf-8")).rstrip()
client = GraphQLClient(url, False)

# check the outpath
if not (args.outdir == None or args.outfile == None):
    if not (os.path.isdir(args.outdir)):
        os.makedirs(args.outdir)

# check for allState requirements
if (args.allStates):
    if (args.outfile == None):
        raise Exception("Cannot run with -allStates enabled without providing an output file - aborting...")
elif (args.state == None):
    raise Exception("Must select either -allStates or -state when running this script - aborting...")

toas = Toas(client, url, token)
toas.get_dicts = True
toas.set_use_pagination(True)

print ("Compiling raw list of TOA entries (this will take a few minutes)...")
toa_data = toas.list(None, None, None, None, None)
print ("Raw data compiled - {0} TOA entries found".format(len(toa_data)))

results_list = []

# nothing do to but scroll - make as few PSRDB calls as possible

for x in range(0, len(toa_data)):

    toa_entry = toa_data[x]['node']

    # check if the quality field matches
    quality = toa_entry['quality']

    if not (args.allStates) and not (job_state == args.state):
        continue

    toa_id = toas.decode_id(toa_entry['id'])
    proc_id = toas.decode_id(toa_entry['processing']['id'])
    fold_id = toas.decode_id(toa_entry['inputFolding']['id'])
    mjd = toa_entry['mjd']
    comment = toa_entry['comment']
    results_list.append([toa_id, proc_id, fold_id, mjd, quality, comment])

# write the results to file, if there are any

if (len(results_list) > 0):

    header = "# TOA_ID Proc_ID Fold_ID MJD Quality Comment"
    arr = results_list

    # check file or screen output
    if not (args.allStates):

        if not (args.outfile == None):
            if not (args.outdir == None):
                outpath = os.path.join(args.outdir, args.outfile)
            else:
                outpath = args.outfile

            outfile = open(outpath, "w")
            outfile.write("{0}\n".format(header))
            for x in range(0, len(arr)):
                outfile.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\n".format(arr[x][0], arr[x][1], arr[x][2], arr[x][3], arr[x][4], arr[x][5]))
            outfile.close()
            print("{0} matching TOA entries written to {1}.".format(len(arr), outpath))
        else:
            print (header)
            for x in range(0, len(arr)):
                print ("{0}\t{1}\t{2}\t{3}\t{4}\t{5}".format(arr[x][0], arr[x][1], arr[x][2], arr[x][3], arr[x][4], arr[x][5]))
            print("{0} matching TOA entries found.".format(len(arr)))

    else:

        print ("Writing each collection of TOA quality states to an individual file...")

        quality_states = ["NOMINAL", "BAD"]
        for qual in quality_states:

            # set up the filename
            outname = "{0}_{1}".format(qual, args.outfile)
            if not (args.outdir == None):
                outpath = os.path.join(args.outdir, outname)
            else:
                outpath = outname

            outfile = open(outpath, "w")
            outfile.write("{0}\n".format(header))

            # check through the array
            count = 0
            for x in range(0, len(arr)):
                if (qual == arr[x][4]):
                    outfile.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\n".format(arr[x][0], arr[x][1], arr[x][2], arr[x][3], arr[x][4], arr[x][5]))
                    count += 1

            outfile.close()
            print ("{0} written with {1} entries...".format(outpath, count))

else:

    print ("No TOA entries found matching the specified criteria - please try again.")
