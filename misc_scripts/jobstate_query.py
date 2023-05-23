#!/usr/bin/env python
"""

Code to collate processing entries reporting a certain job status

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
#sys.path.append('/fred/oz005/users/acameron/pipeline_stuff/andrew_meerpipe_dev/meerpipe/')
from db_utils import (check_response, check_pipeline, get_pulsar_id, get_observation_target_id, check_pulsar_target,
                      get_job_state, get_target_name, utc_psrdb2normal, get_observation_utc, job_state_code)

# Important paths
PSRDB = "psrdb.py"

# Argument parsing
parser = argparse.ArgumentParser(description="Reports the job state of all processings matching the specified criteria.")
parser.add_argument("-outdir", dest="outdir", help="Directory in which to store the output file.", default=None)
parser.add_argument("-outfile", dest="outfile", type=str, help="File in which to store the recalled job state results.", default=None)
parser.add_argument("-state", dest="state", type=str, help="Return processings matching this state.", default = None)
parser.add_argument("-allStates", dest="allStates", action="store_true", help="Reports the states of all jobs. Overrides '-state' and requires an output file, output will not be displayed on screen.")

#parser.add_argument("-pipe_id", dest="pipe_id", type=int, help="Return only those processings matching this pipeline ID.", default=None)
#parser.add_argument("-psr", dest="pulsar", type=str, help="Return only those processings matching this PSR J-name.", default=None)
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

# input verification
#if not (args.pipe_id == None):
#    if not(check_pipeline(args.pipe_id, client, url, token)):
#        raise Exception("Invalid pipeline ID specified - aborting.")

#if not (args.pulsar == None):
#    pulsar_id = get_pulsar_id(args.pulsar, client, url, token)
#    if (pulsar_id == None):
#        raise Exception("Pulsar J-name not found in the database - aborting.")

# query and return all processings matching the specified parameters

#processings = Processings(client, url, token)
#processings.set_field_names(True, False)
#processings.get_dicts = True
#processings.set_use_pagination(True)

processedobservations = ProcessedObservations(client, url, token)
# processedobservations.set_field_names(True, False)
processedobservations.get_dicts = True
processedobservations.set_use_pagination(True)

print ("Compiling raw list of processing entries (this will take a few minutes)...")
# proc_data = processings.list(None, None, None, None, None)
proc_data = processedobservations.list(None, None, None, None, None)
print ("Raw data compiled - {0} processing entries found".format(len(proc_data)))

results_list = []

# nothing do to but scroll - make as few PSRDB calls as possible

for x in range(0, len(proc_data)):

    proc_entry = proc_data[x]['node']
    #proc_id = processings.decode_id(proc_entry['id'])
    proc_id = processedobservations.decode_id(proc_entry['id'])
    # can't avoid calling for the decode, hopefully this is quick

    # check if the job state matches
    job_state_json = json.loads(proc_entry['jobState'].replace("'", '"'))
    if "job_state" in job_state_json.keys():
        job_state = job_state_json['job_state']
    else:
        job_state = None

    if not (job_state == None):
        if not (args.allStates) and not (job_state == args.state):
            continue
    else:
        continue

    # check if the pipeline matches
    #if not (args.pipe_id == None):
        #proc_pipe_id = int(processings.decode_id(proc_entry['pipeline']['id']))
        #if not (args.pipe_id == proc_pipe_id):
            #continue

    # check if the pulsar name matches
    #if not (args.pulsar == None or pulsar_id == None):
        #obs_id = int(processings.decode_id(proc_entry['observation']['id']))
        #target_id = get_observation_target_id(obs_id, client, url, token)
        #if not (check_pulsar_target(pulsar_id, target_id, client, url, token)):
            #continue

    # if we have survived this far, we have a match
    # collect the output info and add to the results list
    #target_name = get_target_name(target_id, client, url, token)
    #obs_utc = utc_psrdb2normal(get_observation_utc(obs_id, client, url, token))
    #results_list.append([proc_id, target_name, obs_id, obs_utc, job_state])

    target_name = proc_entry['observation']['target']['name']
    obs_utc = utc_psrdb2normal(proc_entry['observation']['utcStart'])
    pipe_name = proc_entry['pipeline']['name']
    results_list.append([proc_id, pipe_name, target_name, obs_utc, job_state])

    #results_list.append([proc_id, job_state])

# write the results to file, if there are any

if (len(results_list) > 0):

    #header = "# ProcID Target ObsID ObsUTC JobState"
    #header = "# ProcID JobState"
    header = "# ProcID PipeName Target ObsUTC JobState"
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
                #outfile.write("{0}\t{1}\t{2}\t{3}\t{4}\n".format(arr[x][0], arr[x][1], arr[x][2], arr[x][3], arr[x][4]))
                #outfile.write("{0}\t{1}\n".format(arr[x][0], arr[x][1]))
                outfile.write("{0}\t{1}\t{2}\t{3}\t{4}\n".format(arr[x][0], arr[x][1], arr[x][2], arr[x][3], arr[x][4]))
            outfile.close()
            print("{0} matching processing entries written to {1}.".format(len(arr), outpath))
        else:
            print (header)
            for x in range(0, len(arr)):
                #print ("{0}\t{1}\t{2}\t{3}\t{4}".format(arr[x][0], arr[x][1], arr[x][2], arr[x][3], arr[x][4]))
                #print ("{0}\t{1}".format(arr[x][0], arr[x][1]))
                print ("{0}\t{1}\t{2}\t{3}\t{4}".format(arr[x][0], arr[x][1], arr[x][2], arr[x][3], arr[x][4]))
            print("{0} matching processing entries found.".format(len(arr)))

    else:

        print ("Writing each collection of job states to an individual file...")

        job_code = 0
        while not (job_state_code(job_code) == None):

            ref_job_state = job_state_code(job_code)['job_state']

            # set up the filename
            outname = "{0}_{1}".format(ref_job_state.replace(" ", "_"), args.outfile)
            if not (args.outdir == None):
                outpath = os.path.join(args.outdir, outname)
            else:
                outpath = outname

            outfile = open(outpath, "w")
            outfile.write("{0}\n".format(header))

            # check through the array
            count = 0
            for x in range(0, len(arr)):
                if (ref_job_state == arr[x][4]):
                    outfile.write("{0}\t{1}\t{2}\t{3}\t{4}\n".format(arr[x][0], arr[x][1], arr[x][2], arr[x][3], arr[x][4]))
                    count += 1

            outfile.close()
            print ("{0} written with {1} entries...".format(outpath, count))

            job_code += 1

else:

    print ("No processing entries found matching the specified criteria - please try again.")
