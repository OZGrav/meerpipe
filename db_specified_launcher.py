#!/usr/bin/env python

# Andrew Cameron, May 2021
# Python wrapper script to launch MeerPipe using PSRDB functionality
# PSRDB written and maintained by Andrew Jameson & Stefan Oslowski
# Intention is to mimic the function of query_obs.py & reprocessing.py but in a single script

# Basic Imports
import os,sys
import argparse
import datetime
import subprocess
import shlex
import json
import time

# Custom Imports
from db_utils import utc_normal2psrdb,utc_psrdb2normal,utc_normal2date,utc_psrdb2date,pid_getofficial,pid_getshort,list_psrdb_query,write_obs_list,get_pulsar_id

from graphql_client import GraphQLClient

# Important paths
PSRDB = "psrdb.py"

# Parse incoming arguments
parser = argparse.ArgumentParser(description="Launches specific observations to be processed by MeerPipe. Provide either a set of searchable parameters (primary input) or a list of observations (secondary input). If both inputs are provided, only the primary input will be used.")
parser.add_argument("-utc1", dest="utc1", help="(Primary: required) - Start UTC for DB search - return only observations after this UTC timestamp.")
parser.add_argument("-utc2", dest="utc2", help="(Primary: required) - End UTC for DB search - return only observations before this UTC timestamp.")
parser.add_argument("-psr", dest="pulsar", help="(Primary: optional) - Pulsar name for DB search - return only observations with this pulsar name.")
parser.add_argument("-pid", dest="pid", help="(Primary: optional) - Project ID for DB search - return only observations matching this Project ID.")
parser.add_argument("-list_out", dest="list_out", help="(Primary: optional) - Output file name to write the list of observations submitted by this particular search. Does not work in secondary mode as it would simply duplicate the input list.")
parser.add_argument("-list_in", dest="list_in", help="(Secondary: required) - List of observations to process, given in standard format. These will be crossmatched against the DB before job submission.")
parser.add_argument("-runas", dest="runas", help="(Optional) - Specify an override PID to use in processing the observations. Other options: 'PIPE' - use pipeline PID (default); 'OBS' - use observation PID.")
args = parser.parse_args()

# for a given array of psrdb observations and argpase arguments, determine which pipelines should be launched for each entry and then launch them
def array_launcher(arr, ag):

    # get the index of the psr name, pid, observation UTC and file location
    psr_index = arr[0].index("processing_observation_target_name")
    pid_index = arr[0].index("processing_observation_project_code")
    utc_index = arr[0].index("processing_observation_utcStart")
    obs_index = arr[0].index("processing_location")

    # set up PSRDB (temp fix until we can rewrite this!)
    env_query = 'echo $PSRDB_TOKEN'
    token = str(subprocess.check_output(env_query, shell=True).decode("utf-8")).rstrip()
    env_query = 'echo $PSRDB_URL'
    url = str(subprocess.check_output(env_query, shell=True).decode("utf-8")).rstrip()

    client = GraphQLClient(url, False)

    # begin scrolling through the array
    for x in range (1, len(arr)):

        # get the pulsar name, observation UTC and file location
        psr_name = arr[x][psr_index]
        utc = utc_psrdb2normal(arr[x][utc_index])
        obs_location = arr[x][obs_index]
        # get the pulsar id
        psr_id = get_pulsar_id(psr_name, client, url, token)
        
        # now lookup which pipelines have to be run from the launches table
        launch_query = "%s -l launches list --pulsar_id %s" % (PSRDB, psr_id)
        launch_data = list_psrdb_query(launch_query) 

        # get the index of the pipeline name and id
        pipe_id_index = launch_data[0].index("pipeline_id")
        
        # now we need to run through the array of launch data and launch every pipeline for this pulsar
        for y in range (1, len(launch_data)):
            
            # query the pipeline name from the pipelines table
            pipe_id = launch_data[y][pipe_id_index]
            pipe_query = "%s pipelines list --id %s" % (PSRDB, pipe_id)
            pipe_data = list_psrdb_query(pipe_query)

            # only one pipeline should have been recalled - check this and raise exception if failure
            if (len(pipe_data) != 2):
                raise Exception("Pipeline launch query on PSR %s returned multiple pipelines with the same ID (%s). Please check PSRDB for internal consistency." % (psr_name, pipe_id))

            # assuming success, extract the configuration data
            config_index = pipe_data[0].index("configuration")
            config_data = pipe_data[1][config_index]
            # convert the config data into a JSON object
            config_json = json.loads(config_data.replace("'", '"'))
            
            # NOW WE HAVE TO RUN SOME SANITY CHECKS
            # 1. Check that the config file will look for observations in the PSRDB location
            # get the input_path from the config file
            in_path_query = "grep input_path %s" % (config_json['config'])
            proc_query = shlex.split(in_path_query)
            proc = subprocess.Popen(proc_query, stdout=subprocess.PIPE)
            out = proc.stdout.read().decode("utf-8")
            linearray = out.split("\n")[0].split(" ")
            # check that the result matches expectations
            if (len(linearray) != 3):
                raise Exception("Config file (%s) parameter 'input_path' is not the correct length." % (config_json['config']))
                
            in_path = linearray[2]

            # check if the paths are self-consistent
            if (not ((in_path in obs_location) or (obs_location in in_path))):
                raise Exception("Config file (%s) parameter 'input_path' is inconsistent with database file locations." % (config_json['config']))

            # 2. Check for PID override
            if (ag.runas):
                # if runas is specified
                if (ag.runas == "OBS"):
                    launch_pid = pid_getshort(arr[x][pid_index])
                elif (ag.runas == "PIPE"):
                    launch_pid = pid_getshort(config_json['pid'])
                else:
                    launch_pid = ag.runas
            else:
                # default option
                launch_pid = pid_getshort(config_json['pid'])
                    
            # NOW LAUNCH THE DAMN THING
            pipeline_launch_instruction = "%s -cfile %s -dirname %s -utc %s -verbose -pid %s -slurm -db -db_pipe %s" % (config_json['path'], config_json['config'], psr_name, utc, launch_pid, pipe_id)
            #pipeline_launch_instruction = "%s -cfile %s -dirname %s -utc %s -verbose -pid %s -db -db_pipe %s" % (config_json['path'], config_json['config'], psr_name, utc, launch_pid, pipe_id)
            proc_query = shlex.split(pipeline_launch_instruction)
            proc = subprocess.Popen(proc_query)
            # wait until jobs are finished launching before returning the command line
            proc.wait()

    return

# TODO LIST
# Check for which arguments got submitted and if they meet the minimum amount required to continue
# If primary:
#     * processes the arguments into the correct format
#     * use them to search the database
#     * write the entries to an output list (if specified)
#     * for each entry, query which pipelines it is to be run through
#     * check that the filepath from the DB matches that given in the config file
#     * run the job using run_pipe.py
# If secondary:
#     * search each observation from the database one at a time
# work this out later, it's of secondary importance. Get primary up and running first.

# -- MAIN PROGRAM --

# Work out which inputs we have, and whether we have enough to complete execution
if (args.utc1 and args.utc2):

    print("Primary mode engaged.")
    
    # process the utc timestamps into a format that PSRDB can understand
    utc1_psrdb = utc_normal2psrdb(args.utc1)
    utc2_psrdb = utc_normal2psrdb(args.utc2)

    # verify that the dates are in the correct order
    if (utc_normal2date(args.utc1) > utc_normal2date(args.utc2)):
        raise Exception("UTC1 (%s) is not before UTC2 (%s)." % (args.utc1, args.utc2))
    else:
        print ("Selected UTC range (%s - %s) is valid." % (args.utc1, args.utc2))

    # build a database query string, conditional on what other arguments have been passed
    dbquery = "%s foldedobservations list --utc_start_gte %s --utc_start_lte %s" % (PSRDB, utc1_psrdb, utc2_psrdb)
    if (args.pulsar):
        dbquery = "%s --pulsar_jname %s" % (dbquery, args.pulsar)
        print ("Querying for observations of pulsar %s." % (args.pulsar))
    if (args.pid):
        # first convert the pid into the format PSRDB understands
        dbquery = "%s --project_code %s" % (dbquery, pid_getofficial(args.pid))
        print ("Querying for observations matching PID %s." % (args.pid))

    # run the query
    dbdata = list_psrdb_query(dbquery)
    
    print("PSRDB query complete.")

    # write the resulting array of data to a file (if specified)
    if (args.list_out):
        outfile = open(args.list_out, "w")
        write_obs_list(outfile, dbdata)
        outfile.close()
        print("List of observations to process written to %s." % (args.list_out))

    # now comes the part where we start launching things. We need to go through the entries in the array, line by line, to work out out which pipelines need to be launched for which pulsar
    # as this will also need to be done for the secondary input option, abstract this to a function
    
    print("Launching requested processing jobs.")
    array_launcher(dbdata, args)

elif (args.list_in):

    print("Secondary mode engaged.")

    # we need to cycle through the lines of the file, find each observation and then send it to be launched
    infile = open(args.list_in, "r")

    print("%s opened successfully." % (args.list_in))

    for x in infile:
        linearray = shlex.split(x)
        query_date = utc_normal2psrdb(linearray[1])
        query_PID = pid_getofficial(linearray[2])

        print("Querying PSRDB for: PSR = %s, DATE = %s, PID = %s." % (linearray[0], linearray[1], linearray[2]))
        # now we need to query PSRDB for the entry in foldedobservations which matches the exact parameters of the line
        dbquery = "%s foldedobservations list  --utc_start_gte %s --utc_start_lte %s --pulsar_jname %s --project_code %s" % (PSRDB, query_date, query_date, linearray[0], query_PID)

        # run the query
        dbdata = list_psrdb_query(dbquery)
        print("PSRDB query complete.")

        # print(dbdata)

        print("Launching requested processing job.")
        array_launcher(dbdata, args)

else:
    # we do not have enough arguments to complete the script - abort
    raise Exception("Insufficient arguments provided. Must provide either UTC1 and UTC2, or LIST_IN.")
