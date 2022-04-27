#!/usr/bin/env python
"""

Python wrapper script to launch MeerPipe using PSRDB functionality
Intention is to mimic the function of query_obs.py & reprocessing.py but in a single script 

__author__ = "Andrew Cameron"
__copyright__ = "Copyright (C) 2022 Andrew Cameron"
__credits__ = ["Aditya Parthasarathy", "Andrew Jameson", "Stefan Oslowski"]                                                                                                                                                                 
__license__ = "Public Domain"
__version__ = "0.3"
__maintainer__ = "Andrew Cameron"
__email__ = "andrewcameron@swin.edu.au"
__status__ = "Development"
"""

# Import packages
import os,sys
import argparse
from argparse import RawTextHelpFormatter
import datetime
import subprocess
import shlex
import json
import time
import datetime

# PSRDB imports
from tables import *
from joins import *
from graphql_client import GraphQLClient
from db_utils import (utc_normal2psrdb, utc_psrdb2normal, utc_normal2date, utc_psrdb2date, pid_getofficial, 
                      check_response, pid_getshort, get_pulsar_id, pid_getdefaultpipe, get_pipe_config, 
                      check_pipeline, get_foldedobservation_obsid, get_job_output)

# Important paths
PSRDB = "psrdb.py"

# Argument parsing
parser = argparse.ArgumentParser(description="Launches specific observations to be processed by MeerPipe. Provide either a set of searchable parameters or a list of observations. If both inputs are provided, the provided search parameters will be used to filter the entries provided in the list.", formatter_class=RawTextHelpFormatter)
parser.add_argument("-utc1", dest="utc1", help="Start UTC for PSRDB search - returns only observations after this UTC timestamp.", default=None)
parser.add_argument("-utc2", dest="utc2", help="End UTC for PSRDB search - returns only observations before this UTC timestamp.", default=None)
parser.add_argument("-psr", dest="pulsar", help="Pulsar name for PSRDB search - returns only observations with this pulsar name. If not provided, returns all pulsars.", default=None)
parser.add_argument("-obs_pid", dest="pid", help="Project ID for PSRDB search - return only observations matching this Project ID. If not provided, returns all observations.", default=None)
parser.add_argument("-list_out", dest="list_out", help="Output file name to write the list of observations submitted by this particular search. Does not work in secondary mode as it would simply duplicate the input list.")
parser.add_argument("-list_in", dest="list_in", help="List of observations to process, given in standard format. These will be crossmatched against PSRDB before job submission. List format must be:\n* Column 1 - Pulsar name\n* Column 2 - UTC\n* Column 3 - Observation PID\nTrailing columns may be left out if needed, but at a minimum the pulsar name must be provided.")
parser.add_argument("-runas", dest="runas", help="Specify an override pipeline to use in processing the observations. \nOptions:\n'PIPE' - launch each observation through multiple pipelines as defined by the 'launches' PSRDB table (default).\n'OBS' - use the observation PID to define pipeline selection.\n<int> - specify a specific PSRDB pipeline ID.\n<pid> - specify a MeerTIME project code (e.g. 'PTA', 'RelBin'), which will launch a default pipeline.", default="PIPE")
parser.add_argument("-slurm", dest="slurm", help="Processes all jobs using the OzStar Slurm queue.",action="store_true")
parser.add_argument("-unprocessed", dest="unprocessed", help="Launch only those observations which have not yet been processed by the specified pipelines.", action="store_true")
parser.add_argument("-job_limit", dest="joblimit", type=int, help="Max number of jobs to accept to the queue at any given time - script will wait and monitor for queue to reduce below this number before sending more.", default=1000)
parser.add_argument("-forceram", dest="forceram", type=float, help="Specify RAM to use for job execution (GB). Recommended only for single-job launches.")
parser.add_argument("-forcetime", dest="forcetime", type=str, help="Specify time to use for job execution (HH:MM:SS). Recommended only for single-job launches.")
parser.add_argument("-errorlog", dest="errorlog", type=str, help="File to store information on any failed launches for later debugging.", default=None)
parser.add_argument("-testrun", dest="testrun", help="Toggles test mode - jobs will not actually be launched.", action="store_true")
parser.add_argument("-obs_id", dest="obsid", type=int, help="Specify a single PSRDB observation ID to be processed. Observation must also be either specified via UTC range or list input. Typically only for use by real-time launch script.")
args = parser.parse_args()


# -- FUNCTIONS --

# ROLE   : Returns a list of folded observations matching the queried parameters
#          UTC should be in PSRDB format, PID should be official code
# INPUTS : String, String, String, String, GraphQL client, String, String
# RETURNS: Array (success) | None (failure) 
def get_foldedobservation_list(utc1, utc2, pulsar, pcode, client, url, token):

    # PSRDB setup
    foldedobs = FoldedObservations(client, url, token)
    foldedobs.get_dicts = True
    foldedobs.set_use_pagination(True)

    # Query based on provided parameters

    foldobs_data = foldedobs.list(
        None,
        pulsar,
        None,
        None,
        None,
        pcode,
        None,
        None,
        utc1,
        utc2
    )
    
    # check for a valid result
    if (len(foldobs_data) > 0):
        return foldobs_data
    else:
        return


# ROLE   : Returns the name of a pulsar associated with a single entry of folded obs data
# INPUTS : PSRDB JSON data, GraphQL client, String, String
# RETURNS: String | None (failure)
def get_pulsarname(dbdata, client, url, token):

    # check for valid input
    if (isinstance(dbdata, list)):
        raise Exception("Passed array to function expecting non-array input - aborting.")

    # PSRDB setup
    pulsars = Pulsars(client, url, token)
    pulsartargets = Pulsartargets(client, url, token)

    # get the name of the target
    target_name = dbdata['node']['processing']['observation']['target']['name']

    # check that this is a valid pulsar name
    response = pulsars.list(
        None,
        target_name
    )
    check_response(response)
    psr_content = json.loads(response.content)
    psr_data = psr_content['data']['allPulsars']['edges']

    # check output
    retval = None
    if (len(psr_data) == 1):
        # success
        retval = target_name
    elif (len(psr_data) > 1):
        raise Exception("Multiple entries in table 'pulsars' found with the same JNAME - PSRDB integrity checks required.")
    elif (len(psr_data) == 0):
        # no records found for pulsars matching the target - check for cross matches with pulsarTargets
        eph_name = dbdata['node']['foldingEphemeris']['pulsar']['jname']

        response = pulsartargets.list(
            None,
            None,
            target_name,
            None,
            eph_name
        )
        check_response(response)
        tgt_content = json.loads(response.content)
        tgt_data = tgt_content['data']['allPulsartargets']['edges']

        # check for valid result
        if (len(tgt_data) == 1):
            # success
            retval = eph_name

    return retval

# ROLE   : Returns the UTC associated with a single entry of folded obs data
# INPUTS : PSRDB JSON data, GraphQL client, String, String
# RETURNS: String | None (failure)
def get_utc(dbdata, client, url, token):

    # check for valid input
    if (isinstance(dbdata, list)):
        raise Exception("Passed array to function expecting non-array input - aborting.")

    return dbdata['node']['processing']['observation']['utcStart']

# ROLE   : Returns the data location associated with a single entry of folded obs data
# INPUTS : PSRDB JSON data, GraphQL client, String, String
# RETURNS: String | None (failure)
def get_location(dbdata, client, url, token):

    # check for valid input
    if (isinstance(dbdata, list)):
        raise Exception("Passed array to function expecting non-array input - aborting.")

    return dbdata['node']['processing']['location']

# ROLE   : Returns the parent processing associated with a single entry of folded obs data
# INPUTS : PSRDB JSON data, GraphQL client, String, String
# RETURNS: Int | None (failure)
def get_parent_id(dbdata, client, url, token):

    # check for valid input
    if (isinstance(dbdata, list)):
        raise Exception("Passed array to function expecting non-array input - aborting.")

    processings = Processings(client, url, token)
    return int(processings.decode_id(dbdata['node']['processing']['id']))

# ROLE   : Checks if there a is a processing already matching the provided criteria
# INPUTS : Int, Int, Int, GraphQL client, String, String
# RETURNS: None | Int
def check_for_processing(parent_id, obs_id, pipe_id, client, url, token):

    # sanitise input
    parent_id = int(parent_id)
    pipe_id = int(pipe_id)
    obs_id = int(obs_id)
    
    # PSRDB setup
    processings = Processings(client, url, token)
    processings.set_field_names(True, False)

    response = processings.list(
        None,
        obs_id,
        parent_id,
        None,
        None
    )
    proc_content = json.loads(response.content)
    proc_data = proc_content['data']['allProcessings']['edges']

    result = None
    # check for matching pipe_id
    for x in range(0, len(proc_data)):
        proc_pipe_id = int(processings.decode_id(proc_data[x]['node']['pipeline']['id']))
        if (proc_pipe_id == pipe_id):
            result = int(processings.decode_id(proc_data[x]['node']['id']))

    return result
        

# ROLE   : Determines which pipeline should be run based on the scheme outlined in the help menu
# INPUTS : Arg dictionary, Int
# RETURNS: Array (ints)
def determine_pipelines(dbdata, ag, psr_id, client, url, token):

    # check for valid input
    if (isinstance(dbdata, list)):
        raise Exception("Passed array to function expecting non-array input - aborting.")

    # PSRDB setup
    launches = Launches(client, url, token)
    launches.set_field_names(True, False)
    pipelines = Pipelines(client, url, token)

    success = False
    pipe_list = []
    if (ag.runas == "PIPE"):

        print ("Recalling pipelines from PSRDB launches table...")

        # check the launches table for entries matching the pulsar id
        response = launches.list(
            None,
            None,
            None,
            psr_id
        )
        check_response(response)
        launch_content = json.loads(response.content)
        launch_data = launch_content['data']['allLaunches']['edges']

        # all pipeline entries here should be valid by virtue of PSRDB integrity checks - assume correctness and copy through
        if (len(launch_data) > 0):
            success = True
            for x in range(0, len(launch_data)):
                next_id = launches.decode_id(launch_data[x]['node']['pipeline']['id'])
                pipe_list.append(next_id)

    else:
        if (ag.runas == "OBS"):

            # determine the pipeline based on the obs project code
            project_code = dbdata['node']['processing']['observation']['project']['code']
            print ("Using default pipeline for observation's project code {0}".format(project_code))
            pipe_id = pid_getdefaultpipe(project_code)
            print ("Default pipeline is {0}".format(pipe_id))

        else:
            # manual specification of a single pipeline by int or project
            try:
                pipe_id = int(ag.runas)
            except:
                # it's not an int, try a PID
                try:
                    project_code = pid_getofficial(ag.runas)
                except:
                    # failure
                    pipe_id = None
                else:
                    # we have a project code - get a default pipeline
                    print ("Using default pipeline for project code {0}".format(ag.runas))
                    pipe_id = pid_getdefaultpipe(project_code)
                    print ("Default pipeline is {0}".format(pipe_id))
            else:
                # it is an int
                print ("User-specified pipeline ID ({0}) identified".format(pipe_id))
                    
        # check result for validity
        if (pipe_id):
            pipe_test = check_pipeline(pipe_id, client, url, token)
            if (pipe_test):
                success = True
                pipe_list.append(pipe_id)

    # check for success
    if not (success):
        return
    else:
        return pipe_list

# ROLE   : Write observations being processed to a file in a similar format to query_obs.py
# INPUTS : File handle | JSON Array
# RETURNS: Nothing
def write_list(fh, arr, client, url, token):

    for x in range(0, len(arr)):

        fh.write("%s\t%s\t%s\n" % (get_pulsarname(arr[x], client, url, token), utc_psrdb2normal(get_utc(arr[x], client, url, token)), pid_getshort(arr[x]['node']['processing']['observation']['project']['code'])))

    return

# for a given array of psrdb observations and argpase arguments, determine which pipelines should be launched for each entry and then launch them
def array_launcher(arr, ag, client, url, token):

    # set up the errorlog, if one is set
    if not (ag.errorlog == None):
        errorfile = open(ag.errorlog, "w")

    # begin scrolling through the array
    for x in range (0, len(arr)):

        # get the pulsar name, observation UTC and file location associated with the provided folding id
        psr_name = get_pulsarname(arr[x], client, url, token)
        if (psr_name):

            # continue execution
            utc = get_utc(arr[x], client, url, token)
            obs_location = get_location(arr[x], client, url, token)
            psr_id = get_pulsar_id(psr_name, client, url, token)
            parent_id = get_parent_id(arr[x], client, url, token)

            # determine which pipelines get launched
            pipeline_list = determine_pipelines(arr[x], ag, psr_id, client, url, token)
            if (pipeline_list):
        
                # set of pipelines determined - scroll through and launch
                for y in range(0, len(pipeline_list)):

                    # catch errors and report to file
                    errorstring=""
                    try:
                        # need to get the configuration data
                        config_data = get_pipe_config(pipeline_list[y], client, url, token)

                        # run some sanity checks
                    
                        # check if config data actually exists
                        # there may be a better way to handle this case, but for now...
                        if (len(config_data) == 0):
                            errorstring="Attempted to run pipeline ({0}) with no config information - aborting. Please inspect execution parameters and try again".format(pipeline_list[y])
                            raise Exception(errorstring)

                        # check for file path consistency for the raw data location
                        config_query = "grep input_path {0}".format(config_data['config'])
                        proc_query = shlex.split(config_query)
                        proc = subprocess.Popen(proc_query, stdout=subprocess.PIPE)
                        out = proc.stdout.read().decode("utf-8")
                        linearray = out.split("\n")[0].split(" ")

                        if (len(linearray) != 3):
                            errorstring="Config file ({0}) parameter 'input_path' is not the correct length.".format(config_data['config'])
                            raise Exception(errorstring)

                        in_path = linearray[2]
                        
                        # check if the paths are self-consistent
                        if (not ((in_path in obs_location) or (obs_location in in_path))):
                            errorstring="Config file ({0}) parameter 'input_path' is inconsistent with database file locations - skipping".format(config_data['config'])
                            raise Exception(errorstring)

                        # get the launch project code from the config data
                        launch_project_code = pid_getshort(config_data['pid'])

                        # get the obs_id and prior proc_id to prevent duplicate job launching
                        obs_id = get_foldedobservation_obsid(utc, psr_name, obs_location, client, url, token)
                        proc_id = check_for_processing(parent_id, obs_id, pipeline_list[y], client, url, token)

                        # check for the obsid flag
                        if (args.obsid):
                            if not (int(args.obsid) == int(obs_id)):
                                print ("Match not found to command line obs ID of {0} - skipping...".format(args.obsid))
                                continue

                        # check for the unprocessed flag
                        if (ag.unprocessed):
                            
                            # check if a processing already exists
                            if not (proc_id == None):
                                print ("Skipping: PSR = {0} | OBS = {1} | PROJECT CODE = {2} | PIPE = {3}".format(psr_name, utc_psrdb2normal(utc), launch_project_code, pipeline_list[y]))
                                print ("Processing already exists!")
                                continue
                        
                        # if a previous proc_id does exist, check for the SLURM job id in case it's still running on the queue!
                        if not (proc_id == None):
                            
                            job_out = get_job_output(proc_id, client, url, token)
                            if 'job_id' in job_out.keys():
                                job_id = job_out['job_id']

                                comm = "slurm job {}".format(job_id)
                                args = shlex.split(comm)
                                proc = subprocess.Popen(args,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                                slurm_data = proc.communicate()
                                slurm_out = slurm_data[0].decode("utf-8").rstrip().split("\n")
                                slurm_err = slurm_data[1].decode("utf-8").rstrip().split("\n")

                                if (len(slurm_out) > 1 and not "Invalid job id specified" in slurm_err[0]):

                                    print ("Skipping: PSR = {0} | OBS = {1} | PROJECT CODE = {2} | PIPE = {3}".format(psr_name, utc_psrdb2normal(utc), launch_project_code, pipeline_list[y]))
                                    print ("Processing of this entry is already in progress!")
                                    continue

                        # finally, launch
                        print ("Launching: PSR = {0} | OBS = {1} | PROJECT CODE = {2} | PIPE = {3}".format(psr_name, utc_psrdb2normal(utc), launch_project_code, pipeline_list[y]))
                        pipeline_launch_instruction = "{0} -cfile {1} -dirname {2} -utc {3} -verbose -pid {4} -db -db_pipe {5} -db_obsid {6}".format(config_data['path'], config_data['config'], psr_name, utc_psrdb2normal(utc), launch_project_code, pipeline_list[y], obs_id)
                        # check for slurm
                        if (ag.slurm):
                            pipeline_launch_instruction = "{0} -slurm".format(pipeline_launch_instruction)

                        # check manual force parameters
                        # no validity checks here - this will be done in run_pipe.py
                        if ag.forceram:
                            pipeline_launch_instruction = "{0} -forceram {1}".format(pipeline_launch_instruction, ag.forceram)
                        if ag.forcetime:
                            pipeline_launch_instruction = "{0} -forcetime {1}".format(pipeline_launch_instruction, ag.forcetime)

                        # launch the jobs - check for SLURM limit if required and wait
                        if (ag.slurm):
                            queue_flag = False
                            while not (queue_flag):

                                # get the slurm queue size
                                comm = "slurm queue"
                                args = shlex.split(comm)
                                proc = subprocess.Popen(args,stdout=subprocess.PIPE)
                                slurm_data = proc.communicate()[0].decode("utf-8").rstrip().split("\n")

                                if ((len(slurm_data) - 1) < ag.joblimit):
                                    queue_flag = True
                                else:
                                    delay = 60 #seconds
                                    print ("Script-imposed SLURM queue limit of {0} exceeded.".format(ag.joblimit))
                                    print ("Current time is {0} - waiting {1} seconds...".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), delay))
                                    time.sleep(delay)
                        
                        # new test mode ability
                        if not (ag.testrun):
                            proc_query = shlex.split(pipeline_launch_instruction)
                            proc = subprocess.Popen(proc_query)
                            # wait until jobs are finished launching before returning the command line
                            proc.wait()

                    except:
                        print ("Error detected - skipping launch of job\nIf error log file enabled, details will be reported there.")
                        # report error
                        if not (ag.errorlog == None):
                            errorfile.write("\n-- Error detected --\n")
                            errorfile.write("{}\n".format(errorstring))
                            errorfile.write("Pipeline = {0}\n".format(pipeline_list[y]))
                            errorfile.write("{}\n".format(json.dumps(arr[x])))
                        

            else:

                print ("Unable to identify any valid pipelines to launch on the following entry")
                print (arr[x])
                print ("\nSkipping...\n")

        else:
 
            print ("Unable to identify a unique pulsar name matching the following entry")
            print (arr[x])
            print ("\nSkipping...\n")

    # close errorfile
    if not (ag.errorlog == None):
        errorfile.close()

    return

# -- MAIN PROGRAM --

# PSRDB setup
env_query = 'echo $PSRDB_TOKEN'
db_token = str(subprocess.check_output(env_query, shell=True).decode("utf-8")).rstrip()
env_query = 'echo $PSRDB_URL'
db_url = str(subprocess.check_output(env_query, shell=True).decode("utf-8")).rstrip()
db_client = GraphQLClient(db_url, False)

# Determine what parameters we have to work with and build search
print ("Launching observations matching the intersection of the following specifications:")
if (args.list_in):
    print ("* List: {}".format(args.list_in))

if (args.utc1):
    print ("* Start UTC: {}".format(args.utc1))
    utc1_psrdb = utc_normal2psrdb(args.utc1)
else:
    utc1_psrdb = None

if (args.utc2):
    print ("* End UTC: {}".format(args.utc2))
    utc2_psrdb = utc_normal2psrdb(args.utc2)
else:
    utc2_psrdb = None

if (args.pulsar):
    print ("* PSR: {}".format(args.pulsar))

if (args.pid):
    print ("* PID: {}".format(args.pid))
    official_PID =  pid_getofficial(args.pid)
else:
    official_PID = None

if (args.unprocessed):
    print ("* Only launching unprocessed observations.")

# Verify input
if (args.utc1 and args.utc2):
    if (utc_normal2date(args.utc1) > utc_normal2date(args.utc2)):
        raise Exception("UTC1 ({0}) is not before UTC2 ({1}).".format(args.utc1, args.utc2))
    else:
        print ("Selected UTC range ({0} - {1}) is valid.".format(args.utc1, args.utc2))

# Query and run observations

print ("\nBeginning launch sequence...\n")

if (args.testrun):
    print ("\nTEST MODE ACTIVATED - JOBS WILL NOT BE LAUNCHED\n")

if (args.list_in):

    # we need to cycle through the lines of the file, find each observation and then send it to be launched
    infile = open(args.list_in, "r")
    # if output list specified, prep for writing
    if (args.list_out):
        outfile = open(args.list_out, "w")
        outfile.close()

    print("%s opened successfully - processing entries line by line..." % (args.list_in))

    for x in infile:
        linearray = shlex.split(x)

        # check for valid input
        if (len(linearray) == 0 or len(linearray) > 3):
            raise Exception("Input file has an incorrect number of fields (line {0})".format(x))

        # check input for query parameters and test against 
        query_pulsar = None
        query_date = None
        query_PID = None

        if (len(linearray) > 0):
            # first column is pulsar name
            query_pulsar = str(linearray[0])

            if (args.pulsar):
                if not (args.pulsar == query_pulsar):
                    print ("File entry '{0}' does not match command line entry '{1}' - skipping...".format(query_pulsar, args.pulsar))
                    continue

        if (len(linearray) > 1):
            # second column is UTC of the observation
            try:
                query_date = utc_normal2psrdb(linearray[1])
            except:
                print ("File UTC '{0}' has an incorrect format - skipping...".format(linearray[1]))
                continue
    
            if (args.utc1):
                if (utc_normal2date(args.utc1) > utc_psrdb2date(query_date)):
                    print ("File UTC '{0}' is before specified command line start date '{1}' - skipping...".format(utc_psrdb2normal(query_date), args.utc1))
                    continue

            if (args.utc2):
                if (utc_normal2date(args.utc2) < utc_psrdb2date(query_date)):
                    print ("File UTC '{0}' is after specified command line end date '{1}' - skipping...".format(utc_psrdb2normal(query_date), args.utc2))
                    continue

        if (len(linearray) > 2):
            # third column is PID
            try:
                query_PID = pid_getofficial(linearray[2])
            except:
                print ("File observation PID '{0}' is not recognised - skipping...".format(linearray[2]))
                continue

            if (official_PID):
                if not (official_PID == query_PID):
                    print ("File observation PID '{0}' does not match command line observation PID '{1}' - skipping...".format(query_PID, official_PID))
                    continue

        print ("Querying PSRDB for:")

        if not (query_pulsar == None):
            print ("PSR = {0}".format(query_pulsar))
        elif (args.pulsar):
            query_pulsar = args.pulsar

        if not (query_date == None):
            print ("DATE = {0}".format(query_date))
            start_date = query_date
            end_date = query_date
        else:
            if (args.utc1):
                start_date = utc1_psrdb
            else:
                start_date = None

            if (args.utc2):
                end_date = utc2_psrdb
            else:
                end_date = None

        if not (query_PID == None):
            print ("Observation PID = {0}".format(query_PID))
        elif (args.pid):
            query_PID = official_PID 

        dbdata = get_foldedobservation_list(start_date, end_date, query_pulsar, query_PID, db_client, db_url, db_token)
        print("PSRDB query complete.")

        if (dbdata):
            print("PSRDB query complete - {0} matching records found".format(len(dbdata)))
            print("Launching requested processing job.")
            array_launcher(dbdata, args, db_client, db_url, db_token)

            if (args.list_out):
                outfile = open(args.list_out, "a")
                write_list(outfile, dbdata, db_client, db_url, db_token)
                outfile.close()

    if (args.list_out):
        print("List of observations to process written to {0}.".format(args.list_out))

elif (args.utc1 and args.utc2):

    # I'm maintaining this restriction of needing at minimum either the LIST IN or the UTCs
    # Otherwise there's a risk that running the script with no inputs will just launch *everything* at once

    dbdata = get_foldedobservation_list(utc1_psrdb, utc2_psrdb, args.pulsar, official_PID, db_client, db_url, db_token)

    # check for valid output
    if (dbdata):
        print("PSRDB query complete - {0} matching records found".format(len(dbdata)))
    else:
        raise Exception("No records found matching specified parameters - aborting.")

    # now run the jobs
    print("Launching requested processing jobs.")
    array_launcher(dbdata, args, db_client, db_url, db_token)

    # write the resulting array of data to a file (if specified)
    if (args.list_out):
        outfile = open(args.list_out, "w")
        write_list(outfile, dbdata, db_client, db_url, db_token)
        outfile.close()
        print("List of observations to process written to {0}.".format(args.list_out))

else:
    # we do not have enough arguments to complete the script - abort
    raise Exception("Insufficient arguments provided. Must provide at least UTC1 and UTC2, or LIST_IN.")
