"""
Code containing utilities for interfacing PSRDB with MeerPIPE

__author__ = "Andrew Cameron"
__copyright__ = "Copyright (C) 2021 Andrew Cameron"
__credits__ = ["Aditya Parthasarathy", "Andrew Jameson", "Stefan Oslowski"]
__license__ = "Public Domain"
__version__ = "0.1"
__maintainer__ = "Andrew Cameron"
__email__ = "andrewcameron@swin.edu.au"
__status__ = "Development"
"""

# Import packages
import os,sys
import argparse
import datetime
import subprocess
import shlex
import json
import time
import pandas as pd

# Important paths
PSRDB = "psrdb.py"

# Function Definitions

# convert a "normal" utc date to a format readable by psrdb
def utc_normal2psrdb(d):

    dr = utc_normal2date(d)
    return utc_date2psrdb(dr)

# convert a psrdb utc date to the "normal" format
def utc_psrdb2normal(d):

    dr = utc_psrdb2date(d)
    return utc_date2normal(dr)

# convert a "normal" utc date to a datetime object
def utc_normal2date(d):

    return  datetime.datetime.strptime(d, '%Y-%m-%d-%H:%M:%S')

# convert a psrdb utc date to a datetime object
def utc_psrdb2date(d):

    return datetime.datetime.strptime(d, '%Y-%m-%dT%H:%M:%S+00:00')

# convert a datetime object into a "normal" utc date
def utc_date2normal(d):

    return "%s-%s" % (d.date(), d.time())

# convert a datetime object into a psrdb utc date
def utc_date2psrdb(d):

    return "%sT%s+00:00" % (d.date(), d.time())

# convert short-hand PID to official PID - encoding taken from query_obs.py (Aditya)
# TODO: May be incomplete! Check official list with Ryan and/or Matthew
def pid_getofficial(sp):

    if sp == "MB01":
        op = "SCI-20180516-MB-01"
    elif sp == "TPA":
        op = "SCI-20180516-MB-02"
    elif sp == "RelBin":
        op = "SCI-20180516-MB-03"
    elif sp == "GC":
        op = "SCI-20180516-MB-04"
    elif sp == "PTA":
        op = "SCI-20180516-MB-05"
    elif sp == "NGC6440":
        op = "SCI-20180516-MB-06"
    elif sp == "fluxcal":
        op = "SCI-20180516-MB-99"
    elif sp == "None":
        op = "None"
    else:
        raise Exception("Unknown PID (%s)." % (sp))

    return op

# convert official PID to short-hand PID - encoding taken from query_obs.py (Aditya)
def pid_getshort(op):

    if op == "SCI-20180516-MB-01":
        sp = "MB01"
    elif op == "SCI-20180516-MB-02":
        sp = "TPA"
    elif op == "SCI-20180516-MB-03":
        sp = "RelBin"
    elif op == "SCI-20180516-MB-04":
        sp = "GC"
    elif op == "SCI-20180516-MB-05":
        sp = "PTA"
    elif op == "SCI-20180516-MB-06":
        sp = "NGC6440"
    elif op == "SCI-20180516-MB-99":
        sp = "fluxcal"
    elif op == "None":
        sp = "None"
    else:
        sp = "Rogue"

    return sp

# shorthand storage for processing job states
def job_state_code(jid):

    if jid == 0:
        state = "Configuring" # job is being set up but has not yet been launched
    elif jid == 1:
        state = "Pending" # job is on queue but has not yet been started
    elif jid == 2:
        state = "Running" # job is on queue and is currently being processed
    elif jid == 3:
        state = "Complete" # job has finished successfully
    elif jid == 4:
        state = "Failure" # job has finished unsuccessfully
    elif jid == 5:
        state = "Unknown" # cover-all for indeterminate states / outcomes still under development (e.g. "fluxcal")

    #return state
    return json.dumps({"job_state": state})

# translate a database query into a usable array
# this array WILL have a header entry in the first index by default, unless the provied query somehow removes it
def list_psrdb_query(query):

    # shlex the query and run through subprocess
    proc_query = shlex.split(query)
    proc = subprocess.Popen(proc_query, stdout=subprocess.PIPE)
    proc.wait()

    # extract the stdout and convert from bytes into a string
    out = proc.stdout.read().decode("utf-8")
    # split the string using '\n' to form a preliminary array
    linearray = out.split("\n")
    # this will create one extra empty element at the end, which we need to check for
    if (len(linearray[len(linearray)-1]) < 1):
        v_size = len(linearray) - 1
    else:
        v_size = len(linearray)

    # we have the vertical dimension, now the horizontal dimension
    h_size = len(linearray[0].split("\t"))

    # construct an output array, now splitting for "\t"
    elemarray = [[None]*h_size]*v_size
    for x in range (0, v_size):
        elemarray[x] = linearray[x].split("\t")

    return elemarray

# function to take a psrdb query designed to create an entry in the database, and run it
# returns the id of the created entry
def create_psrdb_query(query):

    # shlex the query and run through subprocess
    proc_query = shlex.split(query)
    proc = subprocess.Popen(proc_query, stdout=subprocess.PIPE)
    proc.wait()

    out = proc.stdout.read().decode("utf-8")
    linearray = out.split("\n")

    # required value should just be the first element of the returned array

    return linearray[0]

# updats a psrdb entry based on an input query
def update_psrdb_query(query):

    # shlex the query and run through subprocess
    proc_query = shlex.split(query)
    proc = subprocess.Popen(proc_query, stdout=subprocess.PIPE)
    proc.wait()
    
    # no output yet required
    return

# write the contents of a db_array to a file in query_obs format
# to let this function work with both primary & secondary inputs, only append data - the exterior code can determine whether the file is getting wiped or not
def write_obs_list(fh, arr):

    # we need three things: the pulsar name, the utc of the observation, and the short-hand PID
    # determine which colums each of these things belong to, then extract the data and write to file
    psr_index = arr[0].index("processing_observation_target_name")
    pid_index = arr[0].index("processing_observation_project_code")
    utc_index = arr[0].index("processing_observation_utcStart")

    # now for each subsequent row in the array, look up these indices and write to file
    for x in range (1, len(arr)):
        fh.write("%s\t%s\t%s\n" % (arr[x][psr_index], utc_psrdb2normal(arr[x][utc_index]), pid_getshort(arr[x][pid_index])))

    return

# return the psrdb id of a pulsar given a jname
def get_pulsar_id(psr):

    # set up the query
    psr_query = "%s pulsars list --jname %s" % (PSRDB, psr)
    # run the query and get the resulting database
    psr_data = list_psrdb_query(psr_query)

    # there should only be two lines - the header and the unique pulsar entry
    # confirm this and report back result
    if (len(psr_data) == 2):
        id_index = psr_data[0].index("id")
        psr_id = psr_data[1][id_index]
    else:
        raise Exception("Incorrect number of PSRDB entries reported in table 'pulsars' for %s" % (psr))

    return psr_id

# return the observation id given a pulsar name and an exact (normal) utc
def get_observation_id(utc, psr):

    # flag for validity
    valid = False

    # get UTC
    db_utc = utc_normal2psrdb(utc)
    
    # recall all observations with that UTC (in theory unique, but not guaranteed)
    obs_query = "%s observations list --utcstart_gte %s --utcstart_lte %s" % (PSRDB, db_utc, db_utc)
    obs_data = list_psrdb_query(obs_query)
    target_index = obs_data[0].index("target_name")
    id_index = obs_data[0].index("id")

    # check if any of these has a target name matching the psrname (should only be one entry if any)
    for x in range(1, len(obs_data)):
        target_query = "%s pulsartargets list --target_name %s --pulsar_jname %s" % (PSRDB, obs_data[x][target_index], psr)
        target_data = list_psrdb_query(target_query)

        # check for correct length and if pass, record result
        if (len(target_data) == 2):
            obs_id = obs_data[x][id_index]   
            valid = True

    # check for valid result
    if (valid):
        return obs_id
    else:
        raise Exception("Observation ID not found in table 'observations' matching UTC %s and pulsar %s" % (utc, psr))

# return a timedelta object for an embargo timespan given a project id
def get_project_embargo(pid):

    # set up the query
    proj_query = "%s projects list --id %s" % (PSRDB, pid)
    # run the query and get the resulting database
    proj_data = list_psrdb_query(proj_query)

    # there should only be two lines - the header and the unique project entry
    # confirm this and report back result
    if (len(proj_data) == 2):
        embargo_index = proj_data[0].index("embargoPeriod")
        embargo_period = proj_data[1][embargo_index]
    else:
        raise Exception("Incorrect number of PSRDB entries reported in table 'projects' for PID %s" % (pid))

    return pd.to_timedelta(embargo_period)

# return a project's formal code given an observation id
def get_observation_project_code(obs_id):

    # set up the query
    obs_query = "%s observations list --id %s" % (PSRDB, obs_id)
    # run the query and get the resulting database
    obs_data = list_psrdb_query(obs_query)

    # there should only be two lines - the header and the unique observation entry
    # confirm this and report back result
    if (len(obs_data) == 2):
        proj_index = obs_data[0].index("project_code")
        proj_code = obs_data[1][proj_index]
    else:
        raise Exception("Incorrect number of PSRDB entries reported in table 'observations' for ID %s" % (obs_id))

    return proj_code

# return a DB ID for a given project code
def get_project_id(proj_code):

    # set up the query
    proj_query = "%s projects list --code %s" % (PSRDB, proj_code)
    # run the query and get the resulting database
    proj_data = list_psrdb_query(proj_query)

    # there should only be two lines - the header and the unique observation entry
    # confirm this and report back result
    if (len(proj_data) == 2):
        id_index = proj_data[0].index("id")
        id_num = proj_data[1][id_index]
    else:
        raise Exception("Incorrect number of PSRDB entries reported in table 'projects' for code %s" % (proj_code))

    return id_num

# returns the name of the node on which the job is running
def get_node_name():

    info = "hostname"
    arg = shlex.split(info)
    proc = subprocess.Popen(arg,stdout=subprocess.PIPE)
    info_ret = proc.stdout.readline().rstrip().split()

    return str(info_ret[0].decode("utf-8"))

# formats the output of a JSON dumps command into a PSRDB compatible JSON string
def psrdb_json_formatter(json_str):

    return """'{0}'""".format(json_str)

# returns the slurm job id of a processing entry from PSRDB
def get_slurm_id(proc_id):

    # set up the query
    proc_query = "%s processings list --id %s" % (PSRDB, proc_id)
    # run the query and get the resulting database
    proc_data = list_psrdb_query(proc_query)

    # there should only be two lines - the header and the unique observation entry
    # confirm this and report back result
    if (len(proc_data) == 2):
        jobout_index = proc_data[0].index("jobOutput")
        jobout_data = proc_data[1][jobout_index]
    else:
        raise Exception("Incorrect number of PSRDB entries reported in table 'processings' for ID %s" % (proc_id))

    # now we need to interpret the JSON string and recall the JOB_ID
    jobout_json = json.loads(jobout_data.replace("'", '"'))
    
    return jobout_json['job_id']


# returns the job state of a processing entry from PSRDB
def get_job_state(proc_id):

    # set up the query
    proc_query = "%s processings list --id %s" % (PSRDB, proc_id)
    # run the query and get the resulting database
    proc_data = list_psrdb_query(proc_query)

    # there should only be two lines - the header and the unique observation entry
    # confirm this and report back result
    if (len(proc_data) == 2):
        jobstate_index = proc_data[0].index("jobState")
        jobstate_data = proc_data[1][jobstate_index]
    else:
        raise Exception("Incorrect number of PSRDB entries reported in table 'processings' for ID %s" % (proc_id))

    # now we need to interpret the JSON string and recall the job state
    jobstate_json = json.loads(jobstate_data.replace("'", '"'))

    return jobstate_json['job_state']
    
