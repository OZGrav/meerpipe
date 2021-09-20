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
import logging
from util import header, ephemeris
from util import time as util_time
import getpass
from astropy.time import Time as astrotime

from tables import *
from graphql_client import GraphQLClient

# Important paths
PSRDB = "psrdb.py"

# Function Definitions

# ----- MISC UTILITIES FUNCTIONS -----

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

# formats the output of a JSON dumps command into a PSRDB compatible JSON string
def psrdb_json_formatter(json_str):

    return """'{0}'""".format(json_str)

# returns the name of the node on which the job is running
def get_node_name():

    info = "hostname"
    arg = shlex.split(info)
    proc = subprocess.Popen(arg,stdout=subprocess.PIPE)
    info_ret = proc.stdout.readline().rstrip().split()

    return str(info_ret[0].decode("utf-8"))

# ----- CHECK FUNCTIONS -----

# check if a PSRDB GraphQL query gave a valid response
# if not valid, raise exception
def check_response(response):

    valid = False

    # tests for validity
    if (response.status_code == 200):
        valid = True
    
    if not (valid):
        raise Exception("ERROR: Invalid GraphQL response detected (code %d)" % (response.status_code))

    return

# check if a pipeline ID is valid
# future checks may need to be added to ensure that the pipeline being called is a MEERPIPE pipeline
def check_pipeline(pipe_id, client, url, token):

    # setup PSRDB tables
    pipelines = Pipelines(client, url, token)

    # query for matching IDs
    response = pipelines.list_graphql(pipe_id, None)
    check_response(response)
    pipe_content = json.loads(response.content)

    if (pipe_content['data']['pipeline'] == None):
        retval = False
    else:
        retval = True

    return retval

# ----- GET FUNCTIONS -----

# return a unique observation id given a pulsar name and an exact psrdb-format utc
def get_observation_id(utc, psr, client, url, token):

    # setup PSRDB tables
    observations = Observations(client, url, token)
    pulsartargets = Pulsartargets(client, url, token)

    # recall all observations with that UTC
    response = observations.list_graphql(None, None, None, None, None, None, None, None, None, utc, utc)
    check_response(response)
    obs_content = json.loads(response.content)
    obs_data = obs_content['data']['allObservations']['edges']

    # check if any of these has a target name matching the psrname (should only be one entry if any)
    match_counter = 0
    for x in range(0, len(obs_data)):
        target_name = obs_data[x]['node']['target']['name']
        response = pulsartargets.list_graphql(None, None, target_name, None, psr)
        check_response(response)
        target_content = json.loads(response.content)
        target_data = target_content['data']['allPulsartargets']['edges']

        obs_id = observations.decode_id(obs_data[x]['node']['id'])
        match_counter = match_counter + len(target_data)

    # check for valid result
    if (match_counter == 1):
        return obs_id
    else:
        return

# return a unique folding id given an observation ID and a pipeline ID
def get_folding_id(obs_id, pipe_id, client, url, token):

    # setup PSRDB tables
    processings = Processings(client, url, token)
    pipelines = Pipelines(client, url, token)
    foldings = Foldings(client, url, token)

    # recall all processings matching the obs_ID
    response = processings.list_graphql(None, obs_id, None, None)
    check_response(response)
    proc_content = json.loads(response.content)
    proc_data = proc_content['data']['allProcessings']['edges']

    # check if any of these has a parent_id name matching the provided pipe_id
    match_counter = 0
    for x in range(0, len(proc_data)):

        if (pipelines.decode_id(proc_data[x]['node']['parent']['id']) == pipe_id):

            response = foldings.list_graphql(None, processings.decode_id(proc_data[x]['node']['id']), None)
            check_response(response)
            fold_content = json.loads(response.content)
            fold_data = fold_content['data']['allFoldings']['edges']

            for y in range (0, len(fold_data)):
                match_counter = match_counter + 1
                fold_id = foldings.decode_id(fold_data[y]['node']['id'])

    # check for valid result
    if (match_counter == 1):
        return fold_id
    else:
        return

# return the psrdb id of a pulsar given a jname
def get_pulsar_id(psr, client, url, token):

    # setup PSRDB tables
    pulsars = Pulsars(client, url, token)

    # query for entries matching the jname
    response = pulsars.list_graphql(None, psr)
    check_response(response)
    psr_content = json.loads(response.content)
    psr_data = psr_content['data']['allPulsars']['edges']

    # there should only be one entry - check and confirm, else return None
    if (len(psr_data) == 1):
        psr_id = pulsars.decode_id(psr_data[0]['node']['id'])
        return psr_id
    else:
        return

# return a DB ID for a given formal project code
def get_project_id(proj_code, client, url, token):

    # setup PSRDB tables
    projects = Projects(client, url, token)

    # query for project code
    response = projects.list_graphql(None, proj_code)
    check_response(response)
    proj_content = json.loads(response.content)
    proj_data = proj_content['data']['allProjects']['edges']

    # there should only be one entry - check and confirm, else return None
    if (len(proj_data) == 1):
        proj_id = projects.decode_id(proj_data[0]['node']['id'])
        return proj_id
    else:
        return

# return a project's formal code given an observation id
def get_observation_project_code(obs_id, client, url, token):

    # setup PSRDB tables
    observations = Observations(client, url, token)

    # query for observation id
    response = observations.list_graphql(obs_id, None, None, None, None, None, None, None, None, None, None)
    check_response(response)
    obs_content = json.loads(response.content)
    obs_data = obs_content['data']['observation']

    # check the obs_id was valid
    if not (obs_data == None):
        proj_code = obs_data['project']['code']
        return proj_code
    else:
        return

# return a timedelta object for an embargo timespan given a project id
def get_project_embargo(pid, client, url, token):

    # setup PSRDB tables
    projects = Projects(client, url, token)

    # query for pid
    response = projects.list_graphql(pid, None)
    check_response(response)
    proj_content = json.loads(response.content)
    proj_data = proj_content['data']['project']

    # check if pid was valid and return
    if not (proj_data == None):
        embargo_period = proj_data['embargoPeriod']
        return pd.to_timedelta(embargo_period)
    else:
        return

# returns the job state of a processing entry from PSRDB
def get_job_state(proc_id, client, url, token):

    # setup PSRDB tables
    processings = Processings(client, url, token)

    # query for proc id
    response = processings.list_graphql(proc_id, None, None, None)
    check_response(response)
    proc_content = json.loads(response.content)
    proc_data = proc_content['data']['processing']

    # check for valid proc id
    if not (proc_data == None):
        job_state = json.loads(proc_data['jobState'].replace("'", '"'))['job_state']
        return job_state
    else:
        return

# returns the slurm job id of a processing entry from PSRDB
def get_slurm_id(proc_id, client, url, token):

    # setup PSRDB tables
    processings = Processings(client, url, token)

    # query for proc id
    response = processings.list_graphql(proc_id, None, None, None)
    check_response(response)
    proc_content = json.loads(response.content)
    proc_data = proc_content['data']['processing']

    # check for valid proc id
    if not (proc_data == None):
        job_id = json.loads(proc_data['jobOutput'].replace("'", '"'))['job_id']
        return job_id
    else:
        return

# ----- CREATE FUNCTIONS -----

# creates a processing entry with the specified parameters, or returns one if it already exists
def create_processing(obs_id, pipe_id, parent_id, location, client, url, token, logger):

    # setup PSRDB tables
    pipelines = Pipelines(client, url, token)
    processings = Processings(client, url, token)
    processings.set_field_names(True, False)

    location = os.path.normpath(location)

    # recall all observations with matching parameters as best we can
    response = processings.list_graphql(None, obs_id, location, None)
    check_response(response)
    proc_content = json.loads(response.content)
    proc_data = proc_content['data']['allProcessings']['edges']
    
    # check if any of the remaining parameters match
    match_counter = 0
    for x in range(0, len(proc_data)):

        proc_parent_id = pipelines.decode_id(proc_data[x]['node']['parent']['id'])
        proc_pipe_id = pipelines.decode_id(proc_data[x]['node']['pipeline']['id'])

        if (proc_pipe_id == pipe_id and proc_parent_id == parent_id):

            proc_id = processings.decode_id(proc_data[x]['node']['id'])
            match_counter = match_counter + 1

    if (match_counter == 1):
        # return existing processing
        logger.info("Found existing entry in 'processings' matching launch parameters, ID = %s" % (str(proc_id)))
        retval = proc_id
    elif (match_counter == 0):
        # create and return new processing with default parameters
        response = processings.create(
            obs_id,
            pipe_id,
            parent_id,
            utc_date2psrdb(datetime.datetime.now().replace(microsecond=0)),
            location,
            json.dumps({}),
            json.dumps({}),
            json.dumps({}),
            #psrdb_json_formatter({}),
            #psrdb_json_formatter({}),
            #psrdb_json_formatter({}),
        )
        proc_content = json.loads(response.content)
        proc_id = proc_content['data']['createProcessing']['processing']['id']
        logger.info("Creating new entry in 'processings', ID = %s" % (str(proc_id)))
        retval = proc_id
    else:
        # Houston, we have a problem
        logger.error("Multiple entries in 'processings' matching launch parameters - requires resolution")
        retval = None

    return retval

# creates an ephemeris entry in PSRDB, but checks to see if a matching entry already exists
# and if so, will use the existing entry instead
# returns the ID of the relevant entry
def create_ephemeris(psrname, eph, dm, rm, cparams, client, logger):

    logger.info("Checking for ephemeris for {0} as part of TOA generation...".format(psrname))

    # set up PSRDB tables
    ephemerides = Ephemerides(client, cparams["db_url"], cparams["db_token"])

    # set up relevant query parameters
    psr_id = get_pulsar_id(psrname, client, cparams["db_url"], cparams["db_token"])

    # recall matching entries in Ephemerides table and check for equivalence
    response = ephemerides.list_graphql(None, psr_id, None, float(dm), float(rm))
    check_response(response)
    eph_content = json.loads(response.content)
    eph_data = eph_content['data']['allEphemerides']['edges']

    # EDIT COMPLETE UP TO HERE - FIX THIS NEXT WEEK

    # need to introduce a loop to catch any simultaneous writes to the database to avoid ephemeris duplication
    success = False
    counter = 0

    while not (success) and (counter < 3):

        if (counter == 3):
            raise Exception("Stalement detected in processing ID {0}: unable to access PSRDB due to conflict with simulataneous job. Please relaunch this job." % (cparams["db_proc_id"]))

        counter = counter + 1

        # scroll until a match is found
        match = False
        for x in range(0, len(eph_data)):
            check_json = json.loads(eph_data[x]['node']['ephemeris'])
            if (check_json == eph.ephem):
                match = True
                break

        # check for match, otherwise create a new entry
        if (match):
            retval = ephemerides.decode_id(eph_data[x]['node']['id'])
            success = True
            logger.info("Match found, ephemeris ID = {0}".format(retval))
        else:
            # get the required parameters
            created_at = util_time.get_current_time()
            created_by = getpass.getuser()
            comment = "Entry created as part of MeerPIPE - Pipeline ID {0} (Project {1})".format(cparams["db_pipe_id"], cparams["pid"])

            # check if the ephemeris has its own start/end fields
            if 'START' in eph.ephem and 'FINISH' in eph.ephem:
                start = astrotime(float(eph.ephem['START']['val']), format='mjd', scale='utc').datetime.replace(microsecond=0)
                finish = astrotime(float(eph.ephem['FINISH']['val']), format='mjd', scale='utc').datetime.replace(microsecond=0)
                valid_from = utc_date2psrdb(start)
                valid_to = utc_date2psrdb(finish)
            else:
                valid_from = util_time.get_time(0)
                valid_to = util_time.get_time(4294967295)

            # double check for simultaneous writes
            prev_len = len(eph_data)
            response = ephemerides.list_graphql(None, psr_id, None, float(dm), float(rm))
            check_response(response)
            eph_content = json.loads(response.content)
            eph_data = eph_content['data']['allEphemerides']['edges']

            if (len(eph_data) == prev_len):
                # finally, if no new ephemeris has been written by now, create!
                response = ephemerides.create(
                    psr_id,
                    created_at,
                    created_by,
                    json.dumps(eph.ephem),
                    eph.p0,
                    dm,
                    rm,
                    comment,
                    valid_from,
                    valid_to,
                )
                eph_content = json.loads(response.content)
                eph_id = eph_content['data']['createEphemeris']['ephemeris']['id']
                retval = eph_id
                success = True
                logger.info("No match found, new ephemeris entry created, ID = {0}".format(retval))

    return retval

# creates a template entry in PSRDB, but checks to see if a matching entry already exists
# and if so, will use the existing entry instead
# returns the ID of the relevant entry
def create_template(psrname, template, cparams, client, logger):

    logger.info("Checking for templates for {0} as part of TOA generation...".format(psrname))

    # set up PSRDB tables
    templates = Templates(client, cparams["db_url"], cparams["db_token"])

    # extract relevant template info
    comm = "vap -c bw,freq {0}".format(template)
    args = shlex.split(comm)
    proc = subprocess.Popen(args,stdout=subprocess.PIPE)
    proc.wait()
    info = proc.stdout.read().decode("utf-8").split("\n")
    bw = info[1].split()[1]
    freq = info[1].split()[2]

    # recall matching entries in Templates table and check for equivalence
    psr_id = get_pulsar_id(psrname, client, cparams["db_url"], cparams["db_token"])
    response = templates.list_graphql(None, psr_id, float(freq), float(bw))
    check_response(response)
    template_content = json.loads(response.content)
    template_data = template_content['data']['allTemplates']['edges']

    # need to introduced a loop to catch any simultaneous writes to the database to avoid ephemeris duplication
    success = False
    counter = 0

    while not (success) and (counter < 3):

        if (counter == 3):
            raise Exception("Stalement detected in processing ID {0}: unable to access PSRDB due to conflict with simulataneous job. Please relaunch this job." % (cparams["db_proc_id"]))

        counter = counter + 1

        # scroll until a match is found
        match = False
        location = os.path.normpath(template)
        for x in range(0, len(template_data)):
            # check for location match
            if (template_data[x]['node']['location'] == template):
                match = True
                break

        # check for match, otherwise create a new entry
        if (match):
            retval = templates.decode_id(template_data[x]['node']['id'])
            success = True
            logger.info("Match found, template ID = {0}".format(retval))
        else:
            # get the required parameters
            created_at = util_time.get_current_time()
            created_by = getpass.getuser()
            temp_method = "Unknown" # (?)
            # get some extra template info
            ext = template.split(".")[len(template.split(".")) - 1]
            if (ext == "std"):
                comm = "vap -c nchan,nsub,npol,nbin {0}".format(template)
                args = shlex.split(comm)
                proc = subprocess.Popen(args,stdout=subprocess.PIPE)
                proc.wait()
                info = proc.stdout.read().decode("utf-8").split("\n")
                nchan = info[1].split()[1]
                nsub = info[1].split()[2]
                npol = info[1].split()[3]
                nbin = info[1].split()[4]
                temp_type = "Nchan = {0} | Nsub = {1} | Npol = {2} | Nbin = {3}".format(nchan, nsub, npol, nbin)
            else:
                temp_type = "Unknown"
            comment = "Entry created as part of MeerPIPE - Pipeline ID {0} (Project {1})".format(cparams["db_pipe_id"], cparams["pid"])

            # double check for simultaneous writes
            prev_len = len(template_data)
            response = templates.list_graphql(None, psr_id, float(freq), float(bw))
            check_response(response)
            template_content = json.loads(response.content)
            template_data = template_content['data']['allTemplates']['edges']

            if (len(template_data) == prev_len):
                # finally, if no new ephemeris has been written by now, create!
                response = templates.create(
                    psr_id,
                    freq,
                    bw,
                    created_at,
                    created_by,
                    location,
                    temp_method,
                    temp_type,
                    comment,
                )
                template_content = json.loads(response.content)
                template_id = template_content['data']['createTemplate']['template']['id']
                retval = template_id
                success = True
                logger.info("No match found, new template entry created, ID = {0}".format(retval))

    return retval

# ----- UPDATE FUNCTIONS -----

# update the content of a processing entry, using whatever values are provided ('None' if not provided)
# this is going to be dirty as heck, until AJ specifies if there's a better way to do this
def update_processing(proc_id, obs_id, pipe_id, parent_id, embargo_end, location, job_state, job_output, results, client, url, token):

    # setup PSRDB tables
    processings = Processings(client, url, token)
    processings.set_field_names(True, False)

    # query for proc_id
    response = processings.list_graphql(proc_id, None, None, None)
    check_response(response)
    proc_content = json.loads(response.content)
    proc_data = proc_content['data']['processing']

    # if valid, update
    if not (proc_data == None):
        
        # check for parameters
        if (obs_id == None):
            obs_id = processings.decode_id(proc_data['observation']['id'])
        if (pipe_id == None):
            pipe_id = processings.decode_id(proc_data['pipeline']['id'])
        if (parent_id == None):
            parent_id = processings.decode_id(proc_data['parent']['id'])
        if (embargo_end == None):
            embargo_end = proc_data['embargoEnd']
        if (location == None):
            location = proc_data['location']
        if (job_state == None):
            job_state = json.dumps(proc_data['jobState'])
        if (job_output == None):
            job_output = json.dumps(proc_data['jobOutput'])
        if (results == None):
            results = json.dumps(proc_data['results'])

        # update the entry
        processings.update_variables = {
            "id": proc_id,
            "observation_id": obs_id,
            "pipeline_id": pipe_id,
            "parent_id": parent_id,
            "embargo_end": embargo_end,
            "location": location,
            "job_state": job_state,
            "job_output": job_output,
            "results": results,
        }
        response = processings.update_graphql()
        check_response(response)
        update_content = json.loads(response.content)
        update_data = update_content['data']['updateProcessing']['processing']
        update_id = update_data['id']
        return update_id
    else:
        return

# ----- UNSORTED / NON-UPDATED FUNCTIONS -----

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
