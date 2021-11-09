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

# Important constants
SIMUL_WRITE_CHECKS = 3

# Function Definitions

# ----- MISC UTILITIES FUNCTIONS -----

# ROLE   : Convert a UTC date from "normal" to PSRDB format
# INPUTS : UTC date string (YYYY-MM-DD-HH:MM:SS)
# RETURNS: UTC date string (YYYY-MM-DDTHH:MM:SS+00:00)
def utc_normal2psrdb(d):

    dr = utc_normal2date(d)
    return utc_date2psrdb(dr)

# ROLE   : Convert a UTC date from PSRDB to "normal" format
# INPUTS : UTC date string (YYYY-MM-DDTHH:MM:SS+00:00)
# RETURNS: UTC date string (YYYY-MM-DD-HH:MM:SS)
def utc_psrdb2normal(d):

    dr = utc_psrdb2date(d)
    return utc_date2normal(dr)

# ROLE   : Convert a "normal" UTC date to a Datetime object
# INPUTS : UTC date string (YYYY-MM-DD-HH:MM:SS)
# RETURNS: Datetime object
def utc_normal2date(d):

    return  datetime.datetime.strptime(d, '%Y-%m-%d-%H:%M:%S')

# ROLE   : Convert a PSRDB UTC date to a Datetime object
# INPUTS : UTC date string (YYYY-MM-DDTHH:MM:SS+00:00)
# RETURNS: Datetime object
def utc_psrdb2date(d):

    return datetime.datetime.strptime(d, '%Y-%m-%dT%H:%M:%S+00:00')

# ROLE   : Convert a Datetime object to a "normal" UTC date
# INPUTS : Datetime object
# RETURNS: UTC date string (YYYY-MM-DD-HH:MM:SS)
def utc_date2normal(d):

    return "%s-%s" % (d.date(), d.time())

# ROLE   : Convert a Datetime object to a PSRDB UTC date
# INPUTS : Datetime object
# RETURNS: UTC date string (YYYY-MM-DDTHH:MM:SS+00:00)
def utc_date2psrdb(d):

    return "%sT%s+00:00" % (d.date(), d.time())

# ROLE   : Convert short-hand project codes to PSRDB/official project codes
#          Encoding taken from query_obs.py. List may be incomplete.
# INPUTS : String
# RETURNS: String
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

# ROLE   : Convert PSRDB/official project codes to short-hand project codes
#          Encoding taken from query_obs.py. List may be incomplete.
# INPUTS : String
# RETURNS: String
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

# ROLE   : Create a JSON object representing a given state of a pipeline job
# INPUTS : Integer
# RETURNS: JSON object
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
        state = "Failure" # job has finished without crashing, but unsuccessfully (missing output)
    elif jid == 5:
        state = "Unknown" # cover-all for indeterminate states / outcomes still under development (e.g. "fluxcal")
    elif jid == 6:
        state = "Crashed" # job broke down at some point before completing

    return json.loads(json.dumps({"job_state": state}))

# ROLE   : Return the name of the node on which a given job is operating
# INPUTS : None
# RETURNS: String
def get_node_name():

    info = "hostname"
    arg = shlex.split(info)
    proc = subprocess.Popen(arg,stdout=subprocess.PIPE)
    info_ret = proc.stdout.readline().rstrip().split()

    return str(info_ret[0].decode("utf-8"))

# ----- CHECK FUNCTIONS -----

# ROLE   : Checks that a response to a PSRDB query is valid.
# INPUTS : Response object
# RETURNS: None (success) | Exception (failure)
def check_response(response):

    valid = False

    # tests for validity - expand as neccessary
    if (response.status_code == 200):
        valid = True
    
    if not (valid):
        raise Exception("ERROR: Invalid GraphQL response detected (code %d)" % (response.status_code))

    return

# ROLE   : Checks if a Pipeline ID is valid, i.e. that it exists.
#        : May in future also check if pipeline is a MeerPIPE pipeline
# INPUTS : Integer, GraphQL client, String, String
# RETURNS: Boolean
def check_pipeline(pipe_id, client, url, token):

    # PSRDB setup
    pipelines = Pipelines(client, url, token)

    # Query for pipe_id
    response = pipelines.list(pipe_id, None)
    check_response(response)
    pipe_content = json.loads(response.content)

    # Check for validity
    if (pipe_content['data']['pipeline'] == None):
        retval = False
    else:
        retval = True

    return retval

# ----- GET FUNCTIONS -----

# ROLE   : Returns a unique Observation ID, given a pulsar name and PSRDB UTC
# INPUTS : String, String, GraphQL client, String, String
# RETURNS: Integer (success) | None (failure)
def get_observation_id(utc, psr, client, url, token):

    # PSRDB setup
    observations = Observations(client, url, token)
    pulsartargets = Pulsartargets(client, url, token)

    # Query for matching UTC
    response = observations.list(
        None, 
        None,
        None,
        None,
        None, 
        None,
        None,
        None,
        None,
        utc,
        utc
    )
    check_response(response)
    obs_content = json.loads(response.content)
    obs_data = obs_content['data']['allObservations']['edges']

    # Check for Targets matching the pulsar name
    match_counter = 0
    for x in range(0, len(obs_data)):
        target_name = obs_data[x]['node']['target']['name']
        response = pulsartargets.list(
            None,
            None,
            target_name,
            None,
            psr
        )
        check_response(response)
        target_content = json.loads(response.content)
        target_data = target_content['data']['allPulsartargets']['edges']

        obs_id = observations.decode_id(obs_data[x]['node']['id'])
        match_counter = match_counter + len(target_data)

    # Check for valid result
    if (match_counter == 1):
        return int(obs_id)
    else:
        return

# ROLE   : Returns a unique Folding ID, given an Observation ID and Pipeline ID
# INPUTS : Integer, Integer, GraphQL client, String, String
# RETURNS: Integer (success) | None (failure)
def get_folding_id(obs_id, pipe_id, client, url, token):

    # PSRDB setup
    processings = Processings(client, url, token)
    processings.set_field_names(True, False)
    pipelines = Pipelines(client, url, token)
    foldings = Foldings(client, url, token)

    # Query processings for matching obs_id
    response = processings.list(
        None,
        obs_id,
        None,
        None,
        None
    )
    check_response(response)
    proc_content = json.loads(response.content)
    proc_data = proc_content['data']['allProcessings']['edges']

    # Check for matching pipe_id
    match_counter = 0
    for x in range(0, len(proc_data)):

        if (pipelines.decode_id(proc_data[x]['node']['pipeline']['id']) == str(pipe_id)):

            response = foldings.list(
                None,
                processings.decode_id(proc_data[x]['node']['id']),
                None
            )
            check_response(response)
            fold_content = json.loads(response.content)
            fold_data = fold_content['data']['allFoldings']['edges']

            for y in range (0, len(fold_data)):
                match_counter = match_counter + 1
                fold_id = foldings.decode_id(fold_data[y]['node']['id'])

    # Check for valid result
    if (match_counter == 1):
        return int(fold_id)
    else:
        return

# ROLE   : Return a Pulsar ID given a pulsar J-name
# INPUTS : String, GraphQL client, String, String
# RETURNS: Integer (success) | None (failure)
def get_pulsar_id(psr, client, url, token):

    # PSRDB setup
    pulsars = Pulsars(client, url, token)

    # Query for matching jname
    response = pulsars.list(
        None,
        psr
    )
    check_response(response)
    psr_content = json.loads(response.content)
    psr_data = psr_content['data']['allPulsars']['edges']

    # Check for single matching entry and return ID
    if (len(psr_data) == 1):
        psr_id = pulsars.decode_id(psr_data[0]['node']['id'])
        return int(psr_id)
    else:
        return

# ROLE   : Return a Project ID given an official project code
#        : In future, may check for valid project code
# INPUTS : String, GraphQL client, String, String
# RETURNS: Integer (success) | None (failure)
def get_project_id(proj_code, client, url, token):

    # PSRDB setup
    projects = Projects(client, url, token)

    # Query for project code
    response = projects.list(
        None,
        None,
        proj_code
    )
    check_response(response)
    proj_content = json.loads(response.content)
    proj_data = proj_content['data']['allProjects']['edges']

    # Check for single matching entry and return ID
    if (len(proj_data) == 1):
        proj_id = projects.decode_id(proj_data[0]['node']['id'])
        return int(proj_id)
    else:
        return

# ROLE   : Return a Project official code for a given Observation ID
# INPUTS : Integer, GraphQL client, String, String
# RETURNS: String (success) | None (failure)
def get_observation_project_code(obs_id, client, url, token):

    # PSRDB setup
    observations = Observations(client, url, token)

    # Query for obs_id
    response = observations.list(
        obs_id,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None
    )
    check_response(response)
    obs_content = json.loads(response.content)
    obs_data = obs_content['data']['observation']

    # Check for valid obs_id
    if not (obs_data == None):
        proj_code = obs_data['project']['code']
        return proj_code
    else:
        return

# ROLE   : Return an embargo timespan given a Project ID
# INPUTS : Integer, GraphQL client, String, String
# RETURNS: Timedelta object (success) | None (failure)
def get_project_embargo(pid, client, url, token):

    # PSRDB setup
    projects = Projects(client, url, token)

    # Query for project ID
    response = projects.list(
        pid,
        None,
        None
    )
    check_response(response)
    proj_content = json.loads(response.content)
    proj_data = proj_content['data']['project']

    # Check for valid project ID
    if not (proj_data == None):
        embargo_period = proj_data['embargoPeriod']
        return pd.to_timedelta(embargo_period)
    else:
        return

# ROLE   : Return the job state of a given Processing ID
# INPUTS : Integer, GraphQL client, String, String
# RETURNS: JSON object (success) | None (failure)
def get_job_state(proc_id, client, url, token):

    # PSRDB setup
    processings = Processings(client, url, token)

    # Query for proc_id
    response = processings.list(
        proc_id,
        None,
        None,
        None,
        None
    )
    check_response(response)
    proc_content = json.loads(response.content)
    proc_data = proc_content['data']['processing']

    # Check for valid proc_id
    if not (proc_data == None):
        job_state = json.loads(proc_data['jobState'].replace("'", '"'))
        return job_state
    else:
        return

# ROLE   : Return the job output of a given Processing ID
# INPUTS : Integer, GraphQL client, String, String
# RETURNS: JSON object (success) | None (failure)
def get_job_output(proc_id, client, url, token):

    # PSRDB setup
    processings = Processings(client, url, token)

    # Query for proc_id
    response = processings.list(
        proc_id,
        None,
        None,
        None,
        None
    )
    check_response(response)
    proc_content = json.loads(response.content)
    proc_data = proc_content['data']['processing']

    # Check for valid proc_id
    if not (proc_data == None):
        job_output = json.loads(proc_data['jobOutput'].replace("'", '"'))
        return job_output
    else:
        return

# ROLE   : Return the results of a given Processing ID
# INPUTS : Integer, GraphQL client, String, String
# RETURNS: JSON object (success) | None (failure)
def get_results(proc_id, client, url, token):

    # PSRDB setup
    processings = Processings(client, url, token)

    # Query for proc_id
    response = processings.list(
        proc_id,
        None,
        None,
        None,
        None
    )
    check_response(response)
    proc_content = json.loads(response.content)
    proc_data = proc_content['data']['processing']

    # Check for valid proc_id
    if not (proc_data == None):
        results = json.loads(proc_data['results'].replace("'", '"'))
        return results
    else:
        return


# ----- CREATE FUNCTIONS -----

# ROLE   : Creates a Processing entry with the specified parameters.
#        : If a matching entry exists, that is returned instead.
# INPUTS : Integer, Integer, Integer, String, GraphQL client, String, String, Logger object
# RETURNS: Integer (success) | None (failure)
def create_processing(obs_id, pipe_id, parent_id, location, client, url, token, logger):

    # PSRDB setup
    pipelines = Pipelines(client, url, token)
    processings = Processings(client, url, token)
    processings.set_field_names(True, False)

    # Input sanitisation and preparation of defaults
    location = os.path.normpath(location)
    embargo_end = utc_date2psrdb(datetime.datetime.now().replace(microsecond=0))
    job_state = json.loads('{}')
    job_output = json.loads('{}')
    results = json.loads('{}')

    # Query for processings matching input parameters
    response = processings.list(
        None,
        obs_id,
        parent_id,
        location,
        None
    )
    check_response(response)
    proc_content = json.loads(response.content)
    proc_data = proc_content['data']['allProcessings']['edges']
    
    # Check for matches of non-query parameters
    match_counter = 0
    for x in range(0, len(proc_data)):
        proc_pipe_id = int(pipelines.decode_id(proc_data[x]['node']['pipeline']['id']))
        if (proc_pipe_id == pipe_id):
            proc_id = int(processings.decode_id(proc_data[x]['node']['id']))
            match_counter = match_counter + 1

    # Take action based on number of matching entries
    if (match_counter == 1):
        # Update and return existing processing
        logger.info("Found existing entry in 'processings' matching launch parameters, ID = {0}".format(proc_id))
        logger.info("Updating to default starting parameters")
        retval = int(proc_id)
        update_id = update_processing(
            proc_id,
            obs_id,
            pipe_id,
            parent_id,
            embargo_end,
            location,
            job_state,
            job_output,
            results, 
            client,
            url,
            token
        )
        if (update_id != proc_id) or (update_id == None):
            logger.error("Failure to update 'processings' entry ID {0} - PSRDB cleanup may be required.".format(retval))
        else:
            logger.info("Updated PSRDB entry in 'processings' table, ID = {0}".format(retval))
    elif (match_counter == 0):
        # Create new processing with default parameters
        response = processings.create(
            obs_id,
            pipe_id,
            parent_id,
            embargo_end,
            location,
            json.dumps(job_state),
            json.dumps(job_output),
            json.dumps(results),
        )
        proc_content = json.loads(response.content)
        proc_id = proc_content['data']['createProcessing']['processing']['id']
        logger.info("Creating new entry in 'processings', ID = {0}".format(proc_id))
        retval = int(proc_id)
    else:
        # Houston, we have a problem
        logger.error("Multiple entries in 'processings' matching launch parameters - requires resolution")
        retval = None

    return retval

# ROLE   : Creates a Ephemeris entry with the specified parameters.
#        : If a matching entry exists, that is returned instead.
# INPUTS : String, Ephemeris object, Float, Float, Dictionary, GraphQL client, Logger object
# RETURNS: Integer (success) | Exception (failure)
def create_ephemeris(psrname, eph, dm, rm, cparams, client, logger):

    logger.info("Checking for ephemeris for {0} as part of TOA generation...".format(psrname))

    # PSRDB setup
    ephemerides = Ephemerides(client, cparams["db_url"], cparams["db_token"])
    psr_id = get_pulsar_id(
        psrname,
        client,
        cparams["db_url"],
        cparams["db_token"]
    )

    # Input sanitisation
    dm = float(dm)
    rm = float(rm)

    # Query for matching Ephemerides records and check for equivalence
    response = ephemerides.list(
        None,
        psr_id,
        None,
        dm,
        rm
    )
    check_response(response)
    eph_content = json.loads(response.content)
    eph_data = eph_content['data']['allEphemerides']['edges']

    # Loop to catch simultaneous PSRDB access attempts, thereby avoiding duplication errors
    success = False
    counter = 0

    while not (success):

        if (counter == SIMUL_WRITE_CHECKS):
            raise Exception("Stalement detected in processing ID {0}: unable to modify PSRDB due to conflict with simulataneous job. Please relaunch this job." % (cparams["db_proc_id"]))

        counter = counter + 1

        # Check for matching eph field
        match = False
        for x in range(0, len(eph_data)):
            check_json = json.loads(eph_data[x]['node']['ephemeris'])
            if (check_json == eph.ephem):
                match = True
                break

        # Check for match, otherwise create a new entry
        if (match):
            retval = int(ephemerides.decode_id(eph_data[x]['node']['id']))
            success = True
            logger.info("Match found, ephemeris ID = {0}".format(retval))
        else:
            # Get required parameters
            created_at = util_time.get_current_time()
            created_by = getpass.getuser()
            comment = "Entry created as part of MeerPIPE - Pipeline ID {0} (Project {1})".format(cparams["db_pipe_id"], cparams["pid"])

            # Check for ephemeris START/FINISH fields
            if 'START' in eph.ephem and 'FINISH' in eph.ephem:
                start = astrotime(float(eph.ephem['START']['val']), format='mjd', scale='utc').datetime.replace(microsecond=0)
                finish = astrotime(float(eph.ephem['FINISH']['val']), format='mjd', scale='utc').datetime.replace(microsecond=0)
                valid_from = utc_date2psrdb(start)
                valid_to = utc_date2psrdb(finish)
            else:
                valid_from = util_time.get_time(0)
                valid_to = util_time.get_time(4294967295)

            # Double check for simultaneous writes
            prev_len = len(eph_data)
            response = ephemerides.list(
                None,
                psr_id,
                None,
                dm,
                rm
            )
            check_response(response)
            eph_content = json.loads(response.content)
            eph_data = eph_content['data']['allEphemerides']['edges']

            if (len(eph_data) == prev_len):
                # No write conflict detected
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
                retval = int(eph_id)
                success = True
                logger.info("No match found, new ephemeris entry created, ID = {0}".format(retval))

    return retval

# creates a TOA record linking a template, folding, ephemeris and processing
# if one already exists, returns the existing entry's ID


# ROLE   : Creates a TOA entry linking a template, folding, ephemeris and processing.
#        : If a matching entry exists, that is returned instead.
# INPUTS : Integer, Integer, JSON object, Float, Float, String[1], Float, Boolean, 
#          Dictionary, GraphQL client, Logger object
# RETURNS: Integer (success) | Exception (failure)
def create_toa_record(eph_id, template_id, flags, freq, mjd, site, uncertainty, quality, cparams, client, logger):

    logger.info("Checking for linked TOA entries as part of TOA generation...")

    # PSRDB setup
    toas = Toas(client, cparams["db_url"], cparams["db_token"])

    # Query for matching TOA records and check for equivalence
    response = toas.list(
        None,
        cparams["db_proc_id"],
        cparams["db_fold_id"],
        eph_id,
        template_id
    )
    check_response(response)
    toa_content = json.loads(response.content)
    toa_data = toa_content['data']['allToas']['edges']

    # Check for number of matching results
    if (len(toa_data) == 0):
        # If no entry exists, create one
        comment = "Entry created as part of MeerPIPE - Pipeline ID {0} (Project {1})".format(cparams["db_pipe_id"], cparams["pid"])
        if (quality == True):
            qual_code = "nominal"
        else:
            qual_code = "bad"

        response = toas.create(
            int(cparams["db_proc_id"]),
            int(cparams["db_fold_id"]),
            int(eph_id),
            int(template_id),
            json.dumps(flags),
            freq,
            mjd,
            site,
            uncertainty,
            qual_code,
            comment,
        )
        toa_content = json.loads(response.content)
        toa_id = toa_content['data']['createToa']['toa']['id']
        retval = toa_id
        logger.info("No match found, new TOA entry created, ID = {0}".format(retval))

    elif (len(toa_data) == 1):
        # Entry already exists - update and return
        retval = int(toas.decode_id(toa_data[0]['node']['id']))
        logger.info("Match found, TOA ID = {0}".format(retval))
        logger.info("Updating entry...")
        comment = "Entry updated as part of MeerPIPE - Pipeline ID {0} (Project {1})".format(cparams["db_pipe_id"], cparams["pid"])
        update_id = update_toa_record(
            retval,
            int(cparams["db_proc_id"]),
            int(cparams["db_fold_id"]),
            int(eph_id),
            int(template_id),
            flags,
            freq,
            mjd,
            site,
            uncertainty,
            quality,
            comment,
            client,
            cparams["db_url"],
            cparams["db_token"]
        )
        if (update_id != retval) or (update_id == None):
            logger.error("Failure to update 'toa' entry ID {0} - PSRDB cleanup may be required.".format(retval))
        else:
            logger.info("Updated PSRDB entry in 'toa' table, ID = {0}".format(retval))
    else:
        # Houston, we have a problem
        raise Exception("Multiple TOA entries found for combination of Proc {0}, Fold {1}, Eph {2}, Template {3}".format(cparams["db_proc_id"], cparams["db_fold_id"], eph_id, template_id))

    return retval

# ROLE   : Creates a Templates entry with the specified parameters.
#        : If a matching entry exists, that is returned instead.
# INPUTS : String, String, Dictionary, GraphQL client, Logger object
# RETURNS: Integer (success) | Exception (failure)
def create_template(psrname, template, cparams, client, logger):

    logger.info("Checking for templates for {0} as part of TOA generation...".format(psrname))

    # PSRDB setup
    templates = Templates(client, cparams["db_url"], cparams["db_token"])

    # Gather template info and sanitise
    comm = "vap -c bw,freq {0}".format(template)
    args = shlex.split(comm)
    proc = subprocess.Popen(args,stdout=subprocess.PIPE)
    proc.wait()
    info = proc.stdout.read().decode("utf-8").split("\n")
    bw = float(info[1].split()[1])
    freq = float(info[1].split()[2])
    location = os.path.normpath(template)

    # Query for matching Templates records and check for equivalence
    psr_id = get_pulsar_id(
        psrname,
        client,
        cparams["db_url"],
        cparams["db_token"]
    )
    response = templates.list(
        None,
        psr_id,
        float(freq),
        float(bw)
    )
    check_response(response)
    template_content = json.loads(response.content)
    template_data = template_content['data']['allTemplates']['edges']

    # Loop to catch simultaneous PSRDB access attempts, thereby avoiding duplication errors
    success = False
    counter = 0

    while not (success):

        if (counter == SIMUL_WRITE_CHECKS):
            raise Exception("Stalement detected in processing ID {0}: unable to modify PSRDB due to conflict with simulataneous job. Please relaunch this job." % (cparams["db_proc_id"]))

        counter = counter + 1

        # Check for matching location
        match = False
        for x in range(0, len(template_data)):
            # check for location match
            if (template_data[x]['node']['location'] == location):
                match = True
                break

        # Check for match, otherwise create a new entry
        if (match):
            retval = int(templates.decode_id(template_data[x]['node']['id']))
            success = True
            logger.info("Match found, template ID = {0}".format(retval))
        else:
            # Gather required parameters
            created_at = util_time.get_current_time()
            created_by = getpass.getuser()
            temp_method = "Unknown" # (?)
            # Extra template info
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

            # Double check for simultaneous writes
            prev_len = len(template_data)
            response = templates.list(
                None,
                psr_id,
                freq,
                bw
            )
            check_response(response)
            template_content = json.loads(response.content)
            template_data = template_content['data']['allTemplates']['edges']

            if (len(template_data) == prev_len):
                # No write conflict detected
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

# ROLE   : Creates a Pipelineimage entry with the specified parameters.
#        : If a matching entry exists, that is updated returned instead.
# INPUTS : String, String, Integer, Dictionary, GraphQL client, Logger object
# RETURNS: Integer (success) | Exception (failure)
def create_pipelineimage(image, image_type, rank, cparams, client, logger):

    logger.info("Checking for existing images matching {0} from processing {1}".format(image,cparams["db_proc_id"]))

    # PSRDB setup
    pipelineimages = Pipelineimages(client, cparams["db_url"], cparams["db_token"])
    
    # Sanitise input
    image = os.path.normpath(image)

    # Query matching Pipelineimages entries and check for equivalence
    response = pipelineimages.list(
        None,
        int(cparams["db_proc_id"])
    )
    check_response(response)
    pipeimage_content = json.loads(response.content)
    pipeimage_data = pipeimage_content['data']['allPipelineimages']['edges']

    # Check for matches based on image type
    matches = 0
    for x in range(0, len(pipeimage_data)):
        if (pipeimage_data[x]['node']['imageType'] == image_type):
            matches = matches +1
            pipeimage_id = pipelineimages.decode_id(pipeimage_data[x]['node']['id'])

    if (matches == 0):
        # if no entry exists, create one
        response = pipelineimages.create(
            image,
            image_type,
            int(rank),
            int(cparams["db_proc_id"]),
        )
        pipeimage_content = json.loads(response.content)
        pipeimage_id = pipeimage_content['data']['createPipelineimage']['pipelineimage']['id']
        retval = int(pipeimage_id)
        logger.info("No match found, new pipelineimage entry created, ID = {0}".format(retval))

    elif (matches == 1):
        # entry already exists - update and return
        logger.info("Match found, pipelineimage ID = {0}".format(pipeimage_id))
        retval = int(pipeimage_id)
        update_id = update_pipelineimage(
            retval,
            image,
            image_type,
            rank,
            int(cparams["db_proc_id"]),
            client,
            cparams["db_url"],
            cparams["db_token"]
        )
        #logger.info(update_id)
        if (update_id != retval) or (update_id == None):
            logger.error("Failure to update 'pipelineimages' entry ID {0} - PSRDB cleanup may be required.".format(retval))
        else:
            logger.info("Updated PSRDB entry in 'pipelineimages' table, ID = {0}".format(retval))
    else:
        # Houston, we have a problem
        raise Exception("Multiple 'pipelineimage' entries found for combination of processing ID {0} and filename {1}".format(cparams["db_proc_id"], image))
        retval = None

    return retval

# ----- UPDATE FUNCTIONS -----

# ROLE   : Update the content of an existing Processing entry.
#        : Unspecified parameters should be set to 'None'.
# INPUTS : Integer, Integer, Integer, Integer, String, String,
#          JSON object, JSON object, JSON object, GraphQL client, 
#          String, String
# RETURNS: Integer (success) | None (failure)
def update_processing(proc_id, obs_id, pipe_id, parent_id, embargo_end, location, job_state, job_output, results, client, url, token):

    # PSRDB setup
    processings = Processings(client, url, token)
    processings.set_field_names(True, False)

    # Query for proc_id
    response = processings.list(
        proc_id,
        None,
        None,
        None,
        None
    )
    check_response(response)
    proc_content = json.loads(response.content)
    proc_data = proc_content['data']['processing']

    # Check for valid proc_id
    if not (proc_data == None):
        
        # Check for parameters
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
            job_state = json.loads(proc_data['jobState'].replace("'", '"'))
            #job_state = json.dumps(proc_data['jobState'])
        if (job_output == None):
            job_output = json.loads(proc_data['jobOutput'].replace("'", '"'))
            #job_output = json.dumps(proc_data['jobOutput'])
        if (results == None):
            results = json.loads(proc_data['results'].replace("'", '"'))
            #results = json.dumps(proc_data['results'])

        # Sanitise
        location = os.path.normpath(location)

        # Update the entry
        response = processings.update(
            proc_id,
            obs_id,
            pipe_id,
            parent_id,
            embargo_end,
            location,
            json.dumps(job_state),
            json.dumps(job_output),
            json.dumps(results),
        )
        check_response(response)
        update_content = json.loads(response.content)
        update_data = update_content['data']['updateProcessing']['processing']
        update_id = update_data['id']
        return int(update_id)
    else:
        return

# ROLE   : Update the content of an existing TOA entry.
#        : Unspecified parameters should be set to 'None'.
# INPUTS : Integer, Integer, Integer, Integer, Integer, JSON object, Float, Float,
#          String[1], Float, Boolean, String, GraphQL client, String, String
# RETURNS: Integer (success) | None (failure)
def update_toa_record(toa_id, proc_id, fold_id, eph_id, template_id, flags, freq, mjd, site, uncertainty, quality, comment, client, url, token):

    # PSRDB setup
    toas = Toas(client, url, token)
    toas.set_field_names(True, False)

    # Query for toa_id
    response = toas.list(
        toa_id,
        None,
        None,
        None,
        None
    )
    check_response(response)
    toa_content = json.loads(response.content)
    toa_data = toa_content['data']['toa']

    # Check for valid toa_id
    if not (toa_data == None):

        # Check for parameters
        if (proc_id == None):
            proc_id = toas.decode_id(toa_data['processing']['id'])
        if (fold_id == None):
            fold_id = toas.decode_id(toa_data['inputFolding']['id'])
        if (eph_id == None):
            eph_id = toas.decode_id(toa_data['timingEphemeris']['id'])
        if (template_id == None):
            template_id = toas.decode_id(toa_data['template']['id'])
        if (flags == None):
            flags = json.loads(toa_data['flags'])
        if (freq == None):
            freq = toa_data['frequency']
        if (mjd == None):
            mjd = toa_data['mjd']
        if (site == None):
            site = toa_data['site']
        if (uncertainty == None):
            uncertainty = toa_data['uncertainty']
        if (quality == True):
            quality = "nominal"
        elif (quality == False):
            quality = "bad"
        elif (quality == None):
            quality = toa_data['quality']
        if (comment == None):
            comment = toa_data['comment']

        # Update the entry
        response = toas.update(
            toa_id,
            proc_id,
            fold_id,
            eph_id,
            template_id,
            json.dumps(flags),
            freq,
            mjd,
            site,
            uncertainty,
            quality,
            comment
        )
        check_response(response)
        update_content = json.loads(response.content)
        update_data = update_content['data']['updateToa']['toa']
        update_id = update_data['id']
        return int(update_id)
    else:
        return


# ROLE   : Update the content of an existing Pipelineimage entry.
#        : Unspecified parameters should be set to 'None'.
# INPUTS : Integer, String, String, Integer, Integer,
#          GraphQL client, String, String
# RETURNS: Integer (success) | None (failure)
def update_pipelineimage(image_id, image, image_type, rank, proc_id, client, url, token):

    # PSRDB setup
    pipelineimages = Pipelineimages(client, url, token)
    pipelineimages.set_field_names(True, False)
    processings = Processings(client, url, token)

    # Query for image_id
    response = pipelineimages.list(
        image_id,
        None
    )
    check_response(response)
    pipeimage_content = json.loads(response.content)
    pipeimage_data = pipeimage_content['data']['pipelineimage']

    # Check for validity and update
    if not (pipeimage_data == None):

        # Check for parameters
        if (image == None):
            image = pipeimage_data['image']
        if (proc_id == None):
            proc_id = processings.decode_id(pipeimage_data['processing']['id'])
        if (image_type == None):
            image_type = pipeimage_data['imageType']
        if (rank == None):
            rank = pipeimage_data['rank']

        # Sanitise
        image = os.path.normpath(image)

        # Update the entry
        response = pipelineimages.update(
            int(image_id),
            image,
            image_type,
            int(rank),
            int(proc_id)
        )
        check_response(response)
        update_content = json.loads(response.content)
        update_data = update_content['data']['updatePipelineimage']['pipelineimage']
        update_id = update_data['id']
        return int(update_id)
    else:
        return

# ----- UNSORTED / NON-UPDATED FUNCTIONS -----
# Note - most of these functions will eventually be deprecated once the remainder of the PSRDB
# code modifications have been overhauled / refined. Do not use these functions in future work.

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
