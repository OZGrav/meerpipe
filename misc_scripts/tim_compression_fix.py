#!/usr/bin/env python
"""

Code to fix the compression of tim pipelinefiles from .tar.gz to .gz only

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
import shlex
import numpy as np
import time

# PSRDB imports
from tables import *
from joins import *
from graphql_client import GraphQLClient
#sys.path.append('/fred/oz005/users/acameron/pipeline_stuff/andrew_meerpipe_dev/meerpipe/')
from db_utils import (check_response, update_pipelinefile)

# Important paths
PSRDB = "psrdb.py"

# Argument parsing
parser = argparse.ArgumentParser(description="Fix compression of .tim pipelinefiles, converting from .tar.gz to .gz only. USE WITH CAUTION.")
parser.add_argument("-infile", dest="infile", type=str, help="List of processing IDs to fix.", required=True)
parser.add_argument("-errfile", dest="errfile", type=str, help="File used to log errors.")
args = parser.parse_args()


# -- MAIN PROGRAM --

# PSRDB setup
env_query = 'echo $PSRDB_TOKEN'
token = str(subprocess.check_output(env_query, shell=True).decode("utf-8")).rstrip()
env_query = 'echo $PSRDB_URL'
url = str(subprocess.check_output(env_query, shell=True).decode("utf-8")).rstrip()
client = GraphQLClient(url, False)

# check valid input
if not (os.path.isfile(args.infile)):
    raise Exception("Input file does not exist - aborting.")

# PSRDB setup
processings = Processings(client, url, token)
pipelinefiles = Pipelinefiles(client, url, token)

# error setup
error_handle = open(args.errfile, 'w')

# read list
proc_list = np.loadtxt(args.infile, dtype=int, ndmin=1)

# scroll through list of processing IDs
for x in range(0, len(proc_list)):

    print ("Fixing PROC_ID {}...".format(proc_list[x]))

    # recall the processing ID
    response = processings.list(proc_list[x])
    check_response(response)
    proc_content = json.loads(response.content)
    proc_data = proc_content['data']['processing']

    # check for unique entry
    if not (proc_data == None):

        # get the location of the processing
        location = proc_data['location']

        # get any matching pipeline files
        response = pipelinefiles.list(None, None, int(proc_list[x]))
        check_response(response)
        pipefile_content = json.loads(response.content)
        pipefile_data = pipefile_content['data']['allPipelinefiles']['edges']

        # find any files with a .tar.gz extension
        ext = '.tar.gz'

        for y in pipefile_data:

            pf = y['node']['file'].split("/")
            fn = pf[len(pf) - 1]
            pipefile_id = pipelinefiles.decode_id(y['node']['id'])

            if ext in fn:

                # extension match - check if a cross match exists in the processing directory
                target_tg_location = os.path.join(location, "images")
                target_tg_file = os.path.join(target_tg_location, fn)

                if os.path.exists(target_tg_file):

                    print ("Identified file to be updated - {} (ID: {})".format(target_tg_file, pipefile_id))

                    # we have found a real match for the file
                    # delete the .tar.gz file, recompress the original file as a .gz file
                    # then re-upload it with the correct file type

                    # change into the relevant directory
                    print ("Changing directory to {}".format(target_tg_location))
                    os.chdir(target_tg_location)

                    # delete the .tar.gz file if safe
                    original_file = fn.replace(ext, '.tim')
                    print ("Original file = {}".format(original_file))

                    if os.path.exists(os.path.join(target_tg_location, original_file)):
                        # safe to delete .tar.gz file
                        os.remove(target_tg_file)

                        # compress the original file
                        timzip = "{}.gz".format(original_file)
                        comm = "gzip -9 {0}".format(original_file)
                        args = shlex.split(comm)
                        proc = subprocess.Popen(args,stdout=subprocess.PIPE)
                        proc.wait()

                        # update the entry in the database
                        filetype = y['node']['fileType']
                        filename = os.path.join(target_tg_location, timzip)

                        print ("Replacing pipelinefile {} with: {} ({})".format(pipefile_id, filename, filetype))

                        success = False
                        tries = 0
                        max_tries = 3

                        while (not success) and (tries < max_tries):

                            try:
                                update_id = update_pipelinefile(
                                    int(pipefile_id),
                                    str(filename),
                                    str(filetype),
                                    int(proc_list[x]),
                                    client,
                                    url,
                                    token
                                )
                            except:
                                tries = tries + 1
                                print ("PSRDB pipelinefiles update failure (try {}/{})- retrying...".format(tries, max_tries))
                                time.sleep(1)
                            else:
                                success = True

                        if not success:
                            err_str = "Unable to update pipelinefile ID {} - proc ID {}".format(pipefile_id, proc_list[x])
                            error_handle.write(err_str)
                            error_handle.close()
                            raise Exception(err_str)

                        if (update_id != int(pipefile_id)) or (update_id == None):
                            print ("WARNING: Failure to update 'pipelinefiles' entry {0}.".format(str(pipefile_id)))
                            error_handle.write("Failure to update 'pipelinefiles' entry {0} for processing {1}.\n".format(str(pipefile_id), str(proc_list[x])))
                        else:
                            print ("Updated pipelinefile {0}".format(pipefile_id))

                    else:
                        # not safe - report error
                        error_handle.write("Not safe to delete {} for processing {} - check and retry.\n".format(fn, proc_list[x]))
                        continue


    else:
        error_handle ("Invalid processing ID - {0}".format(proc_list[x]))
        continue

error_handle.close()

print ("Script complete.")
