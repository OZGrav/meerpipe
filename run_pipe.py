#!/usr/bin/env python
"""
MeerPipe: Processing pipeline for pulsar timing data.

__author__ = "Aditya Parthasarathy"
__copyright__ = "Copyright 2019, MeerTime"
__credits__ = ["Renee Spiewak", "Daniel Reardon", "Andrew Cameron"]
__license__ = "Public Domain"
__version__ = "0.2"
__maintainer__ = "Aditya Parthasarathy"
__email__ = "adityapartha3112@gmail.com"
__status__ = "Development"

"""

"""
Top-level code to execute the pipeline. 
"""

#Basic imports
import os
import sys
import subprocess
import shlex
import argparse
import os.path
import numpy as np
import logging
import glob
import time
import pandas as pd
import pickle
import json

#Importing pipeline utilities
from initialize import parse_config,create_structure,get_outputinfo, \
    setup_logging

from archive_utils import decimate_data,mitigate_rfi,generate_toas,add_archives, calibrate_data, fluxcalibrate, dynamic_spectra, cleanup, generate_summary

from db_utils import utc_normal2psrdb,utc_psrdb2normal,utc_normal2date,utc_psrdb2date,utc_date2normal,utc_date2psrdb,pid_getofficial,pid_getshort,list_psrdb_query,write_obs_list,get_pulsar_id,get_observation_id,get_project_embargo,get_observation_project_code,get_project_id,job_state_code,create_psrdb_query,update_psrdb_query,get_node_name,psrdb_json_formatter

# DB path
PSRDB = "psrdb.py"

#Argument parsing
parser = argparse.ArgumentParser(description="Run MeerPipe")
parser.add_argument("-cfile", dest="configfile", help="Path to the configuration file")
parser.add_argument("-dirname", dest="dirname", help="Process a specified observation")
parser.add_argument("-utc", dest="utc", help="Process a particular UTC. Should be in conjunction with a pulsar name")
parser.add_argument("-batch", dest="batch", help="Enable batch processing - multiple files")
parser.add_argument("-slurm", dest="slurm", help="Processes using Slurm",action="store_true")
parser.add_argument("-pid",dest="pid",help="Process pulsars as PID (Ignores original PID)")
parser.add_argument("-forceram", dest="forceram", help="Force RAM to this value. Automatic allocation is ignored")
parser.add_argument("-verbose", dest="verbose", help="Enable verbose terminal logging",action="store_true")
parser.add_argument("-db", dest="db_flag", help="Toggle PSRDB functionality (e.g. launching from / writing to PSRDB)", action="store_true")
parser.add_argument("-db_pipe", dest="db_pipe", help="PSRDB ID of the pipeline being launched")
args = parser.parse_args()

#Parsing the configuration file
config_params = parse_config(str(args.configfile))

#Checking validity of the input and output paths
if not os.path.exists(config_params["input_path"]):
    print ("Input path not valid. Quitting.")
    sys.exit()
if not os.path.exists(config_params["output_path"]):
    print ("Output path not valid. Quitting.")
    sys.exit()
    
#setting up the logger for this instance of the pipeline run
if not args.slurm:
    if args.dirname:
        logger = setup_logging(os.path.join(config_params["output_path"],str(args.dirname)),args.verbose,False)
    else:
        logger = setup_logging(config_params["output_path"],args.verbose,False)

else:
    logger = setup_logging(config_params["output_path"],args.verbose,False)


############ Analysis ##############
logger.info("Logger setup")
logger.info ("User:{0}".format(config_params["user"]))

#Adding filename as config_param if specified
if args.dirname:
    config_params["batch"] = "none"
    config_params["dirname"] = str(os.path.join(config_params["input_path"],str(args.dirname)))
    if not os.path.exists(config_params["dirname"]):
        logger.info("Specified directory path: {0}, does not exist. Quitting.".format(config_params["dirname"]))
        sys.exit()
    else:
        if args.utc:
            config_params["utc"] = str(os.path.join(config_params["dirname"],str(args.utc)))
            if not os.path.exists(config_params["utc"]):
                logger.info("Specified UTC directory path: {0} does not exist. Quitting.".format(config_params["utc"]))
                sys.exit()
            else:
                logger.info("Processing {0}".format(config_params["utc"]))
        else:
            config_params["utc"] = "none"
            logger.info("Processing {0}".format(config_params["dirname"]))
        
        toggle = True 

elif args.batch:
    logger.info("Batch processing enabled")
    config_params["dirname"]= str(args.batch)
    config_params["batch"] = "batch"
    toggle=True

if (args.db_flag):
    env_query = 'echo $PSRDB_TOKEN'
    PSRDB_TOKEN = str(subprocess.check_output(env_query, shell=True).decode("utf-8"))

if toggle:

    logger.info("Toggle set to True")
    #Gather information on directory structures
    if args.pid:
        config_params["pid"] = str(args.pid)

    output_info,archive_list,psrnames,proposal_ids,required_ram_list,obstime_list = get_outputinfo(config_params,logger)

    #For each input directory, an equivalent output directory is created using create_MTstructure.
    for obs_num, output_dir in enumerate(output_info):
        logger.info ("############")

        config_params["pid"] = proposal_ids[obs_num]
        obstime = obstime_list[obs_num]
        required_ram = required_ram_list[obs_num]

        if args.forceram:
            required_ram = int(args.forceram)
            logger.warning("Forcing RAM to be {0}".format(required_ram))

        #Creating output directories for saving the data products
        create_structure(output_dir,config_params,psrnames[obs_num],logger)

        # setup the DB processing entry to record results if the DB flag is set
        if (args.db_flag):
            
            # - observation ID
            split_path = output_info[obs_num].split("/")
            path_args = len(split_path)
            obs_utc = split_path[path_args - 4]
            obs_id = get_observation_id(obs_utc, psrnames[obs_num])
            # - pipeline ID
            pipe_query = "%s pipelines list --id %s" % (PSRDB, args.db_pipe)
            pipe_data = list_psrdb_query(pipe_query)
            if (len(pipe_data) != 2):
                raise Exception("Invalid pipeline ID (%s), not found in PSRDB table 'pipelines'" % (args.db_pipe))
            # - parent ID - id of PTUSE pipeline
            parent_id = str(1)
            # - embargo_end - tied to obs_id
            project_id = get_project_id(get_observation_project_code(obs_id))
            embargo_period = get_project_embargo(project_id)
            embargo_end = utc_date2psrdb(utc_normal2date(obs_utc) + embargo_period)
            # - location - this is just output_info[obs_num]
            # - job_state
            job_state = psrdb_json_formatter(job_state_code(0))
            # - job_output - for the moment an empty JSON string
            job_output = psrdb_json_formatter({})
            # - results - for the moment an empty JSON string
            results = psrdb_json_formatter({})

            # information has been compiled - seed the database entry and record the resulting ID
            proc_query = "%s processings create %s %s %s %s %s %s %s %s" % (PSRDB, obs_id, args.db_pipe, parent_id, output_info[obs_num], embargo_end, job_state, job_output, results)
            proc_id = create_psrdb_query(proc_query)
            logger.info("Created PSRDB entry in 'processings' table, ID = {0}".format(proc_id))
            
            # seed the processing id into the config params
            config_params["db_proc_id"] = proc_id
        
        if args.slurm:
            logger.info("Creating and submitting pipeline jobs using Slurm")
            logger.info("Observation length is {0}. Using RAM of {1} to process".format(obstime,required_ram))
            np.save(os.path.join(output_dir,"archivelist"),archive_list[obs_num])
            np.save(os.path.join(output_dir,"output"),output_info[obs_num])
            np.save(os.path.join(output_dir,"psrname"),psrnames[obs_num])
            with open(os.path.join(output_dir,"config_params.p"), 'wb') as pckl:
                pickle.dump(config_params, pckl, protocol=pickle.HIGHEST_PROTOCOL)
            pckl.close()
            job_name = "{0}_{1}.bash".format(psrnames[obs_num],obs_num)
            #mysoft_path = "/fred/oz005/meerpipe" - TEMP SWITCH FOR LOCAL TESTING - ADC
            mysoft_path = "/fred/oz005/users/acameron/pipeline_stuff/andrew_meerpipe_dev/meerpipe"
            with open(os.path.join(output_dir,str(job_name)),'w') as job_file:
                job_file.write("#!/bin/bash \n")
                job_file.write("#SBATCH --job-name={0}_{1} \n".format(psrnames[obs_num],obs_num))
                job_file.write("#SBATCH --output={0}meerpipe_out_{1}_{2} \n".format(str(output_dir),psrnames[obs_num],obs_num))
                job_file.write("#SBATCH --ntasks=1 \n")
                job_file.write("#SBATCH --mem={0} \n".format(required_ram))
                job_file.write("#SBATCH --time=01:00:00 \n")
                #job_file.write("#SBATCH --reservation=oz005_obs \n")
                #job_file.write("#SBATCH --account=oz005 \n")
                #job_file.write("#SBATCH --mail-type=FAIL --mail-user=adityapartha3112@gmail.com,andrewcameron@swin.edu.au \n")
                job_file.write("#SBATCH --mail-type=FAIL --mail-user=andrewcameron@swin.edu.au \n")
                job_file.write('cd {0} \n'.format(mysoft_path))
                job_file.write("source env_setup.sh\n")
                if (args.db_flag):
                    job_file.write("export PSRDB_TOKEN={0} \n".format(PSRDB_TOKEN))
                job_file.write("source /home/acameron/virtual-envs/meerpipe_db/bin/activate\n")
                job_file.write("python slurm_pipe.py -obsname {0}archivelist.npy -outputdir {0}output.npy -psrname {0}psrname.npy".format(output_dir))

            logger.info("Slurm job - {0} created".format(job_name))

            logger.info("Deploying {0}".format(job_name))
            com_sbatch = 'sbatch {0}'.format(os.path.join(output_dir,str(job_name)))
            args_sbatch = shlex.split(com_sbatch)
            proc_sbatch = subprocess.Popen(args_sbatch, stdout=subprocess.PIPE)

            if (args.db_flag):
                # wait for the job to be launched
                proc_sbatch.wait()
                # grab the submitted jobid
                out = proc_sbatch.stdout.read().decode("utf-8")
                jobid = out.split("\n")[0].split(" ")[3]

                # update the processing entry in PSRDB
                job_state = psrdb_json_formatter(job_state_code(1))
                job_output = psrdb_json_formatter(json.dumps({"job_id": jobid, "job_node": "Unallocated"}))
                proc_query = "%s processings update %s %s %s %s %s %s %s %s %s" % (PSRDB, proc_id, obs_id, args.db_pipe, parent_id, output_info[obs_num], embargo_end, job_state, job_output, results)
                update_psrdb_query(proc_query)
                logger.info("Updated PSRDB entry in 'processings' table, ID = {0}".format(proc_id))

            else:
                time.sleep(1)

            logger.info("{0} deployed.".format(job_name))

            """
            logger.info("Cleaning up")
            os.remove(os.path.join(output_dir,"archivelist.npy"))
            os.remove(os.path.join(output_dir,"output.npy"))
            os.remove(os.path.join(output_dir,"psrname.npy"))
            """

        else:
            
            # describes pipeline operation on a host node, not being routed to the OZSTAR HPC nodes

            if (args.db_flag):

                # update the processing entry in PSRDB
                job_state = psrdb_json_formatter(job_state_code(2))
                # get the name of the current node
                node_name = get_node_name()
                job_output = psrdb_json_formatter(json.dumps({"job_id": "N/A", "job_node": node_name}))
                proc_query = "%s processings update %s %s %s %s %s %s %s %s %s" % (PSRDB, proc_id, obs_id, args.db_pipe, parent_id, output_info[obs_num], embargo_end, job_state, job_output, results)
                update_psrdb_query(proc_query)            
                logger.info("Updated PSRDB entry in 'processings' table, ID = {0}".format(proc_id))

            #Add the archive files per observation directory into a single file
            added_archives = add_archives(archive_list[obs_num],output_dir,config_params,psrnames[obs_num],logger)
            logger.info("Added archive: {0}".format(added_archives))
 
            if not config_params["fluxcal"]:
                #Calibration 
                calibrated_archives = calibrate_data(added_archives,output_dir,config_params,logger)
                logger.info("Calibrated archives: {0}".format(calibrated_archives))
          
            if not config_params["fluxcal"]:
                #RFI zapping using coastguard on the calibrated archives
                cleaned_archives = mitigate_rfi(calibrated_archives,output_dir,config_params,psrnames[obs_num],logger)
                logger.info("Cleaned archives: {0}".format(cleaned_archives))
            elif config_params["fluxcal"]:
                #RFI cleaning the added archives
                cleaned_archives = mitigate_rfi(added_archives,output_dir,config_params,psrnames[obs_num],logger)
                logger.info("Cleaned archives: {0}".format(cleaned_archives))

            if not config_params["fluxcal"]:
                #Checking flags and creating appropriate data products
                processed_archives = decimate_data(cleaned_archives,output_dir,config_params,logger)
                #logger.info("Processed archives {0}".format(processed_archives))


                #Generating dynamic spectra from calibrated archives
                dynamic_spectra(output_dir,config_params,psrnames[obs_num],logger)


                #Flux calibrating the decimated data products
                fluxcalibrate(output_dir,config_params,psrnames[obs_num],logger)

                #Performing a clean up
                cleanup(output_dir, config_params, psrnames[obs_num], logger)
                
                #Forming ToAs from the processed archives
                generate_toas(output_dir,config_params,psrnames[obs_num],logger)

                #Generating summary file
                generate_summary(output_dir,config_params,psrnames[obs_num],logger)

                logger.info ("##############")

