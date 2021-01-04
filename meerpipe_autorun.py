#!/usr/bin/env python

import os
import sys
import numpy as np
import glob
import argparse
import logging
import subprocess
import shlex

from initialize import parse_config, get_outputinfo

parser = argparse.ArgumentParser(description="Run MeerPipe automatically on new data")
parser.add_argument("-cfile", dest="configfile", help="Path to the configuration file")
parser.add_argument("-mwatch", dest="meerwatch", help="MeerWatch UTCs to run", action="store_true")
parser.add_argument("-view", dest="viewlist", help="Lists the unprocessed observations",action="store_true")
parser.add_argument("-run", dest="run", help="Submits jobs", action="store_true")
parser.add_argument("-verbose", dest="verbose", help="Enable verbose mode", action="store_true")
parser.add_argument("-jitter", dest="jitter", help="Use only pulsars in the jitter list", action='store_true')
parser.add_argument("-pid", dest="pid", help="Filter observations based on PID")

args = parser.parse_args()

mw=False
if args.meerwatch:
    mw=True

if args.pid:
    pid = str(args.pid)
else:
    pid = "all"


def get_obsheadinfo(obsheader_path):
    """
    Parse the obs.header file and return important parameters
    """
    params={}
    with open(obsheader_path) as file:
        lines=file.readlines()
        for line in lines:
            (key, val) = line.split()
            params[str(key)] = str(val).rstrip()

    file.close()
    return params


def get_pid(pid):
    #Routine to return a readable PID directory for the pipeline

    if pid == "SCI-20180516-MB-01":
        pid_dir = "MB01"
    elif pid == "SCI-20180516-MB-02":
        pid_dir = "TPA"
    elif pid == "SCI-20180516-MB-03":
        pid_dir = "RelBin"
    elif pid == "SCI-20180516-MB-04":
        pid_dir = "GC"
    elif pid == "SCI-20180516-MB-05":
        pid_dir = "PTA"
    elif pid == "SCI-20180516-MB-06":
        pid_dir = "NGC6440"
    elif pid == "SCI-20180516-MB-99":
        pid_dir = "fluxcal"
    elif pid == "None":
        pid_dir = "None"
    else:
        pid_dir = "Rogue"

    return pid_dir

def setup_logging(path,verbose,file_log):
    """
    Setup log handler - this logs in the terminal (if not run with --slurm).
    For slurm based runs - the logging is done by the job queue system

    """
    log_toggle=False

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if file_log == True:
        logfile = "MP_autorun.log"
        logger = logging.getLogger(logfile)
        logger.setLevel(logging.INFO)

        if not os.path.exists(path):
            os.makedirs(path)
        #Create file logging only if logging file path is specified
        fh = logging.FileHandler(os.path.join(path,logfile))
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        #Check if file already exists, if so, add a demarcator to differentiate among runs
        if os.path.exists(os.path.join(path, logfile)):
            with open(os.path.join(path,logfile), 'a') as f:
                f.write(20*"#")
                f.write("\n")
        logger.info("File handler created")
        log_toggle=True

    if verbose:
        #Create console handler with a lower log level (INFO)
        logfile = "MP_autorun.log"
        logger = logging.getLogger(logfile)
        logger.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        # add the handlers to the logger
        logger.addHandler(ch)
        logger.info("Verbose mode enabled")
        log_toggle=True

    if log_toggle:
        return logger
    else:
        return none

def run_meerpipe(utc):
    """
    Routine to execute meerpipe as a Slurm job on a particular UTC
    """
    print "Running meerpipe for {0}".format(utc)
    utcpath,utcname = os.path.split(utc)
    psrpath,psrname = os.path.split(utcpath)
    meerpipe = "python /fred/oz005/meerpipe/run_pipe.py -cfile {2} -dirname {0} -utc {1} -verbose -pid {3} -slurm".format(psrname,utcname,args.configfile,args.pid)
    proc_meerpipe = shlex.split(meerpipe)
    p_meerpipe = subprocess.Popen(proc_meerpipe)
    p_meerpipe.wait()
    print "MeerPipe job submitted for PSR:{0}, UTC:{1}".format(psrname,utcname)


def run_meerwatch(slurm_path,psrname):
    """
    Routine to execute meerwatch as a Slurm job on a particular pulsar
    """
    print "Running meerwatch for {0}".format(psrname)
    job_name = "{0}.bash".format(psrname)
    soft_path = "/fred/oz005/meerpipe/MeerWatch"
    with open(os.path.join(slurm_path,str(job_name)),'w') as job_file:
        job_file.write("#!/bin/bash \n")
        job_file.write("#SBATCH --job-name=meerwatch_{0} \n".format(psrname))
        job_file.write("#SBATCH --output={0}/meerwatch_out_{1} \n".format(str(slurm_path),psrname))
        job_file.write("#SBATCH --ntasks=1 \n")
        job_file.write("#SBATCH --mem=150g \n")
        job_file.write("#SBATCH --time=2:00:00 \n")
        job_file.write("#SBATCH --reservation=oz005_obs \n")
        job_file.write("#SBATCH --account=oz005 \n")
        job_file.write("#SBATCH --x11 \n")
        job_file.write("#SBATCH --mail-type=BEGIN,END,FAIL --mail-user=adityapartha3112@gmail.com \n")
        job_file.write('cd {0} \n'.format(soft_path))
        job_file.write("python meerwatch_gen.py /fred/oz005/meerpipe/configuration_files/meertime.config -p {0} -v".format(psrname))

    job_file.close()
    com_sbatch = "sbatch {0}".format(os.path.join(slurm_path,str(job_name)))
    args_sbatch = shlex.split(com_sbatch)
    proc_sbatch = subprocess.Popen(args_sbatch)
        
    print "MeerWatch job submitted for PSR:{0}".format(psrname)

#--------------------------- Main -------------------------


if mw == False:
    config_params = parse_config(str(args.configfile))

    #Checking validity of the input and output paths
    if not os.path.exists(config_params["input_path"]):
        print ("Input path not valid. Quitting.")
        sys.exit()
    if not os.path.exists(config_params["output_path"]):
        print ("Output path not valid. Quitting.")
        sys.exit()


    logger = setup_logging(config_params["output_path"], args.verbose, False)

    if args.jitter:
        jitter_file = "/fred/oz005/users/aparthas/Meertime_Jitter/psr_path.list"
        pulsar_list = np.loadtxt(jitter_file,comments="#",dtype=str)
    else:
        pulsar_list = sorted(glob.glob(os.path.join(config_params["input_path"],"J*")))


    kronos_path_1 = "/fred/oz005/kronos/1"
    kronos_path_2 = "/fred/oz005/kronos/2"
    kronos_path_3 = "/fred/oz005/kronos/3"
    kronos_path_4 = "/fred/oz005/kronos/4"
    for pulsar in pulsar_list:
        psrpath,psrname = os.path.split(pulsar)
        obs_list = sorted(glob.glob(os.path.join(pulsar,"2*")))
        for obs in obs_list:
            obspath,obsname = os.path.split(obs)
            raw_obs = obs #Observations in /timing
            proc_obs = os.path.join(config_params["output_path"],"{2}/{0}/{1}".format(psrname,obsname,pid)) #Observations in /timing_processed
            kronos_obs_1 = os.path.join(kronos_path_1,"{0}/{1}".format(obsname,psrname)) #Observations in /kronos/<utc>/<psrname>
            kronos_obs_2 = os.path.join(kronos_path_2,"{0}/{1}".format(obsname,psrname)) #Observations in /kronos/<utc>/<psrname>
            kronos_obs_3 = os.path.join(kronos_path_3,"{0}/{1}".format(obsname,psrname)) #Observations in /kronos/<utc>/<psrname>
            kronos_obs_4 = os.path.join(kronos_path_4,"{0}/{1}".format(obsname,psrname)) #Observations in /kronos/<utc>/<psrname>
           
            #For every /timing observation, check if the utc already exists in /timing_processed and if not, confirm that it's finished transferring
            #checking /kronos and if it exists in /kronos, then run meerpipe on that pulsar and utc. 
            if not os.path.exists(proc_obs):
                if os.path.exists(kronos_obs_1):
                    if not pid == "all":
                        if os.path.exists(os.path.join(kronos_obs_1,"obs.header")):
                            obshead_params = get_obsheadinfo(os.path.join(kronos_obs_1,"obs.header"))
                        raw_pid = get_pid(obshead_params["PID"])
                        if raw_pid == pid:
                            if pid == "TPA":
                                psrname = os.path.split(str(proc_obs))[0].split('/')[-1]
                                if psrname == "J0437-4715":
                                    pass
                                else:
                                    if args.viewlist:
                                        print "kronos_1", proc_obs
                                    if args.run:
                                        run_meerpipe(proc_obs)
                            else:
                                if args.viewlist:
                                    print "kronos_1", proc_obs
                                if args.run:
                                    run_meerpipe(proc_obs)

                    else:
                        if args.viewlist: 
                            print proc_obs
                        if args.run:
                            run_meerpipe(proc_obs)

                if os.path.exists(kronos_obs_2):
                    if not pid == "all":
                        if os.path.exists(os.path.join(kronos_obs_2,"obs.header")): 
                            obshead_params = get_obsheadinfo(os.path.join(kronos_obs_2,"obs.header"))
                        raw_pid = get_pid(obshead_params["PID"])
                        if raw_pid == pid:
                            if pid == "TPA":
                                psrname = os.path.split(str(proc_obs))[0].split('/')[-1]
                                if psrname == "J0437-4715":
                                    pass
                                else:
                                    if args.viewlist:
                                        print "kronos_2", proc_obs
                                    if args.run:
                                        run_meerpipe(proc_obs)
                            else:
                                if args.viewlist:
                                    print "kronos_2", proc_obs
                                if args.run:
                                    run_meerpipe(proc_obs)

                    else:
                        if args.viewlist: 
                            print proc_obs
                        if args.run:
                            run_meerpipe(proc_obs)

                if os.path.exists(kronos_obs_3):
                    if not pid == "all":
                        if os.path.exists(os.path.join(kronos_obs_3,"obs.header")): 
                            obshead_params = get_obsheadinfo(os.path.join(kronos_obs_3,"obs.header"))
                        raw_pid = get_pid(obshead_params["PID"])
                        if raw_pid == pid:
                            if pid == "TPA":
                                psrname = os.path.split(str(proc_obs))[0].split('/')[-1]
                                if psrname == "J0437-4715":
                                    pass
                                else:
                                    if args.viewlist:
                                        print "kronos_3", proc_obs
                                    if args.run:
                                        run_meerpipe(proc_obs)

                            else:
                                if args.viewlist:
                                    print "kronos_3", proc_obs
                                if args.run:
                                    run_meerpipe(proc_obs)


                    else:
                        if args.viewlist: 
                            print proc_obs
                        if args.run:
                            run_meerpipe(proc_obs)

                if os.path.exists(kronos_obs_4):
                    if not pid == "all":
                        if os.path.exists(os.path.join(kronos_obs_4,"obs.header")):
                            obshead_params = get_obsheadinfo(os.path.join(kronos_obs_4,"obs.header"))
                        raw_pid = get_pid(obshead_params["PID"])
                        if raw_pid == pid:
                            if pid == "TPA":
                                psrname = os.path.split(str(proc_obs))[0].split('/')[-1]
                                if psrname == "J0437-4715":
                                    pass
                                else:
                                    if args.viewlist:
                                        print "kronos_4", proc_obs
                                    if args.run:
                                        run_meerpipe(proc_obs)
                                        
                            else:
                                if args.viewlist:
                                    print "kronos_4", proc_obs
                                if args.run:
                                    run_meerpipe(proc_obs)


                    else:
                        if args.viewlist: 
                            print proc_obs
                        if args.run:
                            run_meerpipe(proc_obs)
               
if mw == True:
    config_params = parse_config(str(args.configfile))

    #Checking validity of the input and output paths
    if not os.path.exists(config_params["input_path"]):
        print ("Input path not valid. Quitting.")
        sys.exit()
    if not os.path.exists(config_params["output_path"]):
        print ("Output path not valid. Quitting.")
        sys.exit()

    pulsar_list = sorted(glob.glob(os.path.join(config_params["output_path"],"J*")))
    mw_path = "/fred/oz005/timing_processed/meerwatch_data"
    mw_slurm = "/fred/oz005/timing_processed/meerwatch_data/mw_slurm"

    for pulsar in pulsar_list:
        psrpath,psrname = os.path.split(pulsar)
        obs_list = sorted(glob.glob(os.path.join(pulsar,"2*")))
        for obs in obs_list:
            obspath,obsname = os.path.split(obs)
            proc_path = os.path.join(obs,"*/*/{0}.launch".format(psrname))
            if glob.glob(proc_path):
                mw_utc = os.path.join(mw_path,"{0}/{1}".format(psrname,obsname))
                if not os.path.exists(mw_utc):
                    if args.viewlist:
                        print "MeerWatch running for {0}:{1}".format(psrname,obsname)
                    if args.run:
                        run_meerwatch(mw_slurm,psrname)

        



