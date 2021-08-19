#!/usr/bin/env python

import os
import sys
import numpy as np
import glob
import argparse
import logging
import subprocess
import shlex
import pandas as pd

parser = argparse.ArgumentParser(description="Run MeerPipe automatically on new data - use in conjunction with query_obs.py")
parser.add_argument("-cfile", dest="configfile", help="Path to the configuration file")
parser.add_argument("-list_pid",dest="list_pid",help="List of PSR and UTCs and PIDs")
parser.add_argument("-list", dest="list",help="List of PSR and UTCs")
parser.add_argument("-runas", dest="runas", help="Process observation as PID")

args = parser.parse_args()

def run_meerpipe(psrname,utcname,default_pid,config_path):
    """
    Routine to execute meerpipe as a Slurm job on a particular UTC
    """

    if args.runas:
        pid = str(args.runas)
    else:
        pid = default_pid

    print ("Running meerpipe for {0},{1} as PID:{2}".format(psrname,utcname,pid))

    meerpipe = "python /fred/oz005/meerpipe/run_pipe.py -cfile {2} -dirname {0} -utc {1} -verbose -pid {3} -slurm".format(psrname,utcname,config_path,pid)

    proc_meerpipe = shlex.split(meerpipe)
    p_meerpipe = subprocess.Popen(proc_meerpipe)
    p_meerpipe.wait()
    print ("MeerPipe job submitted for PSR:{0}, UTC:{1} and PID:{2}".format(psrname,utcname,pid))



if args.list_pid:
    #psrlist = np.genfromtxt(str(args.list_pid),delimiter=" ",dtype=str,comments="#")
    psrlist = pd.read_csv(str(args.list_pid), delimiter=" ", header=None, dtype=str, comment="#")
    psrlist = psrlist.values

elif args.list:
    #psrlist = np.genfromtxt(str(args.list),delimiter=" ",dtype=str,comments="#")
    psrlist = pd.read_csv(str(args.list), delimiter=" ", header=None, dtype=str, comment="#")
    psrlist = psrlist.values

for item in psrlist:
    #psrname, utcname, pid, configpath

    if args.list_pid:
        run_meerpipe(item[0],item[1],item[2],str(args.configfile))

    elif args.list:
        run_meerpipe(item[0],item[1],str(args.runas),str(args.configfile))
