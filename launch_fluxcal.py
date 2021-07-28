#!/usr/bin/env python

import os
import sys
import shlex
import subprocess
import argparse
import os.path
import numpy as np
import logging
import glob
from shutil import copyfile, rmtree
import matplotlib.pyplot as plt
import pandas as pd
import psrchive as ps
from scipy import stats
import time

import getopt
from astropy.io import fits

    
#=============================================================================


parser = argparse.ArgumentParser(description="Launch flux calibration for MeerTime")
parser.add_argument("-path", dest="path", help="Path to the reprocessing directory",required=True)
parser.add_argument("-psr", dest="psr", help="Pulsar name",required=True)
parser.add_argument("-obs", dest="obsdir", help="Directory name or directory lists (like 2020*)")
parser.add_argument("-pid", dest="pid", help="Project ID", required=True)
args = parser.parse_args()


pulsar_dir = os.path.join(args.path,args.psr)

psr_path,psr_name = os.path.split(pulsar_dir)

if not args.obsdir:
    print "Looping through all observations of {0}".format(psr_name)
    observation_dirs = sorted(glob.glob(os.path.join(pulsar_dir,"*")))
else:
    print "Using {0} observations of {1}".format(args.obsdir,psr_name)
    observation_dirs = sorted(glob.glob(os.path.join(pulsar_dir,"{0}*".format(args.obsdir))))
    
for observation in observation_dirs:
    obs_path,obs_name = os.path.split(observation)    
    print "Processing {0}:{1}".format(psr_name,obs_name)
    beam_dirs = sorted(glob.glob(os.path.join(observation,"*")))
    for beam in beam_dirs:
        beam_path,beam_name = os.path.split(beam)
        freq_dirs = sorted(glob.glob(os.path.join(beam,"1284"))) #Forcing only L-band observations! 
        for files in freq_dirs:
            freq_path,freq_name = os.path.split(files)

            if not len(glob.glob(os.path.join(files,"decimated/*fluxcal")))> 0:

                if len(glob.glob(os.path.join(files,"*add")))> 0:
                    obsheader_path = glob.glob(os.path.join(files,"*obs.header"))[0]
                    add_file = glob.glob(os.path.join(files,"*add"))[0]
                    if str(args.pid) == "TPA":
                        TP_file = glob.glob(os.path.join(files,"decimated/*zapTp.ar"))[0]
                    if str(args.pid) == "PTA":
                        TP_file = glob.glob(os.path.join(files,"decimated/*t32p*ar"))[0]
                    if str(args.pid) == "RelBin":
                        TP_file = glob.glob(os.path.join(files,"decimated/*zap.Tp.ar"))[0]

                    decimated_products = glob.glob(os.path.join(files,"decimated/*.ar"))
                    cleaned_archive = glob.glob(os.path.join(files,"cleaned/*.ar"))[0]
                    decimated_products.append(cleaned_archive)
                    print "Using {0} for fluxcal".format(os.path.split(add_file)[-1])
                else:
                    print "WARNING: No added file present. Observation may not have been processed well."
                    continue
                    
                
                output_dir = os.path.join(files,"fluxcal_jobs")
                if not os.path.exists(output_dir):
                    print "Created fluxcal_job directory for {0}:{1}".format(psr_name,obs_name)
                    os.makedirs(output_dir)
                    
                    
                np.save(os.path.join(output_dir,"decimatedlist"),decimated_products)
                decimated_list = os.path.join(output_dir,"decimatedlist.npy")
                 
                job_name = "{0}_{1}.bash".format(psr_name,obs_name)
                mysoft_path = "/fred/oz005/meerpipe"
                with open(os.path.join(output_dir,str(job_name)),'w') as job_file:
                    job_file.write("#!/bin/bash \n")
                    job_file.write("#SBATCH --job-name={0}_{1} \n".format(psr_name,obs_name))
                    job_file.write("#SBATCH --output={0}/{1}_{2}_fluxcal.out\n".format(output_dir,psr_name,obs_name))
                    job_file.write("#SBATCH --ntasks=1 \n")
                    job_file.write("#SBATCH --mem=64g \n")
                    job_file.write("#SBATCH --time=00:20:00 \n")
                    job_file.write("#SBATCH --mail-type=FAIL --mail-user=adityapartha3112@gmail.com \n")
                    job_file.write('cd {0} \n'.format(mysoft_path))
                    job_file.write("python fluxcal.py -psrname {0} -obsname {1} -obsheader {2} -TPfile {3} -rawfile {4} -dec_path {5}".format(psr_name,obs_name,obsheader_path,TP_file,add_file,decimated_list))
                    
                job_file.close()
                
                print "Job script created for {0}:{1}".format(psr_name,obs_name)
                
                print ("Deploying {0}".format(job_name))
                com_sbatch = 'sbatch {0}'.format(os.path.join(output_dir,str(job_name)))
                args_sbatch = shlex.split(com_sbatch)
                proc_sbatch = subprocess.Popen(args_sbatch)
                time.sleep(1)  
                print ("{0} deployed.".format(job_name))
                
                
            else:
                print "Flux calibrated files already exist for {0}:{1}".format(psr_name,obs_name)
                
                                   
                                   
                
                    
                    
                    
                
                
                
                
