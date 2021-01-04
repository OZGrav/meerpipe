#!/usr/bin/env python

import os
import sys
import glob
import shlex
import subprocess


input_path = "/fred/oz005/timing_processed" #Path containing the pulsar directories
cg_jobs = "/fred/oz005/users/aparthas/coastguard_characterize/cg_jobs" #Path where the bash scripts for CG jobs will be saved
template_path = "/fred/oz005/timing_processed/meertime_templates" #Path to obtain the templates from (psrname.std)
out_path = "/fred/oz005/users/aparthas/coastguard_characterize" #Path to output the saved text files after every coastguard run

pulsar_dirs = sorted(glob.glob(os.path.join(input_path,"J*")))

thresholds = [5,7,9,11] #CG subint and channel thresholds

for pulsar in pulsar_dirs:
    psrpath,psrname = os.path.split(pulsar)
    utcs = sorted(glob.glob(os.path.join(pulsar,"2*")))
    for utc in utcs:
        utc_path,utc_name = os.path.split(utc)
        beams = glob.glob(os.path.join(utc,"*"))
        for beam in beams:
            freqs = glob.glob(os.path.join(beam,"*"))
            for freq in freqs:
                print ("Checking {0}".format(freq))
                total_file = glob.glob(os.path.join(freq,"*.add"))   
                if len(total_file) > 0:
                    total_file = total_file[0]
                    template = os.path.join(template_path,"{0}.std".format(psrname))
                    if os.path.exists(template):
                        if not os.path.exists(os.path.join(out_path,"{0}/{1}".format(psrname,utc_name))):
                            os.makedirs(os.path.join(out_path,"{0}/{1}".format(psrname,utc_name)))
                        if os.path.exists(os.path.join(out_path,"{0}/{1}".format(psrname,utc_name))):
                            output = os.path.join(out_path,"{0}/{1}".format(psrname,utc_name))
                            if not os.path.exists(os.path.join(output,"slurm_output")):
                                os.makedirs(os.path.join(output,"slurm_output"))
                            
                            slurm_output = os.path.join(output,"slurm_output")
                            print ("Processing {0}".format(total_file))
                            for sub_thresh in thresholds:
                                for chan_thresh in thresholds:
                                    with open (os.path.join(cg_jobs,"{3}_{0}_{1}_{2}.bash".format(
                                        utc_name,sub_thresh,chan_thresh,psrname)),'w') as job_file:

                                        job_file.write("#!/bin/bash \n")
                                        job_file.write("#SBATCH --job-name=cg_{0}_{1}_{2}_{3} \n".format(
                                            psrname,utc_name,sub_thresh,chan_thresh))
                                        job_file.write("#SBATCH --output={4}/cg_{0}_{1}_{2}_{3} \n".format(
                                            psrname,utc_name,sub_thresh,chan_thresh,slurm_output))
                                        job_file.write("#SBATCH --ntasks=1 \n")
                                        job_file.write("#SBATCH --mem=64g \n")
                                        job_file.write("#SBATCH --time=2:00:00 \n")
                                        job_file.write("cd /fred/oz005/meerpipe \n")
                                        job_file.write("python characterize_coastguard.py -ar {0} -temp {1} -st {2} -ct {3} -out {4} \n".format(
                                            total_file,template,sub_thresh,chan_thresh,output))

                                    job_file.close()

                                    print ("Deploying the job")
                                    com_sbatch = 'sbatch {0}'.format(
                                            os.path.join(cg_jobs,"{0}_{1}_{2}_{3}.bash".format(
                                                psrname,utc_name,sub_thresh,chan_thresh)))
                                    args_sbatch = shlex.split(com_sbatch)
                                    proc_sbatch = subprocess.Popen(args_sbatch)
                                    print ("{0}_{1}_{2}_{3}.bash for {1} deployed".format(psrname,utc_name,sub_thresh,chan_thresh,psrname))

                        else:
                            print ("Output path does not exist")
                    else:
                        print ("Template does not exist")
                else:
                    print ("Total file does not exist")

    print "##################################"
