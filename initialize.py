#!/usr/bin/env python
"""
MeerPipe: Processing pipeline for pulsar timing data.

__author__ = "Aditya Parthasarathy"
__copyright__ = "Copyright 2019, MeerTime"
__credits__ = ["Renee Spiewak", "Daniel Reardon"]
__license__ = "Public Domain"
__version__ = "0.2"
__maintainer__ = "Aditya Parthasarathy"
__email__ = "adityapartha3112@gmail.com"
__status__ = "Development"

"""

"""
Contains routines that help initialize the pipeline. 
"""

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

def get_obsinfo(obsinfo_path):
    """
    Parse the obs_info.dat file and return important parameters
    """
    params={}
    with open(obsinfo_path) as file:
        lines=file.readlines()
        for line in lines:
            (key, val) = line.split(";")
            params[str(key)] = str(val).rstrip()

    file.close()
    return params

def get_pid_dir(pid):
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


def parse_config(path_cfile):
    """
    INPUT: Path to the configuration file
    """
    
    config_params = {}
    with open (str(path_cfile)) as cfile:
        for line in cfile.readlines():
            sline = line.split("=")
            attr = (sline[0].rstrip())
            if attr == 'input_path':
                config_params["input_path"] = sline[1].rstrip().lstrip(' ')
            if attr == 'output_path':
                config_params["output_path"] = sline[1].rstrip().lstrip(' ')
            if attr == 'flags':
                config_params["flags"] = sline[1].rstrip().lstrip(' ').split(',')
            if attr == 'repo_urls':
                config_params["repos"] = sline[1].rstrip().lstrip(' ').split(',')
            if attr == 'rfi_alg':
                config_params["rfi_alg"] = sline[1].rstrip().lstrip(' ')
            if attr == "type":
                config_params["type"] = sline[1].rstrip().lstrip(' ')
            if attr == "user":
                config_params["user"] = sline[1].rstrip().lstrip(' ')
            if attr == "email":
                config_params["email"] = sline[1].rstrip().lstrip(' ')
            if attr == "calibrators_path":
                config_params["calibrators_path"] = sline[1].rstrip().lstrip(' ')
            if attr == "rm_cat":
                config_params["rmcat"] = sline[1].rstrip().lstrip(' ')
            if attr == "dm_cat":
                config_params["dmcat"] = sline[1].rstrip().lstrip(' ')
            if attr == "pipe":
                config_params["pipe"] = sline[1].rstrip().lstrip(' ')
            if attr == "decimation_products":
                config_params["decimation_products"] = sline[1].rstrip().lstrip(' ')
            if attr == "overwrite":
                config_params["overwrite"] = sline[1].rstrip().lstrip(' ')
            if attr == "ref_freq_list":
                config_params["ref_freq_list"] = sline[1].rstrip().lstrip()
            if attr == "meertime_ephemerides":
                config_params["meertime_ephemerides"] = sline[1].rstrip().lstrip()
            if attr == "meertime_templates":
                config_params["meertime_templates"] = sline[1].rstrip().lstrip()
            if attr == "toa_display_list":
                config_params["toa_display_list"] = sline[1].rstrip().lstrip()
            if attr == "global_toa_path":
                config_params["global_toa_path"] = sline[1].rstrip().lstrip()
            if attr == "redundant_products":
                config_params["red_prod"] = sline[1].rstrip().lstrip(' ').split(',')

    cfile.close()
    
    return config_params


def setup_logging(path,verbose,file_log):
    """
    Setup log handler - this logs in the terminal (if not run with --slurm).
    For slurm based runs - the logging is done by the job queue system

    """
    log_toggle=False
     
    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    if file_log == True:
        logfile = "meerpipe.log"
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
        logfile = "meerpipe.log"
        logger = logging.getLogger(logfile)
        logger.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        # add the handlers to the logger
        logger.addHandler(ch)
        logger.info("Verbose mode enabled")
        log_toggle=True
        
    """
    #Create console handler with a lower log level (INFO)
    logfile = "meerpipe.log"
    logger = logging.getLogger(logfile)
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(ch)
    logger.info("Verbose mode enabled")
    """

    if log_toggle:
        return logger
    else:
        return none


def get_outputinfo(cparams,logger):
    """
    Routine to gather information about the directory structure from the input data
    The input path is assumed to have directories that are pulsarname/UTCs/beamnumber/freq/*ar 
    Returns a list of output paths that needs to exist for the data products to be stored
    """
    input_path = cparams["input_path"]
    output_path = cparams["output_path"]
    logger.info("Gathering directory structure information")
    results_path=[]
    all_archives=[]
    psrnames=[]
    observations = []
    proposal_ids = []
    required_ram_list = []
    obs_time_list = []
    required_time_list = []

    if cparams["batch"] == "batch":
        pulsar_dirs = sorted(glob.glob(os.path.join(input_path,cparams["dirname"])))
    elif cparams["batch"] == "none":
        pulsar_dirs = sorted(glob.glob(cparams["dirname"]))

    if cparams["type"] == "caspsr":
        #For CASPSR data
        pid = str(cparams["pid"])
        for pulsar in pulsar_dirs:
            psr_path,psr_name = os.path.split(pulsar)
            psr_name_split = psr_name.split("_")
            if not psr_name_split[-1] == "R":
                obs_dirs = sorted(glob.glob(os.path.join(pulsar,"*")))
                #logger.info("Number of observations: {0}".format((len(obs_dirs))))
                for num,observation in enumerate(obs_dirs):
                    obs_path,obs_name = os.path.split(observation)
                    if not num > 50:
                        freq_dirs = sorted(glob.glob(os.path.join(observation,"*")))
                        for files in freq_dirs:
                            freq_path,freq_name = os.path.split(files)
                            archives = sorted(glob.glob(os.path.join(files,"*.ar"))) #TODO: change extension to be generic
                            results_path.append(str(output_path+"/"+pid+"/"+psr_name+"/"+obs_name+"/"+freq_name+"/"))
                            psrnames.append(psr_name)
                            all_archives.append(archives)
                    else:
                        logger.info("Number of observations exceeded 50")
                        break
            else:
                print ("Skipping CAL observations : {0}".format(psr_name))

    elif cparams["type"] == "ppta_zap":
        #For PPTA zapping 
        for pulsar in pulsar_dirs:
            psr_path,psr_name = os.path.split(pulsar)
            psrnames.append(psr_name)
            archives = sorted(glob.glob(os.path.join(pulsar,"*.dzF"))) #TODO: change extension to be generic
            results_path.append(str(output_path+"/"+psr_name+"/"))
            all_archives.append(archives)

    elif cparams["type"] == "meertime":
        #For MeerTime data on OzStar
        #Example: 2018-10-25-05:31:38/J0835-4510/1284/<*.ar>
        #New directory structure: <psrname>/<UTC>/<beam>/<freq>/<*.ar>
        """
        for observation in pulsar_dirs:
            obs_path,obs_name = os.path.split(observation)
            pulsars = sorted(glob.glob(os.path.join(observation,"J*")))
            logger.info("{0}".format(pulsars))
            for pulsar in pulsars:
                psr_path,psr_name = os.path.split(pulsar)
                psr_name_split = psr_name.split("_")
                if not psr_name_split[-1] == "R":
                    freq_dirs = sorted(glob.glob(os.path.join(pulsar,"*")))
                    for files in freq_dirs:
                        freq_path,freq_name = os.path.split(files)
                        archives = sorted(glob.glob(os.path.join(files,"*.ar")))
                        results_path.append(str(output_path+"/"+obs_name+"/"+psr_name+"/"+freq_name+"/"))
                        psrnames.append(psr_name)
                        all_archives.append(archives)
        """

        #NEW directory structure
        for pulsar in pulsar_dirs:


            psr_path,psr_name = os.path.split(pulsar)
            psr_name_split = psr_name.split("_")
            
            if psr_name_split[-1] == "R" or psr_name_split[-1] == "N" or psr_name_split[-1] == "O" or psr_name_split[-1] == "S":
                cparams["fluxcal"] = True
                logger.info("This is a fluxcal observation")
            else:
                cparams["fluxcal"] = False

            observation_dirs = sorted(glob.glob(os.path.join(pulsar,"2*")))
            if not cparams["utc"] == "none":
                logger.info("Processing custom UTC")
                observation_dirs = glob.glob(cparams["utc"])

            for observation in observation_dirs:
                obs_path,obs_name = os.path.split(observation)
                beam_dirs = sorted(glob.glob(os.path.join(observation,"*")))
                for beam in beam_dirs:
                    beam_path,beam_name = os.path.split(beam)
                    freq_dirs = sorted(glob.glob(os.path.join(beam,"*")))
                    logger.info("{0}".format(freq_dirs))
                    for files in freq_dirs:
                        freq_path,freq_name = os.path.split(files)
                        archives = sorted(glob.glob(os.path.join(files,"*.ar")))
                        info_params = get_obsinfo(glob.glob(os.path.join(files,"obs_info.dat"))[0])
                        if not "pid" in cparams:
                            pid_dir = get_pid_dir(info_params["proposal_id"])
                            proposal_ids.append(str(pid_dir))
                            results_path.append(str(output_path+"/"+pid_dir+"/"+psr_name+"/"+obs_name+"/"+beam_name+"/"+freq_name+"/"))
                        elif "pid" in cparams:
                            pid_dir = str(cparams["pid"])
                            proposal_ids.append(pid_dir)
                            results_path.append(str(output_path+"/"+pid_dir+"/"+psr_name+"/"+obs_name+"/"+beam_name+"/"+freq_name+"/"))
                        psrnames.append(psr_name)
                        all_archives.append(archives)

                        #Computing RAM requirements for this observation
                        """
                        if float(info_params["target_duration"]) <= 900.0: #Less than 15 mins
                            #reqram = "64g"
                            reqram = "32g"
                        elif float(info_params["target_duration"]) > 900.0 and float(info_params["target_duration"]) <= 3600.0: #15 mins to 1 hour
                            #reqram = "128g"
                            reqram = "64g"
                        elif float(info_params["target_duration"]) > 3600.0 and float(info_params["target_duration"]) <= 10800.0: #1 to 3 hours
                            #reqram = "256g"
                            reqram = "128g"
                        elif float(info_params["target_duration"]) > 10800.0 and float(info_params["target_duration"]) < 18000.0: #3 hours to 5 hours
                            #reqram = "512g"
                            reqram = "256g"
                        elif float(info_params["target_duration"]) > 18000.0: #More than 5 hours
                            #reqram = "768g"
                            reqram = "512g"
                        """
                        # now calculating RAM requirements based on file size, per an empirically derived relation

                        # new - 23/08/2022 - get channel count to branch settings for high channel count files
                        if (len(archives) > 0):
                            comm = "vap -c nchan {0}".format(archives[0])
                            args = shlex.split(comm)
                            proc = subprocess.Popen(args,stdout=subprocess.PIPE)
                            proc.wait()
                            info = proc.stdout.read().decode("utf-8").split("\n")
                            nchan = int(info[1].split()[1])
                        else:
                            nchan = 1024

                        # may yet require future tweaking
                        ram_slope = 10.6
                        ram_intercept = 0.4 # GB
                        ram_min = 0.6 # GB
                        ram_max = 750 # GB - MAXIMUM RAM AVAILABLE ON OZSTAR

                        if (nchan > 1024):
                            # adjusted up for high channel count files
                            ram_factor = 1.40
                        else:
                            # normal allocation
                            ram_factor = 1.20
                        #ram_factor_max = 15 # GB

                        file_size = 0
                        for subint in archives:
                            file_size += os.stat(subint).st_size # KB

                        # calculate RAM request in GB
                        reqram = ram_factor * ((file_size/(1024**3)) * ram_slope + ram_intercept)

                        #inter_ram = ((file_size/(1024**3)) * ram_slope + ram_intercept)
                        #if (inter_ram * ram_factor - inter_ram > ram_factor_max):
                        #    reqram = inter_ram + ram_factor_max
                        #else:
                        #    reqram = inter_ram * ram_factor

                        if reqram < ram_min:
                            reqram = ram_min
                        elif reqram > ram_max:
                            reqram = ram_max
                        
                        # report result in MB
                        reqram_str = "{0}m".format(int(np.ceil(reqram*1024)))

                        obs_time_list.append(info_params["target_duration"])
                        #required_ram_list.append(reqram)
                        required_ram_list.append(reqram_str)

                        # Computing time requirements for this observation to be processed
                        # This is WIP - may need additional tweaking

                        # based now on empirical study of the processing time required by jobs
                        time_factor = 2.3
                        if (nchan > 1024):
                            # (extra factor for high channel count jobs)
                            time_factor = time_factor * 1.55
                        effective_time = int(np.ceil((file_size/(1024**2)) * time_factor)) # seconds

                        if ( effective_time <= 14400 ): # 4 hours
                            # minimum time
                            reqtime = 14400
                        else:
                            # dynamic time
                            reqtime = effective_time

                        # maximum cap of 86399 seconds removed

                        # new - image only processing requires less time
                        # this will need to be adjusted
                        # but as a first guess...
                        if cparams["image_flag"]:
                            if (cparams["image_type"] == "TOAs"):
                                reqtime = reqtime / 4.0

                        required_time_list.append(int(reqtime))

         
    return results_path,all_archives,psrnames,proposal_ids,required_ram_list,obs_time_list,required_time_list


def create_structure(output_dir,cparams,psrname,logger):
    """
    Routine to create an output directory structure as decided by get_directoryinfo.
    Creates a "cleaned", "calibrated" and a "timing" directory in each output path.

    Now includes a "scintillation" directory. 
    """
    output_path = cparams["output_path"]
    flags = cparams["flags"]
    #if 'overwrite' in cparams.keys(): - 2TO3
    if 'overwrite' in list(cparams.keys()):
        overwrite_flag = str(cparams["overwrite"])
    else:
        overwrite_flag = "False"

    logger.info("Creating the directory structure for {0}".format(psrname))
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    cleaned_dir = os.path.join(output_dir,"cleaned")
    calibrated_dir = os.path.join(output_dir,"calibrated")
    timing_dir = os.path.join(output_dir,"timing")
    decimated_dir = os.path.join(output_dir,"decimated")
    scintillation_dir = os.path.join(output_dir,"scintillation")
    images_dir = os.path.join(output_dir,"images")

    if cparams["type"] == "caspsr":
        #Creating the Project ID directory
        project_dir = os.path.join(output_path,str(cparams["pid"]))
        #NOTE: Pulsar directory is the main directory containing the global par and tim files and the processed obs directories.
        pulsar_dir = os.path.join(project_dir,psrname)
    elif cparams["type"] == "meertime":
        pulsar_dir = output_dir
        project_dir = output_dir
    elif cparams["type"] == "ppta_zap":
        pulsar_dir = output_dir
        project_dir = output_dir
        
    #Head pulsar directory
    if not os.path.exists(pulsar_dir):
        logger.info("Pulsar directory created")
        os.makedirs(pulsar_dir)
    else:
        logger.info("Pulsar directory exists")
        if overwrite_flag == "True":
            if not (cparams["image_flag"]):
                rmtree(pulsar_dir)
                logger.info("Pulsar head directory overwritten")
                os.makedirs(pulsar_dir)
            else:
                logger.info("Pulsar head directory NOT overwritten due to 'images' flag")

    if not os.path.exists(cleaned_dir):
        logger.info("Cleaned directory created")
        os.makedirs(cleaned_dir)
    else:
        logger.info("Cleaned directory exists")

    if not cparams["fluxcal"]:
        if not os.path.exists(calibrated_dir):
            logger.info("Calibrated directory created")
            os.makedirs(calibrated_dir)
        else:
            logger.info("Calibrated directory exists")

        if not os.path.exists(timing_dir):
            logger.info("Timing directory created")
            os.makedirs(timing_dir)
        else:
            logger.info("Timing directory exists")

        if not os.path.exists(decimated_dir):
            logger.info("Decimated directory created")
            os.makedirs(decimated_dir)
        else:
            logger.info("Decimated directory exists")

        if not os.path.exists(scintillation_dir):
            logger.info("Scintillation directory created")
            os.makedirs(scintillation_dir)
        else:
            logger.info("Scintillation directory exists")

        # PSRDB
        # if (cparams["db_flag"]):
        if not os.path.exists(images_dir):
            logger.info("Images directory created")
            os.makedirs(images_dir)
        else:
            logger.info("Images directory exists")

        #if not os.path.exists(project_dir):
        #    logger.info("Project directory created")
        #    os.makedirs(project_dir)
        #else:
        #    logger.info("Project directory exists")

    
    # this part shouldn't need to run if we're just rebuilding the images
    if (not cparams["fluxcal"]) and (not cparams["image_flag"]):
    
        #Pull/Update repositories
        #TODO: for now just creating directories. Have to manage_repos eventually!
        logger.info("Checking if the ephemerides and templates directory exists")
        if not cparams["meertime_ephemerides"]:
            if not os.path.exists(os.path.join(output_path,"meertime_ephemerides")):
               # logger.info("Ephemerides directory created")
                os.makedirs(os.path.join(output_path,"meertime_ephemerides"))
            else:
                logger.info("meertime_ephemeredis exists")
            ephem_dir = os.path.join(output_path,"meertime_ephemerides")
        else:
            logger.info("custom meertime_ephemerides being used. {0}".format(cparams["meertime_ephemerides"]))
            ephem_dir = str(cparams["meertime_ephemerides"])


        if not cparams["meertime_templates"]:
            if not os.path.exists(os.path.join(output_path,"meertime_templates")):
                logger.info("Templates directory created")
                os.makedirs(os.path.join(output_path,"meertime_templates"))
            else:
                logger.info("meertime_templates exists")
            template_dir = os.path.join(output_path,"meertime_templates")
        else:
            logger.info("custom meertime_templates being used. {0}".format(cparams["meertime_templates"]))
            template_dir = str(cparams["meertime_templates"])


        #Check for the pulsar epehemeris and templates and copy them to the pulsar directory
        #Copying pulsar ephemeris
        if os.path.exists(os.path.join(ephem_dir,psrname+".par")):
            logger.info("Ephemeris for {0} found".format(psrname))
            if os.path.exists(os.path.join(ephem_dir,psrname+"_p2.par")):
                copyfile(os.path.join(ephem_dir,psrname+"_p2.par"),os.path.join(pulsar_dir,psrname+"_p2.par"))
            elif os.path.exists(os.path.join(ephem_dir,psrname+"_p3.par")):
                copyfile(os.path.join(ephem_dir,psrname+"_p3.par"),os.path.join(pulsar_dir,psrname+"_p3.par"))
            else:
                copyfile(os.path.join(ephem_dir,psrname+".par"),os.path.join(pulsar_dir,psrname+".par"))

        else:
            logger.info("Ephemeris for {0} not found. Generating new one.".format(psrname))
            psrcat = "psrcat -all -e {0}".format(psrname)
            proc = shlex.split(psrcat)
            try:
                f = open("{0}/{1}.par".format(ephem_dir,psrname),"w")
                subprocess.call(proc,stdout=f)
                f.close()
            except:
                logger.error("Could not open / create ephemeris file.")

            # check for success and implement one further contingency
            if os.path.exists(os.path.join(ephem_dir,psrname+".par")):
                logger.info("An ephemeris was generated from the psrcat database")
                copyfile(os.path.join(ephem_dir,psrname+".par"),os.path.join(pulsar_dir,psrname+".par"))
            else:
                # skip storing the ephemeris in a master directory and store locally
                logger.info("Unable to create ephemeris in {0}".format(ephem_dir))
                logger.info("Storing ephemeris locally in {0} only.".format(pulsar_dir))
                f = open("{0}/{1}.par".format(pulsar_dir,psrname),"w")
                subprocess.call(proc,stdout=f)
                f.close()

        #Copying pulsar template
        # NEW - Safeguard in case notemplate.list does not exist
        if (os.path.exists(os.path.join(template_dir,"notemplate.list"))):
            notemplate_list = np.loadtxt(os.path.join(template_dir,"notemplate.list"),dtype=str)
        else:
            notemplate_list = []

        if os.path.exists(os.path.join(template_dir,psrname+".std")):
            logger.info("Template for {0} found".format(psrname))
            if os.path.exists(os.path.join(template_dir,psrname+"_p2.std")):
                copyfile(os.path.join(template_dir,psrname+"_p2.std"),os.path.join(pulsar_dir,psrname+"_p2.std"))
            elif os.path.exists(os.path.join(template_dir,psrname+"_p3.std")):
                copyfile(os.path.join(template_dir,psrname+"_p3.std"),os.path.join(pulsar_dir,psrname+"_p3.std"))
            else:
                copyfile(os.path.join(template_dir,psrname+".std"),os.path.join(pulsar_dir,psrname+".std"))
        
        elif psrname in notemplate_list:
            logger.info("{0} in notemplate list. Using a Gaussian template instead.".format(psrname))
            copyfile(os.path.join(template_dir,"Gaussian.std"),os.path.join(pulsar_dir,"Gaussian.std"))

        else:
            #TODO:Generate new template in this case
            logger.info("Template for {0} not found. Will generate one after zapping.".format(psrname))

            #sys.exit()

