"""
Code containing utilities for parsing/processing pulsar archive files

__author__ = "Aditya Parthasarathy"
__copyright__ = "Copyright (C) 2018 Aditya Parthasarathy"
__license__ = "Public Domain"
__version__ = "0.1"
"""

#Basic imports
import os
import sys
import argparse
import time
import numpy as np
import glob
import shlex
import subprocess
from shutil import copyfile
import datetime
import pandas as pd

#matplotlib comm3 fix
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

#Importing scintools (@dreardon)
#sys.path.append('/fred/oz002/dreardon/scintools/scintools')
#from dynspec import Dynspec

#psrchive imports
import psrchive as ps

#Coastguard imports
from coast_guard import cleaners

#Slack imports
import json
import requests


#---------------------------------- General functions --------------------------------------
def get_ephemeris(psrname,output_path,cparams,logger):
    """
    Routine to return the path of the ephemeris file for a given pulsar name
    """
    if cparams["meertime_ephemerides"]:
        par_path = os.path.join(cparams["meertime_ephemerides"],psrname+".par")
    else:
        ephem = os.path.join(output_path,"meertime_ephemerides")
        par_path = os.path.join(ephem,psrname+".par")

    return par_path

def get_pptatemplate(backend,psrname,cfreq,nbin,logger):
    "Function written specifically to obtain proper PPTA templates"
    toa_path = "/fred/oz002/meertime/meerpipe/ppta_zap_results/ppta_zap_template/ppta_templates"
    print(backend,psrname,cfreq,nbin)
    if backend == "p":
        if cfreq > 500 and cfreq < 1000:
            templates = glob.glob(os.path.join(toa_path,"{0}*40cm*CASPSR*".format(psrname)))
            if len(templates) > 0:
                pass
            else:
                templates = glob.glob(os.path.join(toa_path,"{0}*50cm*CASPSR*".format(psrname)))
        if cfreq > 1000 and cfreq < 1500:
            templates = glob.glob(os.path.join(toa_path,"{0}*20cm*CASPSR*".format(psrname)))
        if cfreq > 1500 and cfreq < 3400:
            templates = glob.glob(os.path.join(toa_path,"{0}*10cm*CASPSR*".format(psrname)))

    if backend == "t":
        if cfreq > 500 and cfreq < 1000:
            templates = glob.glob(os.path.join(toa_path,"{0}*40cm*PDFB*".format(psrname)))
            templates = glob.glob(os.path.join(toa_path,"{0}*50cm*PDFB*".format(psrname)))
        if cfreq > 1000 and cfreq < 1500:
            templates = glob.glob(os.path.join(toa_path,"{0}*20cm*PDFB*".format(psrname)))
        if cfreq > 1500 and cfreq < 3400:
            templates = glob.glob(os.path.join(toa_path,"{0}*10cm*PDFB*".format(psrname)))

    if not len(templates) > 0:
        print ("Nothing found")
        print (templates)
        return None
    else:
        for template in templates:
            loaded_template = ps.Archive_load(template)
            if int(loaded_template.get_nbin()) == nbin:
                print (template)
                return template
                break

def get_meertimetemplate(psrname,output_path,cparams,logger):
    if cparams["meertime_templates"]:
        psr_template = os.path.join(cparams["meertime_templates"],str(psrname)+".std")
    else:
        template_dir = os.path.join(output_path,"meertime_templates")
        psr_template = os.path.join(template_dir,str(psrname)+".std")

    
    if os.path.exists(psr_template):
        #Template sanity checks
        print psr_template
        template_ar = ps.Archive_load(str(psr_template))
        if int(template_ar.get_nbin()) == 1024:
            return psr_template
        else:
            return None
    else:
        return None

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


#--------------------------------------------------- Processing utilities -------------------------------

def add_archives(archive_list,output_dir,cparams,psrname,logger):
    """
    Routine to add the raw archive files/observation directory before RFI excision
    """

    added_archives=[]
    psrname = str(psrname)
    output_path = cparams["output_path"]
    add_name = os.path.split(archive_list[0])[1].split('.')[0]
    added_fname = "{0}_{1}.add".format(psrname,add_name)
    pid = cparams["pid"]

    tmp_path = os.path.split(archive_list[0])[0]
    if os.path.exists(os.path.join(str(tmp_path),"obs.header")):
        copyfile(os.path.join(str(tmp_path),"obs.header"),os.path.join(str(output_dir),"obs.header"))
        logger.info("obs.header file copied")
    
    if not os.path.exists(os.path.join(str(output_dir),str(added_fname))):

        logger.info("Adding raw archive files")
        np.savetxt(os.path.join(str(output_dir),"{0}_raw.archives".format(psrname)),archive_list,fmt='%s')
        raw_archives = os.path.join(str(output_dir),"{0}_raw.archives".format(psrname))

        if not cparams["fluxcal"]:

            #Set ephemeris while psradd
            ephem = get_ephemeris(psrname,output_path,cparams,logger)
            add_raw = 'psradd -M {0} -o {1}/{2} -E {3}'.format(raw_archives,str(output_dir),str(added_fname),ephem)
            proc_add = shlex.split(add_raw)
            p_add = subprocess.Popen(proc_add)
            p_add.wait()

        elif cparams["fluxcal"]:
            #Adding flux cal observations. 
            add_raw = 'psradd -M {0} -o {1}/{2}'.format(raw_archives,str(output_dir),str(added_fname))
            proc_add = shlex.split(add_raw)
            p_add = subprocess.Popen(proc_add)
            p_add.wait()
 
        if pid == "TPA" or pid == "PTA":
            #Check for new MK DMs and RMs and apply if present. If not, the DM is applied from the ephemeris and the RM from psrcat.
            #Applying DM
            dm_path = cparams["dmcat"]
            dm_cat = np.genfromtxt(dm_path,delimiter=" ",dtype=str)
            if psrname in dm_cat[:,0]:
                index = np.where(dm_cat[:,0] == psrname)[0][0]
                dm = dm_cat[index,1]
                logger.info("Found custom MK DM. Updating the header. {0},{1}".format(psrname,dm))
                set_dm = 'pam {0}/{1} -d {2} -m'.format(str(output_dir),str(added_fname), dm)
                proc_dm = shlex.split(set_dm)
                p_dm = subprocess.Popen(proc_dm)
                p_dm.wait()
            
            else:    
                logger.info("Updating DM in header using the value in the ephemeris")
                set_dm = 'pam {0}/{1} --update_dm -m -E {2}'.format(str(output_dir),str(added_fname), ephem)
                proc_dm = shlex.split(set_dm)
                p_dm = subprocess.Popen(proc_dm)
                p_dm.wait()
        
        else:
            if not cparams["fluxcal"]:
                logger.info("Updating DM in header using the value in the ephemeris")
                set_dm = 'pam {0}/{1} --update_dm -m -E {2}'.format(str(output_dir),str(added_fname), ephem)
                proc_dm = shlex.split(set_dm)
                p_dm = subprocess.Popen(proc_dm)
                p_dm.wait()

        
        if pid == "TPA" or pid == "PTA" or pid == "J0437_LT":
            #Applying RMs
            rm_path = cparams["rmcat"]
            rm_cat = np.genfromtxt(rm_path,delimiter=" ",dtype=str)
            if psrname in rm_cat[:,0]:
                index = np.where(rm_cat[:,0] == psrname)[0][0]
                rm = rm_cat[index,1]
                logger.info("Found custom MK RM. Updating the header. {0},{1}".format(psrname,rm))
                set_rm = 'pam {0}/{1} --RM {2} -m'.format(str(output_dir),str(added_fname),rm)
                proc_rm = shlex.split(set_rm)
                p_rm = subprocess.Popen(proc_rm)
                p_rm.wait()
            
            else:
                logger.info("Obtaining and applying RM to the archive file")
                cat_rm = 'psrcat -c RM {0} -X -all'.format(psrname)
                args_rm = shlex.split(cat_rm)
                proc_rm = subprocess.Popen(args_rm, stdout=subprocess.PIPE)
                rm_out = proc_rm.stdout.readline().split()[0]
                if rm_out == "*" or rm_out == " ":
                    logger.info("RM not found in psrcat and in file. Not applying")
                    pass
                else:
                    rm = float(rm_out)
                    logger.info("RM for {0}: {1}".format(psrname,rm))
                    set_rm = 'pam --RM {0} -m {1}/{2}'.format(rm,str(output_dir),str(added_fname))
                    args_setrm = shlex.split(set_rm)
                    proc_setrm = subprocess.Popen(args_setrm)
                    proc_setrm.wait()

        else:
            logger.info("Not applying RM")
            pass

        os.remove(os.path.join(str(output_dir),"{0}_raw.archives".format(psrname)))
    else:
        logger.info("Input files already added")

    added_archives.append(os.path.join(str(output_dir),str(added_fname)))

    return added_archives


def get_calibrator(archive_utc,calib_utcs,header_params,logger):
    #Routine to compare one archive utc with a list of calibrator utcs to identify the appropriate calibrator file to use

    #Getting a list of VH files recorded before the data (starting from the closest)
    archive_utc = datetime.datetime.strptime(archive_utc, '%Y-%m-%d-%H:%M:%S')
    time_diff=[]
    cals_tocheck = []
    for calib_utc in calib_utcs:
        utc = os.path.split(calib_utc)[1].split('.jones')[0]
        utc = datetime.datetime.strptime(utc, '%Y-%m-%d-%H:%M:%S')
        if (archive_utc-utc).total_seconds() > 0:
            time_diff.append(calib_utc)
  
    if not header_params==None:
        #Obtaining the reference antenna and comparing it with the antenna list for the data. 
        reference_strings = ['reference', 'antenna', 'name:']
        for item in reversed(time_diff):
            with open (item,'r') as f:
                lines = f.readlines()
                for line in lines:
                    sline = line.split()
                    if all(elem in sline for elem in reference_strings):
                        while "#" in sline: sline.remove("#")
                        reference_antenna = sline[-1]
                        print reference_antenna
                        if reference_antenna in header_params["ANTENNAE"]:
                            jones_file = item
                            bloop=True
                            break
                        else:
                            bloop=False
            f.close()
            if bloop:
                break
            else:
                continue
        return str(jones_file)

    else:
        logger.info("NO OBS.HEADER FILE - returning the closest calibrator file.")
        return str(time_diff[-1])


def calibrate_data(added_archives,output_dir,cparams,logger):
    
   #Routine to calibrate the data - either using jones matrices or just use pac -XP

    pid = cparams["pid"]
    pipe = cparams["pipe"]

    if pipe == "new":
        calibrated_archives=[]
        flags = cparams["flags"]
        output_path = cparams["output_path"]
        calibrators_path = cparams["calibrators_path"]
        logger.info("Using jones matrices for calibration")
        for add_archive in added_archives:
            add_ar = ps.Archive_load(add_archive)
            archive_path,archive_name = os.path.split(add_ar.get_filename())
            archive_name = archive_name.split('.')[0]

            calibrated_path = os.path.join(str(output_dir),"calibrated")

            #Identify the jones matrix file to use for polarization calibration. 
            calib_utcs = sorted(glob.glob(os.path.join(calibrators_path,"*jones")))
            archive_utc = os.path.split(add_archive)[1].split("_")[-1].split('.add')[0]
            if os.path.exists(os.path.join(str(output_dir),"obs.header")):
                header_params = get_obsheadinfo(os.path.join(str(output_dir),"obs.header"))
            else:
                header_params = None
            calibrator_archive = get_calibrator(archive_utc,calib_utcs,header_params,logger)

            logger.info("Found jones matrix file:{0}".format(calibrator_archive))

            if not os.path.exists(os.path.join(calibrated_path,"{0}.calib".format(archive_name))):
                pac_com = 'pac -Q {0} {1} -O {2} -e calib '.format(calibrator_archive,add_archive,calibrated_path)
                proc_pac = shlex.split(pac_com)
                p_pac = subprocess.Popen(proc_pac)
                p_pac.wait()

                calibrated_file = glob.glob(os.path.join(calibrated_path,"*calibP"))[0]
                name = os.path.split(calibrated_file)[1].split(".calibP")[0]+".calib"
                new_file = os.path.join(calibrated_path,name)
                os.rename(calibrated_file,new_file)
                logger.info("Calibrated file renamed")
            else:
                logger.info("Calibrated data already exists")

            calibrated_archives.append(os.path.join(calibrated_path,archive_name+".calib"))

    elif pipe == "old":
        calibrated_archives=[]
        flags = cparams["flags"]
        output_path = cparams["output_path"]

        logger.info("Using pac -XP for calibration")

        for add_archive in added_archives:
            add_ar = ps.Archive_load(add_archive)
            archive_path,archive_name = os.path.split(add_ar.get_filename())
            archive_name = archive_name.split('.')[0]

            calibrated_path = os.path.join(str(output_dir),"calibrated")

            if not os.path.exists(os.path.join(calibrated_path,"{0}.calib".format(archive_name))):
                pac_com = 'pac -XP {0} -O {1} -e calib '.format(add_archive,calibrated_path)
                proc_pac = shlex.split(pac_com)
                p_pac = subprocess.Popen(proc_pac)
                p_pac.wait()
            else:
                logger.info("Calibrated data already exists")

            calibrated_archives.append(os.path.join(calibrated_path,archive_name+".calib"))

    return calibrated_archives


def mitigate_rfi(calibrated_archives,output_dir,cparams,psrname,logger):
    """
    Routine to mitigate RFI using coastguard - input is a single integrated file containing multiple sub-integrations
    """
    flags = cparams["flags"]
    output_path = cparams["output_path"]
    if not "nozap" in flags:
        logger.info("RFI excision enabled")
        cleaned_archives=[]
        cleaned_path = os.path.join(str(output_dir),"cleaned")
        pulsar_dir = os.path.join(output_path,str(psrname))

        if not len(calibrated_archives) > 0:
            logger.info("No archive files")

        for archive in calibrated_archives:
            archive = ps.Archive_load(str(archive))
            archive_path,archive_name = os.path.split(archive.get_filename())
            archive_name = archive_name.split('.')[0]
            #backend = archive_name[0]
            archive_name = archive_name+"_zap"      

            if not os.path.exists(os.path.join(cleaned_path,archive_name+".ar")):
                logger.info(os.environ["COAST_GUARD"])
                """
                if cparams["rfi_alg"] == "CG":
                    logger.info("Using CoastGuard for RFI mitigation")
                    #Checking if env variable is set properly
                    if not os.environ.get('COAST_GUARD') == "/fred/oz002/dreardon/coast_guard":
                        logger.info("CoastGuard variable not set. Setting it now..")
                        os.environ["COAST_GUARD"] = "/fred/oz002/dreardon/coast_guard"
                        os.environ["COASTGUARD_CFG"] = "/fred/oz002/dreardon/coast_guard/configurations"
                        logger.info("CG environment set to: {0}, {1}".format(os.environ["COAST_GUARD"],os.environ["COASTGUARD_CFG"]))
                    else:
                        logger.info("CG environment already set to: {0}, {1}".format(os.environ["COAST_GUARD"],os.environ["COASTGUARD_CFG"]))
               
                elif cparams["rfi_alg"] == "CHIVE":
                
                    #TODO: Implement PSRCHIVE based zapping (perhaps using the python interface)
                    logger.info("Using PSRCHIVE for RFI mitigation")

                
                elif cparams["rfi_alg"] == "MG":
                    #Uses MeerGuard
                    #Checking if env variable is set properly
                    if not os.environ.get('COAST_GUARD') == "/fred/oz002/dreardon/MeerGuard":
                        logger.info("MeerGuard variable not set")
                        #os.environ["COAST_GUARD"] = "/fred/oz002/dreardon/MeerGuard"
                        #os.environ["COASTGUARD_CFG"] = "/fred/oz002/dreardon/MeerGuard/configurations"
                        #logger.info("MG evironment set to: {0}, {1}".format(os.environ["COAST_GUARD"],os.environ["COASTGUARD_CFG"]))
                        #logger.info("Using MeerGuard for RFI mitigation")

                """


                logger.info("Generating cleaned archive {0}.ar".format(archive_name))
                cloned_archive = archive.clone()


                #Get templates for coastguarding
                if cparams["type"] == "ppta_zap": 
                    template = get_pptatemplate(backend,psrname,float(cloned_archive.get_centre_frequency()),int(cloned_archive.get_nbin()),logger)
                elif cparams["type"] == "meertime":
                    template = get_meertimetemplate(psrname,output_path,cparams,logger)

                if not (int(cloned_archive.get_nsubint()) == 1 and int(cloned_archive.get_nchan()) == 1):

                    #RcvrStandard cleaner
                    logger.info("Applying rcvrstd cleaner")
                    rcvrstd_cleaner = cleaners.load_cleaner('rcvrstd')
                    
                    rcvrstd_parameters = 'badfreqs=None,badsubints=None,trimbw=0,trimfrac=0,trimnum=0,response=None'
                    rcvrstd_cleaner.parse_config_string(rcvrstd_parameters)
                    rcvrstd_cleaner.run(cloned_archive)

                    #Surgical cleaner
                    logger.info("Applying the surgical cleaner")
                    surgical_cleaner = cleaners.load_cleaner('surgical')
                    
                    #logger.info("Using template: {0}".format(template))

                    chan_thresh = 5
                    subint_thresh = 5

                    if not template is None:

                        logger.info("Applying channel threshold of {0} and subint threshold of {0}".format(chan_thresh,subint_thresh))

                        surgical_parameters = 'chan_numpieces=1,subint_numpieces=1,chanthresh={1},subintthresh={2},template={0}'.format(template,chan_thresh,subint_thresh)
                    else:
                        surgical_parameters = 'chan_numpieces=1,subint_numpieces=1,chanthresh={0},subintthresh={1}'.format(chan_thresh,subint_thresh)
                    
                    surgical_cleaner.parse_config_string(surgical_parameters)
                    surgical_cleaner.run(cloned_archive)

                    #Bandwagon cleaner
                    logger.info("Applying the bandwagon cleaner")
                    bandwagon_cleaner = cleaners.load_cleaner('bandwagon')
                    bandwagon_parameters = 'badchantol=0.99,badsubtol=1.0'
                    bandwagon_cleaner.parse_config_string(bandwagon_parameters)
                    bandwagon_cleaner.run(cloned_archive)
                    
                    cleaned_archives.append(os.path.join(cleaned_path,archive_name+".ar"))
                    
                    #unloading
                    logger.info("Unloading the cleaned archive {0}.ar".format(archive_name))
                    if cparams["fluxcal"]:
                        logger.info("Time averaging fluxcal observation")
                        cloned_archive.tscrunch()
                        cloned_archive.unload(os.path.join(cleaned_path,archive_name+"_T.ar"))
                    else:
                        cloned_archive.unload(os.path.join(cleaned_path,archive_name+".ar"))

                else:
                    logger.info("{0} has 1 subintegration. No CoastGuarding done!".format(str(cloned_archive.get_filename())))


            else:

                logger.info("Cleaned archive {0}.ar exists. Skipping RFI excision".format(archive_name))
                cleaned_archives.append(os.path.join(cleaned_path,archive_name+".ar"))
                
    else: #DO NOT ZAP
        logger.info("RFI excision disabled")
        cleaned_archives=[]
        for archive in archive_list:
            archive_path,archive_name = os.path.split(archive) 
            logger.info("Adding {0}.ar to the archive list".format(archive_name))
            cleaned_archives.append(archive)

        logger.info("NOTE: The archive_list points to the original uncleaned archives")
                
    return cleaned_archives


def dynamic_spectra(cleaned_archives,output_dir,cparams,psrname,logger):
    """
    Routine to produce the dynamic spectra by running psrflux.
    Produces dynamic and secondary spectra plots using scintools (@dreardon)
    """

    flags = cparams["flags"]
    output_path = cparams["output_path"]

    ds_path = os.path.join(str(output_dir),"scintillation")
    logger.info("Generating dynamic spectra using psrflux")

    template = get_meertimetemplate(psrname,output_path,cparams,logger)

    if not template == None:

        for clean_archive in cleaned_archives:
     
            clean_ar = ps.Archive_load(clean_archive)
            archive_path,archive_name = os.path.split(clean_ar.get_filename())
            archive_name = archive_name.split('.')[0]
           
            if not os.path.exists(os.path.join(ds_path,"{0}.dynspec".format(archive_name))):

                try: 
                    psrflux_com = 'psrflux -s {0} {1} -e dynspec'.format(template,clean_archive)
                    proc_psrflux = shlex.split(psrflux_com)
                    p_psrflux = subprocess.Popen(proc_psrflux)
                    p_psrflux.wait()

                    #Moving the dynamic spectra to the scintillation directory
                    dynspec = str(clean_archive)+".dynspec"
                    new_dynspec = os.path.join(ds_path,"{0}.dynspec".format(archive_name))
                    os.rename(dynspec,new_dynspec)
                    logger.info("Dynamic spectra generated and moved to Scintillation directory for {0}".format(archive_name))

                    logger.info("Creating dynamic spectra plots using scintools")
                    dynspec_file = glob.glob(os.path.join(ds_path,"*.dynspec"))[0]

                    dyn = Dynspec(dynspec_file, process=False, verbose=False)
                    dyn.plot_dyn(filename=os.path.join(ds_path,"{0}_dynspec.png".format(archive_name)))
                    logger.info("Refilling")
                    dyn.trim_edges()
                    dyn.refill(linear=False)
                    logger.info("Secondary spectra")
                    dyn.cut_dyn(tcuts=0, fcuts=7, plot=True, filename=os.path.join(ds_path,"{0}_subband.png".format(archive_name)))

                except:
                    logger.info("Scintools failed. Dyanmic spectra couldn't be created")
                
            else:
                logger.info("Dynamic spectra already exists")

    else:
        logger.info("Template does not exist. Skipping dynamic spectra generation.")


def get_extension(commands,chopped):
    #Routine to generate extension based on pam command (for RelBin decimation)

    commands = commands.split(" ")
    extension = ""
    for num,item in enumerate(commands):
        if item == "-F":
            extension=extension+"F"
        if item == "-p":
            extension=extension+"p"
        if item == "-f":
            nchan = commands[num+1]
            extension=extension+"f{0}".format(nchan)
        if item == "-t":
            tfactor = commands[num+1]
            extension=extension+"t{0}".format(tfactor)
        if item == "-T":
            extension = extension+"T"

    if chopped:
        extension = extension+".ch.ar"
    else:
        extension = extension+".ar"

    return extension


def decimate_data(cleaned_archives,output_dir,cparams,logger):
    """
    Routine to return a list of decimated data products
    """
    flags = cparams["flags"]
    output_path = cparams["output_path"]
    pid = cparams["pid"]

    processed_archives = []
    decimated_path = os.path.join(str(output_dir),"decimated")
    
    logger.info("Decimating data")
    if "saveall" in flags:
        logger.info("Saving all the decimated products")

    for clean_archive in cleaned_archives:
            
        loaded_archive = ps.Archive_load(clean_archive)
        clean_archive = loaded_archive.clone()
        
        #get archive name
        archive_path,archive_name = os.path.split(str(clean_archive.get_filename()))
        archive_name = archive_name.split('.')[0]
        logger.info("Decimating {0}.ar".format(archive_name))

        #Produces different data products
        for flag in flags:
            sflag = flag.split(" ")
            if len(sflag) > 1:
                if sflag[0] == "t":
                    nsub = sflag[1]
                if sflag[2] == "f":
                    nchan = int(sflag[3])
                print len(sflag)
                if len(sflag) > 4:
                    print sflag
                    if sflag[4] == "P":
                        pol_scrunch = True
                        logger.info("Polarization scrunching enabled")
                    else:
                        pol_scrunch = False
                        logger.info("Polarization scrunching disabled")
                else:
                    pol_scrunch = False
                    logger.info("No polarization flag specified")

                if not nsub == "all":
                    if not pol_scrunch:
                        decimated_name = "{0}.{1}ch{2}s.ar".format(archive_name,nchan,nsub)
                    else:
                        decimated_name = "{0}.{1}ch{2}sP.ar".format(archive_name,nchan,nsub)
                elif nsub == "all":
                    if not pol_scrunch:
                        decimated_name = "{0}.{1}ch.ar".format(archive_name,nchan)
                    else:
                        decimated_name = "{0}.{1}chP.ar".format(archive_name,nchan)

                if not os.path.exists(os.path.join(decimated_path,decimated_name)):

                    if not nsub == "all":

                        if not pol_scrunch:

                            nsub = int(nsub)
                            pam_comm = "pam --setnchn {0} --setnsub {1} -FT -e {0}ch{1}s.ar {2} -u {3}".format(nchan,nsub,clean_archive,decimated_path)
                            logger.info("Producing sub-banded and t-scruched archives with full stokes")
                            proc_pam = shlex.split(pam_comm)
                            subprocess.call(proc_pam)
                            processed_archives.append(os.path.join(decimated_path,"{0}.{1}ch{2}s.ar".format(archive_name,nchan,nsub)))
                        else:
                            nsub = int(nsub)
                            pam_comm = "pam --setnchn {0} --setnsub {1} -FT -p -e {0}ch{1}sP.ar {2} -u {3}".format(nchan,nsub,clean_archive,decimated_path)
                            logger.info("Producing sub-banded and t-scruched archives with only total intensity")
                            proc_pam = shlex.split(pam_comm)
                            subprocess.call(proc_pam)
                            processed_archives.append(os.path.join(decimated_path,"{0}.{1}ch{2}sP.ar".format(archive_name,nchan,nsub)))



                    elif nsub == "all":

                        if not pol_scrunch:

                            pam_comm = "pam --setnchn {0} -F -e {0}ch.ar {1} -u {2}".format(nchan,clean_archive,decimated_path)
                            logger.info("Producing sub-banded, full time and stokes resolution archives")
                            proc_pam = shlex.split(pam_comm)
                            subprocess.call(proc_pam)
                            processed_archives.append(os.path.join(decimated_path,"{0}.{1}ch.ar".format(archive_name,nchan)))
                        else:
                            pam_comm = "pam --setnchn {0} -F -p -e {0}chP.ar {1} -u {2}".format(nchan,clean_archive,decimated_path)
                            logger.info("Producing sub-banded, full time and total intensity resolution archives")
                            proc_pam = shlex.split(pam_comm)
                            subprocess.call(proc_pam)
                            processed_archives.append(os.path.join(decimated_path,"{0}.{1}ch.ar".format(archive_name,nchan)))

               
                else:
                    logger.info("Sub-banded and t-scrunched (P or S pol) data products exist")
                    processed_archives.append(os.path.join(decimated_path,decimated_name))


        if pid == "TPA":

            #Running a frequency extraction to reject ~48 channels on either side if BW is 856 MHz to produce consistent data prodcuts throughout. Only done for 8ch1s.ar
            cleaned_path = os.path.join(str(output_dir),"cleaned")
            cleaned_file = glob.glob(os.path.join(cleaned_path,archive_name+".ar"))[0]
            if os.path.exists(cleaned_file):
                cleaned_ar = ps.Archive_load(cleaned_file)
                freqs = cleaned_ar.get_frequencies().tolist()
                if len(freqs) > 928:
                    logger.info("Extracting frequency channels from cleaned file since BW is 856 MHz (1024 channels)")
                    reference_928ch_freqlist  = np.load(cparams["ref_freq_list"]).tolist()
                    oar = cleaned_ar.clone()
                    dd = oar.get_dedispersed()
                    if dd:
                        oar.dededisperse()

                    recheck=True
                    while recheck:
                        recheck=False
                        freqs = oar.get_frequencies()
                        for i,f in enumerate(freqs):
                            if f in reference_928ch_freqlist:
                                pass
                            else:
                                oar.remove_chan(i,i)
                                recheck=True
                                break

                    logger.info("Done extracting")
                    if dd:
                        oar.dedisperse()

                    oar.unload(os.path.join(decimated_path,archive_name+".tmp"))
                    logger.info("Unloaded extracted file")

                    if os.path.exists(os.path.join(decimated_path,archive_name+".tmp")):
                        tmp_ar = os.path.join(decimated_path,archive_name+".tmp")
                        pam_comm = "pam --setnchn 8 --setnsub 1 -FT -e 8ch1s.ch.ar {0} -u {1}".format(tmp_ar,decimated_path)
                        logger.info("Producing sub-banded and t-scruched archives with full stokes - 8ch1s.ch.ar")
                        proc_pam = shlex.split(pam_comm)
                        subprocess.call(proc_pam)
                        os.remove(tmp_ar)


            #Decimating on a fresh copy of the archive
            logger.info("Loading a fresh version of the archive file")
            clean_archive = loaded_archive.clone()
            #get archive name
            archive_path,archive_name = os.path.split(str(clean_archive.get_filename()))
            archive_name = archive_name.split('.')[0]


            #Frequency+Polarization scrunching - producing new file
            archive_name = archive_name+"Fp"
            if not os.path.exists(os.path.join(decimated_path,archive_name+".ar")):
                logger.info("Frequency scrunching.")
                clean_archive.fscrunch()

                logger.info("Polarization scrunching")
                clean_archive.pscrunch()
                clean_archive.unload(os.path.join(decimated_path,archive_name+".ar"))
            else:
                logger.info("{0}.ar exists. Skipping polarization+frequency scrunching".format(archive_name))


            #Decimating on a fresh copy of the archive
            logger.info("Loading a fresh version of the archive file")
            clean_archive = loaded_archive.clone()
            #get archive name
            archive_path,archive_name = os.path.split(str(clean_archive.get_filename()))
            archive_name = archive_name.split('.')[0]

            #Time+Polarization scrunching - producing new file
            archive_name = archive_name+"Tp"
            if not os.path.exists(os.path.join(decimated_path,archive_name+".ar")):
                logger.info("Time scrunching.")
                clean_archive.tscrunch()
                
                logger.info("Polarization scrunching")
                clean_archive.pscrunch()
                clean_archive.unload(os.path.join(decimated_path,archive_name+".ar"))
            else:
                logger.info("{0}.ar exists. Skipping polarization+time scrunching".format(archive_name))


            #Decimating on a fresh copy of the archive
            logger.info("Loading a fresh version of the archive file")
            clean_archive = loaded_archive.clone()
            #get archive name
            archive_path,archive_name = os.path.split(str(clean_archive.get_filename()))
            archive_name = archive_name.split('.')[0]

            #Time+Polarization scrunching - producing new file
            archive_name = archive_name+"TF"
            if not os.path.exists(os.path.join(decimated_path,archive_name+".ar")):
                logger.info("Time scrunching.")
                clean_archive.tscrunch()
                
                logger.info("Frequency scrunching")
                clean_archive.fscrunch()
                clean_archive.unload(os.path.join(decimated_path,archive_name+".ar"))
            else:
                logger.info("{0}.ar exists. Skipping polarization+time scrunching".format(archive_name))
        

        elif pid == "NGC6440":

            #Decimating on a fresh copy of the archive
            logger.info("Loading a fresh version of the archive file")
            clean_archive = loaded_archive.clone()
            #get archive name
            archive_path,archive_name = os.path.split(str(clean_archive.get_filename()))
            archive_name = archive_name.split('.')[0]


            #Frequency+Polarization scrunching - producing new file
            archive_name = archive_name+"FTp"
            if not os.path.exists(os.path.join(decimated_path,archive_name+".ar")):
                logger.info("Frequency scrunching.")
                clean_archive.fscrunch()
                logger.info("Time scrunching")
                clean_archive.tscrunch()
                logger.info("Polarization scrunching")
                clean_archive.pscrunch()
                clean_archive.unload(os.path.join(decimated_path,archive_name+".ar"))
            else:
                logger.info("{0}.ar exists. Skipping polarization+frequency+time scrunching".format(archive_name))


        elif pid == "RelBin":
            #Using relbin_decimation.list (in additional_info) for decimating cleaned archives. 

            #Decimating on a fresh copy of the archive
            logger.info("Loading a fresh version of the archive file")
            clean_archive = loaded_archive.clone()
            #get archive name
            archive_path,archive_name = os.path.split(str(clean_archive.get_filename()))
            archive_name = archive_name.split('.')[0]

            #decimation_info = np.genfromtxt(cparams["decimation_products"],delimiter=", ",dtype=str)
            decimation_info = pd.read_csv(cparams["decimation_products"],sep=", ", dtype=str, header=None)
            decimation_info = decimation_info.replace(np.nan, 'None', regex=True)
            decimation_info = decimation_info.values.tolist()
            psrname = archive_name.split("_")[0]
            for num in range(0,len(decimation_info)):
                while 'None' in decimation_info[num]: decimation_info[num].remove('None')
                if decimation_info[num][0] == psrname:
                    for item in decimation_info[num]:
                        if not item == psrname:
                            extension = get_extension(item,False)
                            if not os.path.exists(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension))):
                                pam_command = "pam {0} -e {1} -u {2} {3}".format(item,extension,decimated_path,clean_archive)
                                logger.info("Producing {0} archives".format(extension))
                                proc_pam = shlex.split(pam_command)
                                subprocess.call(proc_pam)
                                processed_archives.append(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension)))
                            else:
                                logger.info("{0}.{1} exists".format(archive_name,extension))
                                processed_archives.append(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension)))


        elif pid == "PTA":
            #Using pta_decimation.list (in additional_info) for decimating cleaned archives. 

            #Decimating on a fresh copy of the archive
            logger.info("Loading a fresh version of the archive file")
            clean_archive = loaded_archive.clone()
            #get archive name
            archive_path,archive_name = os.path.split(str(clean_archive.get_filename()))
            archive_name = archive_name.split('.')[0]

            cleaned_path = os.path.join(str(output_dir),"cleaned")
            cleaned_file = glob.glob(os.path.join(cleaned_path,archive_name+".ar"))[0]
            if os.path.exists(cleaned_file):
                cleaned_ar = ps.Archive_load(cleaned_file)
                freqs = cleaned_ar.get_frequencies().tolist()
                if len(freqs) > 928:
                    print "Producing only full BW data products"
                    decimation_info = ["all", "-F -T -p", "-t 32 -f 128 -p"]
                else:
                    print "Producing all data products"
                    #decimation_info = np.genfromtxt(cparams["decimation_products"],delimiter=", ",dtype=str)
                    decimation_info = pd.read_csv(cparams["decimation_products"],sep=", ", dtype=str)
                    decimation_info = decimation_info.replace(np.nan, 'None', regex=True)
                    decimation_info = decimation_info.values.tolist()
                    decimation_info = decimation_info[0]
            
            print decimation_info
            psrname = archive_name.split("_")[0]
            for item in decimation_info:
                #for item in decimation_info[num]:
                if not item == "all":
                    extension = get_extension(item,False)
                    if not os.path.exists(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension))):
                        pam_command = "pam {0} -e {1} -u {2} {3}".format(item,extension,decimated_path,clean_archive)
                        logger.info("Producing {0} archives".format(extension))
                        proc_pam = shlex.split(pam_command)
                        subprocess.call(proc_pam)
                        processed_archives.append(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension)))
                    else:
                        logger.info("{0}.{1} exists".format(archive_name,extension))
                        processed_archives.append(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension)))


            #Running a frequency extraction to reject ~48 channels on either side if BW is 856 MHz to produce consistent data prodcuts throughout. Only done for 8ch1s.ar
            cleaned_path = os.path.join(str(output_dir),"cleaned")
            cleaned_file = glob.glob(os.path.join(cleaned_path,archive_name+".ar"))[0]
            if os.path.exists(cleaned_file):
                cleaned_ar = ps.Archive_load(cleaned_file)
                freqs = cleaned_ar.get_frequencies().tolist()
                if len(freqs) > 928:
                    logger.info("Extracting frequency channels from cleaned file since BW is 856 MHz (1024 channels)")
                    reference_928ch_freqlist  = np.load(cparams["ref_freq_list"]).tolist()
                    oar = cleaned_ar.clone()
                    dd = oar.get_dedispersed()
                    if dd:
                        oar.dededisperse()

                    recheck=True
                    while recheck:
                        recheck=False
                        freqs = oar.get_frequencies()
                        for i,f in enumerate(freqs):
                            if f in reference_928ch_freqlist:
                                pass
                            else:
                                oar.remove_chan(i,i)
                                recheck=True
                                break

                    logger.info("Done extracting")
                    if dd:
                        oar.dedisperse()

                    oar.unload(os.path.join(decimated_path,archive_name+".tmp"))
                    logger.info("Unloaded extracted file")

                    if os.path.exists(os.path.join(decimated_path,archive_name+".tmp")):
                        tmp_ar = os.path.join(decimated_path,archive_name+".tmp")
                        chopped = ["-T -f 4", "-t 32 -p", "-t 32 -f 116 -p", "-F -T -p"]
                        for item in chopped:
                            extension_ch = get_extension(item,True)
                            if not os.path.exists(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension_ch))):
                                pam_comm = "pam {0} -e {1} {2} -u {3}".format(item, extension_ch, tmp_ar,decimated_path)
                                logger.info("Producing sub-banded and t-scruched archives with full stokes - {0}".format(extension_ch))
                                proc_pam = shlex.split(pam_comm)
                                subprocess.call(proc_pam)
                                processed_archives.append(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension_ch)))
                            else:
                                logger.info("{0}.{1} already exists".format(archive_name,extension_ch))
                                processed_archives.append(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension_ch)))
 
                        os.remove(tmp_ar)



        elif pid == "ngcsearch":
            #Using pta_decimation.list (in additional_info) for decimating cleaned archives. 

            #Decimating on a fresh copy of the archive
            logger.info("Loading a fresh version of the archive file")
            clean_archive = loaded_archive.clone()
            #get archive name
            archive_path,archive_name = os.path.split(str(clean_archive.get_filename()))
            archive_name = archive_name.split('.')[0]

            #decimation_info = np.genfromtxt(cparams["decimation_products"],delimiter=", ",dtype=str)
            decimation_info = pd.read_csv(cparams["decimation_products"],sep=", ", dtype=str)
            decimation_info = decimation_info.replace(np.nan, 'None', regex=True)
            decimation_info = decimation_info.values.tolist()
            print decimation_info
            psrname = archive_name.split("_")[0]
            for num in range(0,len(decimation_info)):
                while 'None' in decimation_info[num]: decimation_info[num].remove('None')
                for item in decimation_info[num]:
                    if not item == "all":
                        extension = get_extension(item,False)
                        if not os.path.exists(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension))):
                            pam_command = "pam {0} -e {1} -u {2} {3}".format(item,extension,decimated_path,clean_archive)
                            logger.info("Producing {0} archives".format(extension))
                            proc_pam = shlex.split(pam_command)
                            subprocess.call(proc_pam)
                            processed_archives.append(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension)))
                        else:
                            logger.info("{0}.{1} exists".format(archive_name,extension))
                            processed_archives.append(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension)))



        else:
            #For all other PIDs

            #Polarization scrunching first
            if "pscrunch" in flags:
                archive_name = archive_name+"p"
                if not os.path.exists(os.path.join(decimated_path,archive_name+".ar")):
                    logger.info("Polarization scrunching")
                    clean_archive.pscrunch()
                    if "saveall" in flags:
                        if pid == "TPA":
                            pass
                        else:
                            clean_archive.unload(os.path.join(decimated_path,archive_name+".ar"))
                else:
                    logger.info("{0}.ar exists. Skipping polarization scrunching".format(archive_name))


            if "tscrunch" in flags:
                archive_name = archive_name+"T"
                if not os.path.exists(os.path.join(decimated_path,archive_name+".ar")):
                    logger.info("Time scrunching")
                    clean_archive.tscrunch()
                    if "saveall" in flags:
                        if pid == "TPA":
                            pass
                        else:
                            cloned_archive = clean_archive.clone()
                            cloned_archive.unload(os.path.join(decimated_path,archive_name+".ar"))
                else:
                    logger.info("{0}.ar exists. Skipping time scrunching".format(archive_name))


            if "fscrunch" in flags:
                archive_name = archive_name+"F"
                if not os.path.exists(os.path.join(decimated_path,archive_name+".ar")):
                    logger.info("Full frequency scrunching")
                    clean_archive.fscrunch()
                    if "saveall" in flags:
                        if pid == "TPA":
                            pass
                        else:
                            cloned_archive = clean_archive.clone()
                            cloned_archive.unload(os.path.join(decimated_path,archive_name+".ar"))

                else:
                    logger.info("{0}.ar exists. Skipping full frequency scrunching".format(archive_name))

        
    return processed_archives


def fluxcalibrate(output_dir,cparams,psrname,logger):

    logger.info("Flux calibrating the decimated data products of {0}".format(psrname))
    pid = cparams["pid"]
    decimated_path = os.path.join(str(output_dir),"decimated")
    obsheader_path = glob.glob(os.path.join(str(output_dir),"*obs.header"))[0]
    obsname = decimated_path.split("/")[-4]
    decimated_archives = sorted(glob.glob(os.path.join(decimated_path,"J*.ar")))
    for archive in decimated_archives:
        if pid == "TPA":
            if "zapTp.ar" in archive:
                TP_file = archive
        if pid == "PTA":
            if "t32p" in archive:
                TP_file = archive
    addfile = glob.glob(os.path.join(str(output_dir),"*add"))[0]

    np.save(os.path.join(decimated_path,"decimatedlist"), decimated_archives)
    decimated_list = os.path.join(decimated_path,"decimatedlist.npy")


    fluxcal_command = "python fluxcal.py -psrname {0} -obsname {1} -obsheader {2} -TPfile {3} -rawfile {4} -dec_path {5}".format(psrname,obsname,obsheader_path,TP_file,addfile,decimated_list)

    fluxcalproc = shlex.split(fluxcal_command)
    subprocess.call(fluxcalproc)

    fluxcal_obs = glob.glob(os.path.join(decimated_path,"*.fluxcal"))
    if len(fluxcal_obs) == len(decimated_archives):
        logger.info("All decimated observations of {0}:{1} are flux calibrated".format(psrname,obsname))
    else:
        logger.warning("Flux calibration failed")




def generate_toas(processed_archives,output_dir,cparams,psrname,logger):
    # Routine to call pat

    psrname = str(psrname)
    logger.info("Generating ToAs")
    output_path = cparams["output_path"]
    orig_psrname = psrname

    #Setting required paths
    timing_path = os.path.join(str(output_dir),"timing")
    pulsar_dir = os.path.join(output_path,str(psrname))

    #Obtaining templates
    logger.info("Obtaining templates for timing")
    template = get_meertimetemplate(psrname,output_path,cparams,logger)

    if not template is None:
        for proc_archive in processed_archives:
            tim_name = os.path.split(proc_archive)[1].split('.ar')[0]+".tim"
            #Running pat
            if not os.path.exists(os.path.join(timing_path,tim_name)):
                logger.info("Creating ToAs with pat")
                logger.info(proc_archive)
                arg = 'pat -jp -f "tempo2 IPTA" -C "chan rcvr snr length subint" -s {0} -A FDM {1}'.format(template,proc_archive)
                proc = shlex.split(arg)
                f = open("{0}/{1}".format(timing_path,tim_name), "w")
                subprocess.call(proc, stdout=f)
                logger.info("{0} generated".format(tim_name))
                f.close()

                #Creating a select file
                select_file = open("{0}/{1}.select".format(timing_path,orig_psrname),"w")
                select_file.write("LOGIC -snr < 10 REJECT \n")
                select_file.close()

                #Creating a meerwatch launch file
                logger.info("{0}.launch file for MeerWatch".format(psrname))
                mw_launch = open("{0}/{1}.launch".format(str(output_dir),str(psrname)),"w")
                mw_launch.write("Launch_MeerWatch. MeerPipe successful")
                mw_launch.close()

            else:
                logger.info("{0} file exists. Skipping ToA computation.".format(tim_name))
    else:
        logger.error("Template does not exist or does not have 1024 phase bins. Skipping ToA generation.")
        """
        ftp_file = glob.glob(os.path.join(os.path.join(str(output_dir),"decimated"),"*_zappTF.ar"))[0]
        arg_temp = 'psrsmooth -W {0} -e std'.format(ftp_file)
        proc_temp = shlex.split(arg_temp)
        subprocess.call(proc_temp)
        decimated_path = os.path.join(str(output_dir),"decimated")
        os.rename(glob.glob(os.path.join(decimated_path,"*.std"))[0], os.path.join(str(output_dir),"{0}.std".format(psrname)))
        copyfile(os.path.join(str(output_dir),"{0}.std".format(psrname)), os.path.join(str(output_path),"meertime_templates/{0}.std".format(psrname)))
        template = os.path.join(str(output_dir),"{0}.std".format(psrname))
        for proc_archive in processed_archives:
            tim_name = os.path.split(proc_archive)[1].split('.ar')[0]+".tim"
            #Running pat
            if not os.path.exists(os.path.join(timing_path,tim_name)):
                logger.info("Creating ToAs with pat")
                arg = 'pat -jp -f "tempo2 IPTA" -C "chan rcvr snr length subint" -s {0} -A FDM {1}'.format(template,proc_archive)
                proc = shlex.split(arg)
                f = open("{0}/{1}".format(timing_path,tim_name), "w")
                subprocess.call(proc, stdout=f)
                logger.info("{0} generated".format(tim_name))
                f.close()

                #Creating a select file
                select_file = open("{0}/{1}.select".format(timing_path,orig_psrname),"w")
                select_file.write("LOGIC -snr < 10 REJECT \n")
                select_file.close()

                #Creating a meerwatch launch file
                logger.info("{0}.launch file for MeerWatch".format(psrname))
                mw_launch = open("{0}/{1}.launch".format(str(output_dir),str(psrname)),"w")
                mw_launch.write("Launch_MeerWatch. MeerPipe successful")
                mw_launch.close()

            else:
                logger.info("{0} file exists. Skipping ToA computation.".format(tim_name))
        """

