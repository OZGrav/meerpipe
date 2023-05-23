"""
Code containing utilities for parsing/processing pulsar archive files

__author__ = "Aditya Parthasarathy, Andrew Cameron"
__copyright__ = "Copyright (C) 2023 Aditya Parthasarathy, Andrew Cameron"
__license__ = "Public Domain"
__version__ = "0.1"
"""

#Basic imports
import os
import sys
import numpy as np
import glob
import shlex
import subprocess
from shutil import copyfile
import datetime
import pandas as pd

# image manip imports
from PIL import Image

#matplotlib comm3 fix
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

#Importing scintools (@dreardon)
from scintools.dynspec import Dynspec

#psrchive imports
import psrchive as ps

#Coastguard imports
from coast_guard import cleaners
from coast_guard import clean_utils

#Slack imports
import json
import requests

#Meerwatch imports
from meerpipe.meerwatch_tools import get_res_fromtim, plot_toas_fromarr, get_dm_fromtim

# PSRDB imports - assumes psrdb/latest module
from util import ephemeris
from tables import *
from graphql_client import GraphQLClient
from meerpipe.db_utils import (create_pipelinefile, create_ephemeris, create_template, create_toa_record, create_pipelineimage,
                      get_results, update_processing, update_folding, get_procid_by_location, get_toa_id, check_toa_nominal,
                      get_proc_embargo)
from meerpipe.initialize import setup_logging

#---------------------------------- General functions --------------------------------------
def get_ephemeris(psrname,output_path,cparams,logger):
    """
    Routine to return the path of the ephemeris file for a given pulsar name
    """
    if cparams["meertime_ephemerides"]:
        if os.path.exists(os.path.join(cparams["meertime_ephemerides"],psrname+"_p2.par")):
            par_path = os.path.join(cparams["meertime_ephemerides"],psrname+"_p2.par")
        elif os.path.exists(os.path.join(cparams["meertime_ephemerides"],psrname+"_p3.par")):
            par_path = os.path.join(cparams["meertime_ephemerides"],psrname+"_p3.par")
        else:
            par_path = os.path.join(cparams["meertime_ephemerides"],psrname+".par")
    else:
        ephem = os.path.join(output_path,"meertime_ephemerides")
        par_path = os.path.join(ephem,psrname+".par")

    return par_path

def get_ephemeris_doublecheck(psrname,output_dir,ephem,archive_list,cparams,logger):

    # backup methods of getting an ephemeris in case the primary function fails
    output_dir = str(output_dir)

    if not (os.path.isfile(ephem)):
        # fallback 1 - look in the local directory
        local_par = os.path.join(output_dir,psrname+".par")
    else:
        local_par = ephem

    # fallback 2 - if the file still doesn't exist or is invalid,
    # create an ephemeris in the local directory from the archives themselves

    # check for existence and validity
    if (os.path.isfile(local_par)):
        eph_file = open(local_par, 'r')
        line = eph_file.readline()
        eph_file.close()

        if ("WARNING" in line) and ("not in catalogue" in line):
            eph_test = False
            logger.info("Ephemeris found at {0} but is invalid".format(local_par))
        else:
            logger.info("Valid ephemeris found at {0}".format(local_par))
            eph_test = True
    else:
        logger.info("Could not find ephemeris at {0}".format(local_par))
        eph_test = False

    # go to fallback 2
    if not (eph_test):

        local_par = os.path.join(output_dir,psrname+".par")
        logger.info("Creating local ephemeris at {0} from internal archive ephemeris.".format(local_par))
        par_fh = open(local_par, 'w')
        comm = "vap -E {0}".format(archive_list[0])
        logger.info(comm)
        proc = subprocess.Popen(shlex.split(comm), stdin=subprocess.PIPE, stdout=par_fh)
        proc.wait()
        par_fh.close()

    return local_par

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

        if (os.path.exists(os.path.join(cparams["meertime_templates"],"notemplate.list"))):
            notemplate_list = np.loadtxt(os.path.join(cparams["meertime_templates"],"notemplate.list"),dtype=str)
        else:
            notemplate_list = []

        if os.path.exists(os.path.join(cparams["meertime_templates"],str(psrname)+".std")):
            if os.path.exists(os.path.join(cparams["meertime_templates"],str(psrname)+"_p2.std")):
                psr_template = os.path.join(cparams["meertime_templates"],str(psrname)+"_p2.std")
            elif os.path.exists(os.path.join(cparams["meertime_templates"],str(psrname)+"_p3.std")):
                psr_template = os.path.join(cparams["meertime_templates"],str(psrname)+"_p3.std")
            else:
                psr_template = os.path.join(cparams["meertime_templates"],str(psrname)+".std")
        elif psrname in notemplate_list:
            psr_template = os.path.join(cparams["meertime_templates"],"Gaussian.std")
        else:
            psr_template = "NA"

    else:
        template_dir = os.path.join(output_path,"meertime_templates")
        psr_template = os.path.join(template_dir,str(psrname)+".std")


    if os.path.exists(psr_template):
        #Template sanity checks
        logger.info("Template: {0}".format(psr_template))
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

def get_rcvr(params):
    """
    Determine which receiver is in use
    External conditions may be required for this function, depending on use
    """
    bw = params["BW"]
    freq = float(params["FREQ"])

    if (bw == "544.0") and (freq < 816) and (freq > 815):
        rcvr = "UHF"
    elif (freq < 1284) and (freq > 1283):
        rcvr = "LBAND"
    else:
        rcvr = None

    return rcvr


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
            # redundancy check
            ephem = get_ephemeris_doublecheck(psrname,output_dir,ephem,archive_list,cparams,logger)
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

        if pid == "TPA" or pid == "PTA" or pid == "RelBin" or pid == "GC":
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


        if pid == "TPA" or pid == "PTA" or pid == "J0437_LT" or pid == "RelBin" or pid == "GC":
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
                rm_out = str(proc_rm.stdout.readline().split()[0].decode("utf-8"))
                if rm_out == "*" or rm_out == " " or rm_out == "WARNING:":
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
    # extra step for UHF calibration
    substr = "_sub"

    for calib_utc in calib_utcs:
        utc = os.path.split(calib_utc)[1].split('.jones')[0]
        # extra step for UHF calibration
        if (substr in utc):
            utc = utc.split(substr)[0]
        utc = datetime.datetime.strptime(utc, '%Y-%m-%d-%H:%M:%S')
        if (archive_utc-utc).total_seconds() > 0:
            time_diff.append(calib_utc)

    if not header_params==None:
        #Obtaining the reference antenna and comparing it with the antenna list for the data.
        reference_strings = ['reference', 'antenna', 'name:']
        desired_strings = ['desired', 'antenna', 'name:']
        for item in reversed(time_diff):
            with open (item,'r') as f:
                lines = f.readlines()
                for line in lines:
                    sline = line.split()
                    if all(elem in sline for elem in reference_strings) or all(elem in sline for elem in desired_strings):
                        while "#" in sline: sline.remove("#")
                        reference_antenna = sline[-1]
                        print (reference_antenna)
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

    if os.path.exists(os.path.join(str(output_dir),"obs.header")):
        header_params = get_obsheadinfo(os.path.join(str(output_dir),"obs.header"))
    else:
        header_params = None

    calibrated_archives=[]
    calibrators_path = cparams["calibrators_path"]

    for add_archive in added_archives:
        add_ar = ps.Archive_load(add_archive)
        archive_path,archive_name = os.path.split(add_ar.get_filename())
        archive_name = archive_name.split('.')[0]

        archive_utc = os.path.split(add_archive)[1].split("_")[-1].split('.add')[0]
        archive_utc_datetime = datetime.datetime.strptime(archive_utc, '%Y-%m-%d-%H:%M:%S')

        # multiple reference dates now available
        LBAND_reference_calib_date = datetime.datetime.strptime("2020-04-10-00:00:00",'%Y-%m-%d-%H:%M:%S')
        UHF_reference_calib_date = datetime.datetime.strptime("2021-08-18-00:00:00",'%Y-%m-%d-%H:%M:%S')

        # abstracting repeated code
        calibrated_path = os.path.join(str(output_dir),"calibrated")

        # swapping predence order here - receiver first
        rcvr = get_rcvr(header_params)

        if rcvr == "UHF":

            # UHF branch

            if (archive_utc_datetime - UHF_reference_calib_date).total_seconds() > 0:

                # data past reference date - correct headers only
                logger.info("Data already polarisation calibrated. Just correcting headers.")

                if not os.path.exists(os.path.join(calibrated_path,"{0}.calib".format(archive_name))):
                    pac_com = 'pac -XP {0} -O {1} -e calib '.format(add_archive,calibrated_path)
                    proc_pac = shlex.split(pac_com)
                    p_pac = subprocess.Popen(proc_pac)
                    p_pac.wait()
                else:
                    logger.info("Calibrated data already exists")

                calibrated_archives.append(os.path.join(calibrated_path,archive_name+".calib"))

            elif (archive_utc_datetime - UHF_reference_calib_date).total_seconds() <=0:

                # apply Jones matrices
                logger.info("Polarisation calibration manually applied using Jones matrices")

                # check subdirectory for UHF cals - update cal path
                calibrators_path = os.path.join(calibrators_path,"UHF")

                #Identify the jones matrix file to use for polarization calibration.
                calib_utcs = sorted(glob.glob(os.path.join(calibrators_path,"*jones")))

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

        elif rcvr == "LBAND":

            # L-Band branch

            if (archive_utc_datetime - LBAND_reference_calib_date).total_seconds() > 0:

                # data past reference date - correct headers only
                logger.info("Data already polarisation calibrated. Just correcting headers.")

                if not os.path.exists(os.path.join(calibrated_path,"{0}.calib".format(archive_name))):
                    pac_com = 'pac -XP {0} -O {1} -e calib '.format(add_archive,calibrated_path)
                    proc_pac = shlex.split(pac_com)
                    p_pac = subprocess.Popen(proc_pac)
                    p_pac.wait()
                else:
                    logger.info("Calibrated data already exists")

                calibrated_archives.append(os.path.join(calibrated_path,archive_name+".calib"))

            elif (archive_utc_datetime - LBAND_reference_calib_date).total_seconds() <=0:

                # apply Jones matrices
                logger.info("Polarisation calibration manually applied using Jones matrices")

                #Identify the jones matrix file to use for polarization calibration.
                calib_utcs = sorted(glob.glob(os.path.join(calibrators_path,"*jones")))

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

        else:

            # unrecognised receiver choice
            logger.warning("Unknown receiver ({0}) - unable to calibrate data.".format(rcvr))

    #Setting polc=1 in calibrated archives
    for carchive in calibrated_archives:
        ar = ps.Archive_load(carchive)
        ar.set_poln_calibrated()
        logger.info("polc=1 for {0}".format(carchive))
        ar.unload(carchive)

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
                    orig_template = get_pptatemplate(backend,psrname,float(cloned_archive.get_centre_frequency()),int(cloned_archive.get_nbin()),logger)
                elif cparams["type"] == "meertime":
                    orig_template = get_meertimetemplate(psrname,output_path,cparams,logger)
                else:
                    orig_template = None

                # NEW - produce tempoerary template that has been checked for bin count and has been de-dedispersed
                # this template will be generated separately from the original template, making it safe to delete
                if not orig_template is None:

                    temporary_template = template_adjuster(orig_template, cloned_archive, output_dir, logger)

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

                    if not orig_template is None:

                        logger.info("Applying channel threshold of {0} and subint threshold of {0}".format(chan_thresh,subint_thresh))

                        if os.path.split(orig_template)[-1] == "Gaussian.std":
                            logger.info("Since using a Gaussian template - typically the observation S/N is very low. So RFI exicision is done without a template")
                            surgical_parameters = 'chan_numpieces=1,subint_numpieces=1,chanthresh={0},subintthresh={1}'.format(chan_thresh,subint_thresh)
                        else:
                            surgical_parameters = 'chan_numpieces=1,subint_numpieces=1,chanthresh={1},subintthresh={2},template={0}'.format(temporary_template,chan_thresh,subint_thresh)

                    else:
                        surgical_parameters = 'chan_numpieces=1,subint_numpieces=1,chanthresh={0},subintthresh={1}'.format(chan_thresh,subint_thresh)

                    surgical_cleaner.parse_config_string(surgical_parameters)
                    logger.info("Surgical cleaner parameters correctly parsed.")
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

    # if PSRDB activated, record S/N of cleaned archives
    if cparams["db_flag"]:

        logger.info("PSRDB functionality activated - recording cleaned S/N")

        # Create client
        db_client = GraphQLClient(cparams["db_url"], False)

        # Extract the maximum snr from the cleaned archives
        max_snr = 0
        for x in range(0, len(cleaned_archives)):
             comm = "psrstat -j FTp -c snr=pdmp -c snr {0}".format(cleaned_archives[x])
             args = shlex.split(comm)
             proc = subprocess.Popen(args,stdout=subprocess.PIPE)
             proc.wait()
             info = proc.stdout.read().decode("utf-8").rstrip().split()
             snr = info[1].split("=")[1]
             if (float(snr) > float(max_snr)):
                 max_snr = snr

        # Recall results field and update
        results = get_results(cparams["db_proc_id"], db_client, cparams["db_url"], cparams["db_token"])
        logger.info("Recalled results of processing ID {0}".format(cparams["db_proc_id"]))
        logger.info(results)
        results['snr'] = float(max_snr)
        update_id = update_processing(
            cparams["db_proc_id"],
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            results,
            db_client,
            cparams["db_url"],
            cparams["db_token"]
        )
        if (update_id != cparams["db_proc_id"]) or (update_id == None):
            logger.error("Failure to update 'processings' entry ID {0} - PSRDB cleanup may be required.".format(cparams["db_proc_id"]))
        else:
            logger.info("Updated PSRDB entry in 'processings' table, ID = {0}".format(cparams["db_proc_id"]))


    # clean up any scrunched template, if one was built
    if not orig_template is None:

        logger.info("Cleaning up temporary template...")
        os.remove(temporary_template)

#        template_bins = int(template_ar.get_nbin())

#        if not (str(orig_template) == str(template)) and not (int(ps.Archive_load(str(template)).get_nbin()) == template_bins):
#            logger.info("Removing scrunched template {}".format(template))
            #os.remove(template)

    return cleaned_archives

# Utility function - adjustes a template to match the requirements of RFI mitigation
# This includes:
# - matching the phase bins of the provided file, if possible
# - de-dedispersing the template, if required
# returns a copy of the template which can be safely deleted as needed
def template_adjuster(template, archive, output_dir, logger):

    # setup
    template_ar = ps.Archive_load(str(template))
    template_bins = int(template_ar.get_nbin())
    archive_bins = int(ps.Archive_load(str(archive)).get_nbin())

    # NEW 10/02/2023 - check for dedispersion and channel count
    if (template_ar.get_dedispersed() and template_ar.get_nchan() == 1):

        # convert the archive to undo dedispersion (vap -c dmc == 0)
        logger.info("De-dedispersing the temporary template...")
        template_ar.set_dedispersed(False)
        logger.info("Template successfully de-dedispersed.")

    # if bin counts don't match
    if not (template_bins == archive_bins):

        logger.info("Mismatch detected between phase bin count of archive ({0}) and template ({1})".format(archive_bins, template_bins))
        logger.info("Attempting to produce scrunched template to correct for mismatch...")

        # work out the mismatch factor
        b_factor = template_bins / archive_bins
        b_remainder = template_bins % archive_bins

        if not (b_remainder == 0):
            logger.info("Non-integer factor between the bin values, cannot scrunch to match - skipping step")
        elif (b_factor < 1.0):
            logger.info("Archive has higher bin count than template - skipping step")
        else:
            # create bin-scrunched clone and write to temporary file
            logger.info("Creating scrunched template by factor {}".format(b_factor))
            template_ar.bscrunch_to_nbin(archive_bins)

    # the scrunch has now either been done or it has not
    # write out the temporary standard
    new_template = os.path.join(str(output_dir),"temporary_{}.std".format(archive_bins))
    template_ar.unload(new_template)

    return new_template


def dynamic_spectra(output_dir,cparams,psrname,logger):
    """
    Routine to produce the dynamic spectra by running psrflux.
    Produces dynamic and secondary spectra plots using scintools (@dreardon)
    """

    flags = cparams["flags"]
    output_path = cparams["output_path"]

    ds_path = os.path.join(str(output_dir),"scintillation")
    logger.info("Generating dynamic spectra using psrflux")

    orig_template = get_meertimetemplate(psrname,output_path,cparams,logger)

    cleaned_dir = os.path.join(str(output_dir),"cleaned")
    cleaned_archives = sorted(glob.glob(os.path.join(str(cleaned_dir),"*.ar")))
    cleaned_archives.append(glob.glob(os.path.join(str(output_dir),"calibrated/*.calib"))[0])

    if not orig_template == None:

        max_rfi_frac = 0.0

        for clean_archive in cleaned_archives:

            clean_ar = ps.Archive_load(clean_archive)
            archive_path,archive_name = os.path.split(clean_ar.get_filename())
            extension = archive_name.split('.')[-1]
            archive_name = archive_name.split('.')[0]

            # account for phase bin differences
            temporary_template = template_adjuster(orig_template, clean_ar, output_dir, logger)

            logger.info("Archive name:{0} and extension: {1}".format(archive_name, extension))

            if extension == "ch.ar":
                dynspec_name = archive_name+".ch.dynspec"
            if extension == "ar":
                dynspec_name = archive_name+".dynspec"
            if extension == "calib":
                dynspec_name = archive_name+".calib.dynspec"

            if not os.path.exists(os.path.join(ds_path,"{0}".format(dynspec_name))):

                if "ch" in dynspec_name:
                    psrflux_com = 'psrflux -s {0} {1} -e ch.dynspec'.format(temporary_template,clean_archive)
                if dynspec_name == archive_name+".dynspec":
                    psrflux_com = 'psrflux -s {0} {1} -e dynspec'.format(temporary_template,clean_archive)
                if "calib" in dynspec_name:
                    psrflux_com = 'psrflux -s {0} {1} -e dynspec'.format(temporary_template,clean_archive)

                proc_psrflux = shlex.split(psrflux_com)
                p_psrflux = subprocess.Popen(proc_psrflux)
                p_psrflux.wait()

                #Moving the dynamic spectra to the scintillation directory
                if "ar.ch" in dynspec_name:
                    old_name = "{0}/{1}.ar.ch.dynspec".format(os.path.join(str(output_dir),"cleaned"),archive_name)
                if "ch.ar" in dynspec_name:
                    old_name = "{0}/{1}.ch.ar.dynspec".format(os.path.join(str(output_dir),"cleaned"),archive_name)
                if dynspec_name == archive_name+".dynspec":
                    old_name = "{0}/{1}.ar.dynspec".format(os.path.join(str(output_dir),"cleaned"),archive_name)
                if "calib" in dynspec_name:
                    old_name = "{0}/{1}.calib.dynspec".format(os.path.join(str(output_dir),"calibrated"),archive_name)

                new_name = os.path.join(ds_path,"{0}".format(dynspec_name))
                logger.info("Old name:{0}".format(old_name))
                logger.info("New name:{0}".format(new_name))
                os.rename(old_name,new_name)
                logger.info("Dynamic spectra generated and moved to Scintillation directory: {0}".format(dynspec_name))

                logger.info("Creating dynamic spectra plots using scintools")
                dynspec_file = glob.glob(os.path.join(ds_path,"{0}".format(dynspec_name)))[0]

                try:
                    dyn = Dynspec(dynspec_file, process=False, verbose=False)
                    dyn.plot_dyn(filename=os.path.join(ds_path,"{0}.png".format(dynspec_name)),display=False,title="{0}".format(dynspec_name))
                    logger.info("Refilling")
                    dyn.trim_edges()
                    dyn.refill(linear=False)
                    #logger.info("Secondary spectra")
                    #dyn.cut_dyn(tcuts=0, fcuts=7, plot=True, filename=os.path.join(ds_path,"{0}_subband.png".format(archive_name)))

                except:
                    logger.info("Scintools failed. Dyanmic spectra couldn't be created")

            else:
                logger.info("Dynamic spectra already exists")

            # calculate the RFI fraction
            if cparams["db_flag"]:
                rfi_frac = calc_dynspec_zap_fraction(os.path.join(ds_path,"{0}".format(dynspec_name)))
                if (float(rfi_frac) > max_rfi_frac):
                    max_rfi_frac = rfi_frac

            # cleanup temporary template
            logger.info("Cleaning up temporary template...")
            os.remove(temporary_template)

        # now for some tacked-on PSRDB stuff based on the highest RFI zap fraction
        if cparams["db_flag"]:

            logger.info("PSRDB functionality activated - recording zapped RFI fraction based on dynamic spectra")

            # Create client
            db_client = GraphQLClient(cparams["db_url"], False)

            # we've already calculated the maximum RFI zap fraction - recall results field and update
            results = get_results(cparams["db_proc_id"], db_client, cparams["db_url"], cparams["db_token"])
            results['zap_frac'] = float(max_rfi_frac)
            update_id = update_processing(
                cparams["db_proc_id"],
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                results,
                db_client,
                cparams["db_url"],
                cparams["db_token"]
            )
            if (update_id != cparams["db_proc_id"]) or (update_id == None):
                logger.error("Failure to update 'processings' entry ID {0} - PSRDB cleanup may be required.".format(cparams["db_proc_id"]))
            else:
                logger.info("Updated PSRDB entry in 'processings' table, ID = {0}".format(cparams["db_proc_id"]))

    else:
        logger.info("Template does not exist. Skipping dynamic spectra generation.")



def get_extension(commands,chopped):
    #Routine to generate extension based on pam command

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
                print (len(sflag))
                if len(sflag) > 4:
                    print (sflag)
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


            #Using tpa_decimation.list (in additional_info) for decimating cleaned archives.

            #Decimating on a fresh copy of the archive
            logger.info("Loading a fresh version of the archive file")

            cleaned_path = os.path.join(str(output_dir),"cleaned")
            cleaned_file = glob.glob(os.path.join(cleaned_path,archive_name+".ar"))[0]
            if os.path.exists(cleaned_file):
                cleaned_ar = ps.Archive_load(cleaned_file)
                freqs = cleaned_ar.get_frequencies().tolist()

            #get archive name
            archive_path,archive_name = os.path.split(str(cleaned_ar.get_filename()))
            archive_name = archive_name.split('.')[0]

            #decimation_info = np.genfromtxt(cparams["decimation_products"],delimiter=", ",dtype=str)
            decimation_info = pd.read_csv(cparams["decimation_products"],sep=", ", dtype=str, header=None, engine='python')
            decimation_info = decimation_info.replace(np.nan, 'None', regex=True)
            decimation_info = decimation_info.values.tolist()
            decimation_info = decimation_info[0]
            psrname = archive_name.split("_")[0]

            if len(freqs) == 928:
                logger.info("Recorded number of channels is 928. Producing decimated data products")
                for item in decimation_info:
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


            elif len(freqs) > 928:

                if os.path.exists(os.path.join(str(output_dir),"obs.header")):
                    header_params = get_obsheadinfo(os.path.join(str(output_dir),"obs.header"))
                else:
                    header_params = None

                #L-band data
                if not header_params["BW"] == "544.0":

                    # chopping functionality now replaced by abstracted utility function
                    chopping_utility(cleaned_ar,cleaned_path,archive_name,cparams,header_params,logger)

                    if os.path.exists(os.path.join(cleaned_path,archive_name+".ch.ar")):
                        chopped_cleaned_file = os.path.join(cleaned_path,archive_name+".ch.ar")

                        for item in decimation_info:
                            if not item == "all":
                                extension = get_extension(item,True)
                                if not os.path.exists(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension))):
                                    pam_command = "pam {0} -e {1} -u {2} {3}".format(item,extension,decimated_path,chopped_cleaned_file)
                                    logger.info("Producing {0} archives".format(extension))
                                    proc_pam = shlex.split(pam_command)
                                    subprocess.call(proc_pam)
                                    og_name = "{0}/{1}.ch.{2}".format(decimated_path,archive_name,extension)
                                    corrected_name = "{0}/{1}.{2}".format(decimated_path,archive_name,extension)
                                    os.rename(og_name,corrected_name)
                                    processed_archives.append(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension)))
                                else:
                                    logger.info("{0}.{1} exists".format(archive_name,extension))
                                    processed_archives.append(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension)))


                #UHF DATA
                elif header_params["BW"] == "544.0":
                    logger.info("No chopping required for UHF data. Just decimating products as per 1024 channel resolution")
                    if os.path.exists(os.path.join(cleaned_path,archive_name+".ar")):
                        cleaned_file = os.path.join(cleaned_path,archive_name+".ar")

                        # Adjust for 4096 channel count
                        if (header_params["NCHAN"] == "4096"):
                            # add a new decimation product
                            decimation_info.append('-f 4 -T -S')

                        for item in decimation_info:
                            if not item == "all":
                                #Scaling the scrunch factors to 1024 channels (only for UHF data)
                                if item == "-f 116 -T -S":
                                    if (header_params["NCHAN"] == "4096"):
                                        item = "-f 512 -T -S"
                                    else:
                                        item = "-f 128 -T -S"
                                if item == "-f 29 -T -S":
                                    if (header_params["NCHAN"] == "4096"):
                                        item = "-f 128 -T -S"
                                    else:
                                        item = "-f 32 -T -S"

                                extension = get_extension(item,False)
                                if not os.path.exists(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension))):
                                    pam_command = "pam {0} -e {1} -u {2} {3}".format(item,extension,decimated_path,cleaned_file)
                                    logger.info("Producing {0} UHF archives".format(extension))
                                    proc_pam = shlex.split(pam_command)
                                    subprocess.call(proc_pam)
                                    processed_archives.append(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension)))
                                else:
                                    logger.info("{0}.{1} exists".format(archive_name,extension))
                                    processed_archives.append(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension)))


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


        elif pid == "RelBin" or pid == "GC":
            #Using relbin_decimation.list (in additional_info) for decimating cleaned archives.

            #Decimating on a fresh copy of the archive
            logger.info("Loading a fresh version of the archive file")

            cleaned_path = os.path.join(str(output_dir),"cleaned")
            cleaned_file = glob.glob(os.path.join(cleaned_path,archive_name+".ar"))[0]

            if os.path.exists(cleaned_file):
                cleaned_ar = ps.Archive_load(cleaned_file)

                #get archive name
                archive_path,archive_name = os.path.split(str(cleaned_ar.get_filename()))
                archive_name = archive_name.split('.')[0]
                psrname = archive_name.split("_")[0]

                freqs = cleaned_ar.get_frequencies().tolist()


            decimation_info = pd.read_csv(cparams["decimation_products"],sep=", ", dtype=str, header=None, engine='python')
            decimation_info = decimation_info.replace(np.nan, 'None', regex=True)
            decimation_info = decimation_info.values.tolist()

            #If the recorded number of channels is 928 (Mostly L-band observations)
            if len(freqs) == 928:

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




            #If the recorded number of channels is greater than 928 (could be either L-band or UHF)

            elif len(freqs) > 928:

                if os.path.exists(os.path.join(str(output_dir),"obs.header")):
                    header_params = get_obsheadinfo(os.path.join(str(output_dir),"obs.header"))
                else:
                    header_params = None

                #L-BAND DATA
                if not header_params["BW"] == "544.0":

                    # chopping functionality now replaced by abstracted utility function
                    chopping_utility(cleaned_ar,cleaned_path,archive_name,cparams,header_params,logger)

                    if os.path.exists(os.path.join(cleaned_path,archive_name+".ch.ar")):
                        chopped_cleaned_file = os.path.join(cleaned_path,archive_name+".ch.ar")
                        for num in range(0,len(decimation_info)):
                            while 'None' in decimation_info[num]: decimation_info[num].remove('None')
                            if decimation_info[num][0] == psrname:
                                for item in decimation_info[num]:
                                    if not item == psrname:
                                        extension = get_extension(item,True)
                                        if not os.path.exists(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension))):
                                            pam_command = "pam {0} -e {1} -u {2} {3}".format(item,extension,decimated_path,chopped_cleaned_file)
                                            logger.info("Producing {0} chopped archives".format(extension))
                                            proc_pam = shlex.split(pam_command)
                                            subprocess.call(proc_pam)
                                            og_name = "{0}/{1}.ch.{2}".format(decimated_path,archive_name,extension)
                                            corrected_name = "{0}/{1}.{2}".format(decimated_path,archive_name,extension)
                                            os.rename(og_name,corrected_name)
                                            logger.info("Renamed to {0}".format(corrected_name))
                                            processed_archives.append(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension)))
                                        else:
                                            logger.info("{0}.{1} exists".format(archive_name,extension))
                                            processed_archives.append(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension)))

                #UHF DATA
                elif header_params["BW"] == "544.0":
                    logger.info("No chopping required for UHF data. Just decimating products as per 1024 channel resolution")
                    if os.path.exists(os.path.join(cleaned_path,archive_name+".ar")):
                        cleaned_file = os.path.join(cleaned_path,archive_name+".ar")
                        for num in range(0,len(decimation_info)):
                            while 'None' in decimation_info[num]: decimation_info[num].remove('None')
                            if decimation_info[num][0] == psrname:
                                for item in decimation_info[num]:
                                    if not item == psrname:

                                        #Scaling the scrunch factors to 1024 channels (only for UHF data)
                                        #if item == "-f 58 -t 8":
                                        #    item = "-f 64 -t 8"
                                        #if item == "-f 58 -t 8 -p":
                                        #    item = "-f 64 -t 8 -p"
                                        #if item == "-f 116 -t 128 -p":
                                        #    item = "-f 128 -t 128 -p"
                                        #if item == "-f 116 -t 32 -p":
                                        #    item = "-f 128 -t 32 -p"
                                        # making the above more general
                                        item.replace('-f 58', '-f 64').replace('-f 116', '-f 128')

                                        extension = get_extension(item,False)
                                        if not os.path.exists(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension))):
                                            pam_command = "pam {0} -e {1} -u {2} {3}".format(item,extension,decimated_path,cleaned_file)
                                            logger.info("Producing {0} UHF archives".format(extension))
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

            cleaned_path = os.path.join(str(output_dir),"cleaned")
            cleaned_file = glob.glob(os.path.join(cleaned_path,archive_name+".ar"))[0]
            if os.path.exists(cleaned_file):
                cleaned_ar = ps.Archive_load(cleaned_file)
                freqs = cleaned_ar.get_frequencies().tolist()

            #get archive name
            archive_path,archive_name = os.path.split(str(cleaned_ar.get_filename()))
            archive_name = archive_name.split('.')[0]

            #decimation_info = np.genfromtxt(cparams["decimation_products"],delimiter=", ",dtype=str)
            decimation_info = pd.read_csv(cparams["decimation_products"], sep=", ", dtype=str, header=None, engine='python')
            decimation_info = decimation_info.replace(np.nan, 'None', regex=True)
            decimation_info = decimation_info.values.tolist()
            decimation_info = decimation_info[0]
            psrname = archive_name.split("_")[0]

            if len(freqs) == 928:
                logger.info("Recorded number of channels is 928. Producing decimated data products")
                for item in decimation_info:
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


            elif len(freqs) > 928:

                if os.path.exists(os.path.join(str(output_dir),"obs.header")):
                    header_params = get_obsheadinfo(os.path.join(str(output_dir),"obs.header"))
                else:
                    header_params = None

                #L-band data
                if not header_params["BW"] == "544.0":

                    # chopping functionality now replaced by abstracted utility function
                    chopping_utility(cleaned_ar,cleaned_path,archive_name,cparams,header_params,logger)

                    if os.path.exists(os.path.join(cleaned_path,archive_name+".ch.ar")):
                        chopped_cleaned_file = os.path.join(cleaned_path,archive_name+".ch.ar")

                        for item in decimation_info:
                            if not item == "all":
                                extension = get_extension(item,True)
                                if not os.path.exists(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension))):
                                    pam_command = "pam {0} -e {1} -u {2} {3}".format(item,extension,decimated_path,chopped_cleaned_file)
                                    logger.info("Producing {0} archives".format(extension))
                                    proc_pam = shlex.split(pam_command)
                                    subprocess.call(proc_pam)
                                    og_name = "{0}/{1}.ch.{2}".format(decimated_path,archive_name,extension)
                                    corrected_name = "{0}/{1}.{2}".format(decimated_path,archive_name,extension)
                                    os.rename(og_name,corrected_name)
                                    processed_archives.append(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension)))
                                else:
                                    logger.info("{0}.{1} exists".format(archive_name,extension))
                                    processed_archives.append(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension)))


                #UHF DATA
                elif header_params["BW"] == "544.0":
                    logger.info("No chopping required for UHF data. Just decimating products as per 1024 channel resolution")
                    if os.path.exists(os.path.join(cleaned_path,archive_name+".ar")):
                        cleaned_file = os.path.join(cleaned_path,archive_name+".ar")
                        for item in decimation_info:
                            if not item == "all":
                                #Scaling the scrunch factors to 1024 channels (only for UHF data)
                                if item == "-t 32 -f 116 -p":
                                    item = "-t 32 -f 128 -p"
                                if item == "-T -f 29":
                                    item = "-T -f 32 "
                                if item == "-T -f 58":
                                    item = "-T -f 64"

                                extension = get_extension(item,False)
                                if not os.path.exists(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension))):
                                    pam_command = "pam {0} -e {1} -u {2} {3}".format(item,extension,decimated_path,cleaned_file)
                                    logger.info("Producing {0} UHF archives".format(extension))
                                    proc_pam = shlex.split(pam_command)
                                    subprocess.call(proc_pam)
                                    processed_archives.append(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension)))
                                else:
                                    logger.info("{0}.{1} exists".format(archive_name,extension))
                                    processed_archives.append(os.path.join(decimated_path,"{0}.{1}".format(archive_name,extension)))




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
            print (decimation_info)
            psrname = archive_name.split("_")[0]
            for num in range(0,en(decimation_info)):
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

# NEW - 16/08/2022
# Abstraction of the chopping code used in decimate_data()
# Future abstraction may be required, this is just a starting point
def chopping_utility(cleaned_ar,cleaned_path,archive_name,cparams,hparams,logger):

    # CHOPPING LBAND DATA TO 775.75 MHz
    logger.info("Extracting frequency channels from cleaned file since BW is 856 MHz")

    # recalling comparison frequency list (should be contiguous)
    reference_928ch_freqlist  = np.load(cparams["ref_freq_list"]).tolist()
    # cloning archive and ensuring it has not been dedispersed
    oar = cleaned_ar.clone()
    dd = oar.get_dedispersed()
    if dd:
        oar.dededisperse()

    # chopping channels
    # check for channel count in header parameters
    if (hparams == None):
        nchan = 1024
    else:
        nchan = int(hparams["FOLD_OUTNCHAN"])

    # in theory, the new chopping technique is faster and would work for 1024
    # however, just in case there's some caveat I haven't spotted, I will only implement it
    # for non-1024 channel data

    if (nchan == 1024):

        logger.info("Defaulting to standard 1024 channel procedure...")

        # complex structure required as with every channel removal, indexes of oar get reset
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
    else:

        logger.info("Applying new chopping algorithm (designed for non-1024 channel configurations...")

        # assumes continuous block of channels to zap, which should be true or something has gone seriously wrong
        # also assumes positive bandwidth
        # calculate channel bandwidth and min/max frequencies
        chbw = np.abs((reference_928ch_freqlist[0] - reference_928ch_freqlist[len(reference_928ch_freqlist) - 1])/(len(reference_928ch_freqlist) - 1))
        minfreq = reference_928ch_freqlist[0] - chbw/2
        maxfreq = reference_928ch_freqlist[len(reference_928ch_freqlist) - 1] + chbw/2

        recheck=True
        while recheck:
            recheck=False
            freqs = oar.get_frequencies()
            if (freqs[0] < minfreq):
                for i,f in enumerate(freqs):
                    if (f <= minfreq):
                        pass
                    else:
                        oar.remove_chan(0, i-1)
                        recheck=True
                        break
            elif (freqs[len(freqs) - 1] > maxfreq):
                for i,f in enumerate(freqs):
                    if (f <= maxfreq):
                        pass
                    else:
                        oar.remove_chan(i, len(freqs) - 1)
                        recheck=True
                        break

    logger.info("Done extracting")
    # dedisperse is previously true
    if dd:
        oar.dedisperse()

    # write file if it does not already exist
    if not os.path.exists(os.path.join(cleaned_path,archive_name+".ch.ar")):
        oar.unload(os.path.join(cleaned_path,archive_name+".ch.ar"))
        logger.info("Unloaded extracted file")
    else:
        logger.info("Chopped cleaned file already exists")

    return


def fluxcalibrate(output_dir, cparams, psrname, logger):

    logger.info("Beginning flux calibration routine...")

    # needed for a hack later
    del_string = "delme"

    obsheader_path = glob.glob(os.path.join(str(output_dir), "*obs.header"))[0]
    header_params = get_obsheadinfo(obsheader_path)

    #parfile = glob.glob(os.path.join(cparams['meertime_ephemerides'], "{}*par".format(psrname)))
    #parfile = glob.glob(os.path.join(str(output_dir),"{0}.par".format(psrname)))

    #parfile = []
    parfile = None
    #logger.info("Parfile = {}".format(parfile))

    if len(glob.glob(os.path.join(cparams["meertime_ephemerides"],"{0}.par".format(psrname)))) > 0:
        parfile = glob.glob(os.path.join(cparams['meertime_ephemerides'], "{0}.par".format(psrname)))[0]
    elif len(glob.glob(os.path.join(cparams["meertime_ephemerides"],"{0}_p2.par".format(psrname)))) > 0:
        parfile = glob.glob(os.path.join(cparams["meertime_ephemerides"],"{0}_p2.par".format(psrname)))[0]
    elif len(glob.glob(os.path.join(cparams["meertime_ephemerides"],"{0}_p3.par".format(psrname)))) > 0:
        parfile = glob.glob(os.path.join(cparams["meertime_ephemerides"],"{0}_p3.par".format(psrname)))[0]

    #logger.info("Parfile = {}".format(parfile))

    if not parfile == None:
        logger.info("Parfile found: {}".format(parfile))
    else:
        logger.warning("No par file found for {}".format(psrname))
        parfile = None

    #if len(parfile) == 0:
        #logger.warning("No par file found for "+psrname)
        #parfile = None
    #else:
        #parfile = parfile[0]
        #logger.info("Parfile found: {}".format(parfile))

    # determine receiver based on header information
    rcvr = get_rcvr(header_params)

    if (rcvr == "UHF") or (rcvr == "LBAND") :
        logger.info("Flux calibrating the decimated data products of {0}".format(psrname))
        pid = cparams["pid"]
        decimated_path = os.path.join(str(output_dir), "decimated")
        obsname = decimated_path.split("/")[-4]
        decimated_archives = sorted(glob.glob(os.path.join(decimated_path, "J*.ar")))
        logger.info("Also adding the cleaned file for flux calibration")
        cleaned_archive = glob.glob(os.path.join(str(output_dir), "cleaned", "J*ar"))

        if len(cleaned_archive) > 1:
            for clean_ar in cleaned_archive:
                decimated_archives.append(clean_ar)
        else:
            decimated_archives.append(cleaned_archive[0])

        TP_file = None

        for archive in decimated_archives:
            if pid == "TPA" and "Tp" in archive:
                TP_file = archive
            elif pid == "PTA" and "t32p" in archive:
                TP_file = archive
            elif (pid == "RelBin" or pid == "GC") and "Tp" in archive:
                TP_file = archive

        if (TP_file == None):
            # HACK - this file is required, so we need to build one and patch it back in
            clarch = None
            for entry in cleaned_archive:
                if ".ch.ar" not in entry:
                    clarch = entry
            if (clarch == None):
                clarch = cleaned_archive[0]
            # cleaned archive selected - now scrunch it
            comm = "pam -Tp -u {0} -e {1} {2}".format(decimated_path, del_string, clarch)
            args = shlex.split(comm)
            proc = subprocess.Popen(args,stdout=subprocess.PIPE)
            proc.wait()
            TP_file = proc.stdout.read().decode("utf-8").rstrip().split()[0]

        if (TP_file == None):
            raise Exception("TP_file not set in fluxcalibrate() - check into this, because apparently this file is important!")
        else:
            logger.info("TP_file = {0}".format(TP_file))

        addfile = glob.glob(os.path.join(str(output_dir), "*add"))[0]

        np.save(os.path.join(decimated_path, "decimatedlist"), decimated_archives)
        decimated_list = os.path.join(decimated_path, "decimatedlist.npy")
        par_opt = " -parfile "+parfile if parfile is not None else ""

        fluxcal_command = "python fluxcal.py -psrname {0} -obsname {1} -obsheader {2} -TPfile {3} -rawfile {4} -dec_path {5}{6}".format(psrname, obsname, obsheader_path, TP_file, addfile, decimated_list, par_opt)

        fluxcalproc = shlex.split(fluxcal_command)
        try:
            subprocess.check_call(fluxcalproc)
        except subprocess.CalledProcessError:
            logger.error("fluxcal failed")

        # clean the hack
        if (del_string in TP_file):
            os.remove(TP_file)

        fluxcal_obs = glob.glob(os.path.join(decimated_path, "*.fluxcal"))
        archives_indecimated = glob.glob(os.path.join(decimated_path, "*.ar"))
        if len(fluxcal_obs) == len(archives_indecimated):
            logger.info("All decimated observations of {0}:{1} are flux calibrated".format(psrname, obsname))

            # Add flux estimation to PSRDB
            if cparams["db_flag"] and len(fluxcal_obs) > 0:

                logger.info("PSRDB functionality activated - recording flux density estimate")

                # Create client
                db_client = GraphQLClient(cparams["db_url"], False)

                # calculate the fully scrunched flux density - any decimated product should do
                comm = "pdv -FTp -f {0}".format(fluxcal_obs[0])
                args = shlex.split(comm)
                proc = subprocess.Popen(args,stdout=subprocess.PIPE)
                proc.wait()
                info = proc.stdout.read().decode("utf-8")
                flux = info.split("\n")[1].split()[6]

                # Recall results field and update
                results = get_results(cparams["db_proc_id"], db_client, cparams["db_url"], cparams["db_token"])
                results['flux'] = float(flux)
                update_id = update_processing(
                    cparams["db_proc_id"],
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    results,
                    db_client,
                    cparams["db_url"],
                    cparams["db_token"]
                )
                if (update_id != cparams["db_proc_id"]) or (update_id == None):
                    logger.error("Failure to update 'processings' entry ID {0} - PSRDB cleanup may be required.".format(cparams["db_proc_id"]))
                else:
                    logger.info("Updated PSRDB entry in 'processings' table, ID = {0}".format(cparams["db_proc_id"]))

            if pid == "TPA" or pid == "PTA":
                logger.info("Removing non-flux calibrated archives from the decimated directory...") #change this in future
                for archive in archives_indecimated:
                    os.remove(archive)
        else:
            logger.warning("Flux calibration failed")

    else:
        logger.info("Unknown receiver ({0}) - flux calibration not yet implemented.".format(rcvr))
        pass

def generate_toas(output_dir,cparams,psrname,logger):
    # Routine to call pat

    psrname = str(psrname)
    logger.info("Generating ToAs")
    output_path = cparams["output_path"]
    orig_psrname = psrname
    local_pid = cparams["pid"].lower()

    #Setting required paths
    timing_path = os.path.join(str(output_dir),"timing")
    pulsar_dir = os.path.join(output_path,str(psrname))

    #Obtaining templates
    logger.info("Obtaining templates for timing")
    template = get_meertimetemplate(psrname,output_path,cparams,logger)

    if len(glob.glob(os.path.join(str(output_dir),"{0}.par".format(psrname)))) > 0:
        parfile = glob.glob(os.path.join(str(output_dir),"{0}.par".format(psrname)))[0]
        copy_parfile = os.path.join(timing_path,"{0}.par".format(psrname))
    elif len(glob.glob(os.path.join(str(output_dir),"{0}_p2.par".format(psrname)))) > 0:
        parfile = glob.glob(os.path.join(str(output_dir),"{0}_p2.par".format(psrname)))[0]
        copy_parfile = os.path.join(timing_path,"{0}_p2.par".format(psrname))
    elif len(glob.glob(os.path.join(str(output_dir),"{0}_p3.par".format(psrname)))) > 0:
        parfile = glob.glob(os.path.join(str(output_dir),"{0}_p3.par".format(psrname)))[0]
        copy_parfile = os.path.join(timing_path,"{0}_p3.par".format(psrname))

    if os.path.exists(parfile):
        os.rename(parfile,copy_parfile)
        logger.info("Ephemeris copied to the timing directory")

    decimated_path = os.path.join(str(output_dir),"decimated")
    processed_archives = sorted(glob.glob(os.path.join(decimated_path,"J*.ar")))

    if not template is None:

        #Creating a select file
        select_file = open("{0}/{1}.select".format(timing_path,orig_psrname),"w")
        select_file.write("LOGIC -snr < 10 REJECT \n")
        select_file.close()

        for proc_archive in processed_archives:
            tim_name = os.path.split(proc_archive)[1].split('.ar')[0]+".tim"
            #Running pat

            # NEW - August 2022 - Check for channel and subintegration count of decimated product
            # If they are too high, do not create TOAs - PAT will seize up and the job will time out
            # Experimentation will be needed to determine the appropriate limiting channel count

            comm = "vap -c nchan,nsub {0}".format(proc_archive)
            args = shlex.split(comm)
            proc = subprocess.Popen(args,stdout=subprocess.PIPE)
            proc.wait()
            info = proc.stdout.read().decode("utf-8").split("\n")
            nchan = int(info[1].split()[1])
            nsub = int(info[1].split()[2])

            if not ((nchan >= 2048) and (nsub > 1)):

                if not os.path.exists(os.path.join(timing_path,tim_name)):
                    logger.info("Creating ToAs with pat")
                    logger.info(proc_archive)

                    # check PORTRAIT toggle
                    portrait_str = get_portrait_toggle(template, nchan, logger)

                    arg = 'pat -jp {2} -f "tempo2 IPTA" -C "chan rcvr snr length subint" -s {0} -A FDM {1}'.format(template,proc_archive, portrait_str)
                    proc = shlex.split(arg)
                    f = open("{0}/{1}".format(timing_path,tim_name), "w")
                    subprocess.call(proc, stdout=f)
                    logger.info("{0} generated".format(tim_name))
                    f.close()

                    #Creating a meerwatch launch file
                    logger.info("{0}.launch file for MeerWatch".format(psrname))
                    mw_launch = open("{0}/{1}.launch".format(str(output_dir),str(psrname)),"w")
                    mw_launch.write("Launch_MeerWatch. MeerPipe successful")
                    mw_launch.close()
                else:
                    logger.info("{0} file exists. Skipping ToA computation.".format(tim_name))

            else:
                # report inability to create TOA
                logger.info("ALERT: Decimated product channel count of {0} and subintegration count of {1} are too large for TOA production - skipping...".format(nchan, nsub))

        # create the relevant entries in PSRDB to summarise the TOA production
        # create / recall the ephemeris and template entries
        if cparams["db_flag"]:

            logger.info("TOA PSRDB functionality activated - recording TOA production")

            # Create client
            db_client = GraphQLClient(cparams["db_url"], False)

            if not cparams["fluxcal"]:

                # chose a suitable processed archive for summary TOA production
                proc_archive = None
                chop_string = ".ch"
                fluxcal_string = "fluxcal"
                for archive in processed_archives:
                    if chop_string not in archive:
                        # first preference - unchopped archives
                        proc_archive = archive

                if (proc_archive == None):
                    if (len(processed_archives) > 0):
                        # second preference - chopped archives
                        proc_archive = processed_archives[0]
                    else:
                        # need to check the cleaned archives
                        cleaned_path = os.path.join(str(output_dir),"cleaned")
                        cleaned_archives = sorted(glob.glob(os.path.join(cleaned_path,"J*.ar")))
                        for archive in cleaned_archives:
                            if fluxcal_string in archive and chop_string not in archive:
                                # third preference - unchopped fluxcal archive
                                proc_archive = archive

                        if (proc_archive == None):
                            for archive in cleaned_archives:
                                # fourth preference - unchopped unfluxcal archive
                                if chop_string not in archive:
                                    proc_archive = archive

                if (proc_archive == None):
                    raise Exception("Unable to identify file to use in PSRDB TOA production - investigation required.")

                # load and convert the ephemeris
                eph = ephemeris.Ephemeris()
                eph.load_from_file(copy_parfile)

                # NEW - 30/08/2022
                # Now uploading the ephemeris file to the pipelinefiles table in PSRDB
                file_type = "{0}.parfile".format(local_pid)
                if (os.path.exists(copy_parfile)):
                    logger.info("File {0} identified (type {1}) - recording to PSRDB.".format(copy_parfile, file_type))
                    create_pipelinefile(copy_parfile, file_type, cparams, db_client, logger)

                # recall the DM, RM and site code being used by this file
                comm = "vap -c dm,rm,asite {0}".format(proc_archive)
                args = shlex.split(comm)
                proc = subprocess.Popen(args,stdout=subprocess.PIPE)
                proc.wait()
                info = proc.stdout.read().decode("utf-8").split("\n")
                dm = float(info[1].split()[1])
                rm = float(info[1].split()[2])
                site = info[1].split()[3]

                # call the ephemeris and template creation functions
                eph_id = create_ephemeris(psrname, eph, dm, rm, cparams, db_client, logger)
                template_id = create_template(psrname, template, cparams, db_client, logger)

                # check output and report

                logger.info("Used ephemeris ID {0}.".format(eph_id))
                logger.info("Used template ID {0}.".format(template_id))

                # gather required TOA information
                quality = True # assumed for the moment

                # ensure this pat comment closely matches the one above
                # summary TOA only - unaffected by PORTRAIT modes
                comm = 'pat -jFTp -f "tempo2 IPTA" -C "chan rcvr snr length subint" -s {0} -A FDM {1}'.format(template, proc_archive)
                # note that 'proc_archive' could be any type of decimated file
                # (here taken asthe last entry in the processed_archives list)
                # however, as the above command fully scrunches it, it doesn't matter what choice we made
                args = shlex.split(comm)
                proc = subprocess.Popen(args,stdout=subprocess.PIPE)
                proc.wait()
                info = proc.stdout.read().decode("utf-8").split("\n")[1].split()
                # this deconstruction may change depending on formatting of the pat command
                freq = float(info[1])
                mjd = float(info[2])
                uncertainty = float(info[3])
                # parse flags
                flags_dict = {}
                for x in range(5, len(info), 2):
                    flags_dict[info[x]] = info[x+1]

                flags_json = json.dumps(flags_dict)
                flags = json.loads(flags_json)

                # link via entry in TOA table
                toa_id = create_toa_record(eph_id, template_id, flags, freq, mjd, site, uncertainty, quality, cparams, db_client, logger)

                logger.info("Entry in table 'toas' successfully created - ID {0}".format(toa_id))

    else:
        logger.error("Template does not exist or does not have 1024 phase bins. Skipping ToA generation.")

# determines whether to toggle portrait mode and returns the appropriate PAT string
def get_portrait_toggle(template, ref_nchan, logger):

    # set default
    portrait_str = ""

    # check for a valid template
    if not (template == None) and (os.path.exists(template)):

        # get template channel count
        comm = "vap -c nchan {0}".format(template)
        args = shlex.split(comm)
        proc = subprocess.Popen(args,stdout=subprocess.PIPE)
        proc.wait()
        info = proc.stdout.read().decode("utf-8").split("\n")
        template_nchan = int(info[1].split()[1])

        # check for toggle
        if (template_nchan >= ref_nchan) and not (template_nchan == 1):
            # PORTRAIT MODE ON
            portrait_str = "-P"
            logger.info("Portrait mode activated for current timing data product...")

    return portrait_str

def cleanup(output_dir, cparams, psrname, logger):
    #Routine to rename, remove and clean the final output files produced by the pipeline

    logger.info("Running a clean up")

    output_dir = str(output_dir)

    #Moving template to the timing directory
    timing_dir = os.path.join(output_dir,"timing")
    if os.path.exists(os.path.join(output_dir,"{0}.std".format(psrname))):
        stdfile = glob.glob(os.path.join(output_dir,"{0}.std".format(psrname)))[0]
        stdfile_timing = os.path.join(timing_dir,"{0}.std".format(psrname))
        os.rename(stdfile,stdfile_timing)
    elif os.path.exists(os.path.join(output_dir,"{0}_p2.std".format(psrname))):
        stdfile = glob.glob(os.path.join(output_dir,"{0}_p2.std".format(psrname)))[0]
        stdfile_timing = os.path.join(timing_dir,"{0}_p2.std".format(psrname))
        os.rename(stdfile,stdfile_timing)
    elif os.path.exists(os.path.join(output_dir,"{0}_p3.std".format(psrname))):
        stdfile = glob.glob(os.path.join(output_dir,"{0}_p3.std".format(psrname)))[0]
        stdfile_timing = os.path.join(timing_dir,"{0}_p3.std".format(psrname))
        os.rename(stdfile,stdfile_timing)
    elif os.path.exists(os.path.join(output_dir,"Gaussian.std")):
        stdfile = glob.glob(os.path.join(output_dir,"Gaussian.std"))[0]
        stdfile_timing = os.path.join(timing_dir,"Gaussian.std")
        os.rename(stdfile,stdfile_timing)

    logger.info("Timing standard renamed")

    #Renaming cleaned and decimated archives
    cleaned_dir = os.path.join(output_dir,"cleaned")
    decimated_dir = os.path.join(output_dir,"decimated")

    cleaned_files = sorted(glob.glob(os.path.join(cleaned_dir,"J*")))
    if len(cleaned_files) > 0:
        for archive in cleaned_files:
            path,name = os.path.split(archive)
            sname = name.split("_")
            if sname[-1] == "zap.fluxcal":
                logger.info("Renaming .fluxcal to .fluxcal.ar (cleaned file)")
                new_extension = "zap.fluxcal.ar"
                new_name = "{0}_{1}_{2}".format(sname[0],sname[1],new_extension)
                renamed_archive = os.path.join(cleaned_dir,new_name)
                os.rename(archive,renamed_archive)

            if sname[-1] == "zap.ch.fluxcal":
                logger.info("Renaming .ch.fluxcal to .ch.fluxcal.ar (cleaned file)")
                new_extension = "zap.ch.fluxcal.ar"
                new_name = "{0}_{1}_{2}".format(sname[0],sname[1],new_extension)
                renamed_archive = os.path.join(cleaned_dir,new_name)
                os.rename(archive,renamed_archive)


    obsheader_path = glob.glob(os.path.join(str(output_dir),"*obs.header"))[0]
    header_params = get_obsheadinfo(obsheader_path)
    rcvr = get_rcvr(header_params)

    #if not header_params["BW"] == "544.0":
    #    decimated_files = sorted(glob.glob(os.path.join(decimated_dir,"J*fluxcal")))
    #else:
    #    decimated_files = sorted(glob.glob(os.path.join(decimated_dir,"J*ar")))

    if (rcvr == "UHF") or (rcvr == "LBAND"):
        decimated_files = sorted(glob.glob(os.path.join(decimated_dir,"J*fluxcal")))
    else:
        decimated_files = sorted(glob.glob(os.path.join(decimated_dir,"J*ar")))

    if len(decimated_files) > 0:
        for archive in decimated_files:
            path,name = os.path.split(archive)
            sname = name.split("_")

            #Get archive properties
            ar = ps.Archive_load(archive)
            nchan = ar.get_nchan()
            nsubint = ar.get_nsubint()
            npol = ar.get_npol()

            if nchan == 1:
                ext_nch = "F"
            elif nchan > 1:
                ext_nch = "{0}ch".format(nchan)

            if nsubint == 1:
                ext_nsubint = "T"
            elif nsubint > 1:
                ext_nsubint = "none"

            if npol == 4:
                ext_pol = "S"
            else:
                ext_pol = "I"


            #if not header_params["BW"] == "544.0":
            if (rcvr == "UHF") or (rcvr == "LBAND"):
                if ext_nsubint == "T":
                    new_extension = "zap.{0}{1}{2}.fluxcal.ar".format(ext_nch,ext_nsubint,ext_pol)
                else:
                    new_extension = "zap.{0}{1}.fluxcal.ar".format(ext_nch,ext_pol)

            else:
                if ext_nsubint == "T":
                    new_extension = "zap.{0}{1}{2}.ar".format(ext_nch,ext_nsubint,ext_pol)
                else:
                    new_extension = "zap.{0}{1}.ar".format(ext_nch,ext_pol)

            new_name = "{0}_{1}_{2}".format(sname[0],sname[1],new_extension)
            renamed_archive = os.path.join(decimated_dir,new_name)
            os.rename(archive,renamed_archive)

            if "TI" in new_name:
                os.remove(renamed_archive)

        logger.info("Renamed all decimated archives")

    numpy_decimated_file = glob.glob(os.path.join(decimated_dir,"*npy"))
    if len(numpy_decimated_file) > 0:
        os.remove(numpy_decimated_file[0])


def generate_summary(output_dir, cparams, psrname, logger):

    #Routine to create a summary file for each UTC - final stage of processing pipeline

    output_dir = str(output_dir)
    split_path = output_dir.split("/")
    #psrname = split_path[7] - psrname is already defined in the function arguments
    path_args = len(split_path)
    utcname = split_path[path_args - 4]
    # Counting backwards seems more logical than counting forwards, given that the absolute path may change
    # depending on where results are being stored, while the lower directory structure remains constant.

    # This will need modification once the S-Band receiver comes online. I have already made an appropriate start.
    if str(split_path[path_args - 2]) == "816" or str(split_path[path_args - 2]) == "815":
        rcvr = "UHF"
    elif str(split_path[path_args - 2]) == "1284" or str(split_path[path_args - 2]) == "1283":
        rcvr = "L-band"
    else:
        rcvr = "RCVR Unknown"

    summaryfile = os.path.join(output_dir,"{0}_{1}.summary".format(psrname,utcname))
    if os.path.exists(summaryfile):
        os.remove(summaryfile)

    with open(summaryfile,"w") as sfile:
        sfile.write("{0} -- {1} -- {2} \n".format(psrname,utcname,rcvr))

        #Checking if meerpipe log file exists (only if SLURM launched)
        mpipe_out = glob.glob(os.path.join(output_dir,"meerpipe_out*"))
        bfile = glob.glob(os.path.join(output_dir,"*.bash"))
        if len(mpipe_out) > 0:
            sfile.write("MeerPipeLog: CHECK \n")
        elif len(bfile) > 0:
            sfile.write("MeerPipeLog: FAIL \n")


        #Checking if ADD file exists
        add_file = glob.glob(os.path.join(output_dir,"*add"))
        if len(add_file) > 0:
            sfile.write("ADDfile: CHECK \n")
        else:
            sfile.write("ADDfile: FAIL \n")


        #Checking if obs.header exists
        obsheader = glob.glob(os.path.join(output_dir,"obs.header"))
        if len(obsheader) > 0:
            sfile.write("ObsHeader: CHECK \n")
        else:
            sfile.write("ObsHeader: FAIL \n")


        #Checking if calibrated file exists
        calibrated_path = os.path.join(output_dir,"calibrated")
        calibfile = glob.glob(os.path.join(calibrated_path,"*.calib"))
        if len(calibfile) > 0:
            sfile.write("PolnCalibration: CHECK \n")
        else:
            sfile.write("PolnCalibration: FAIL \n")


        #Checking if cleaned files exists
        cleaned_path = os.path.join(output_dir,"cleaned")
        cleanedfiles = glob.glob(os.path.join(cleaned_path,"J*.ar"))

        # Holding this code to be re-used when S-Band is deployed.
        """
        if (rcvr == "UHF"):
            if (len(cleanedfiles) == 2):
                if ((".ch" in cleanedfiles[0] or ".ch" in cleanedfiles[1]) and not (".ch" in cleanedfiles[0] and ".ch" in cleanedfiles[1])):
                    sfile.write("CleanedChoppedFiles: CHECK \n")
                else:
                    sfile.write("CleanedChoppedFiles: FAIL \n")
            elif (len(cleanedfiles) == 1 and ".ch" not in cleanedfiles[0]):
                sfile.write("CleanedFiles: CHECK \n")
            else:
                sfile.write("CleanedFiles: FAIL \n")

        else:
            if len(cleanedfiles)  == 2:
                sfile.write("CleanedFluxFiles: CHECK \n")
            elif len(cleanedfiles) < 2:
                sfile.write("CleanedFluxFiles: FAIL \n")

            elif len(cleanedfiles) == 4:
                sfile.write("CleanedChoppedFluxFile: CHECK \n")
            elif len(cleanedfiles) < 4 and len(cleanedfiles) > 2:
                sfile.write("CleanedChoppedFluxFile: FAIL \n")
        """

        if len(cleanedfiles)  == 2:
            sfile.write("CleanedFluxFiles: CHECK \n")
        elif len(cleanedfiles) < 2:
            sfile.write("CleanedFluxFiles: FAIL \n")
        elif len(cleanedfiles) == 4:
            sfile.write("CleanedChoppedFluxFile: CHECK \n")
        elif (len(cleanedfiles) < 4 and len(cleanedfiles) > 2) or (len(cleanedfiles) > 4):
            sfile.write("CleanedChoppedFluxFile: FAIL \n")

        #Checking if decimated files exist
        decimated_path = os.path.join(output_dir,"decimated")
        decimatedfiles = glob.glob(os.path.join(decimated_path,"J*.ar"))
        if len(decimatedfiles) > 0:
            sfile.write("DecimatedFiles: CHECK {0} \n".format(len(decimatedfiles)))
        else:
            sfile.write("DecimatedFiles: FAIL \n")


        #Checking if scintillation files exist
        scint_path = os.path.join(output_dir,"scintillation")
        scintfiles = glob.glob(os.path.join(scint_path,"J*"))
        if len(scintfiles) == 4:
            sfile.write("ScintillationFiles: CHECK \n")
        else:
            sfile.write("ScintillationFiles: FAIL \n")


        #Checking if timing files exist
        timing_path = os.path.join(output_dir,"timing")
        timingfiles = glob.glob(os.path.join(timing_path,"J*tim"))

        # setup to prevent invalid variable access
        parfile = []
        stdfile = []

        if len(glob.glob(os.path.join(timing_path,"{0}.par".format(psrname))))>0:
            parfile = glob.glob(os.path.join(timing_path,"{0}.par".format(psrname)))
        elif len(glob.glob(os.path.join(timing_path,"{0}_p2.par".format(psrname))))>0:
            parfile = glob.glob(os.path.join(timing_path,"{0}_p2.par".format(psrname)))
        elif len(glob.glob(os.path.join(timing_path,"{0}_p3.par".format(psrname))))>0:
            parfile = glob.glob(os.path.join(timing_path,"{0}_p3.par".format(psrname)))

        if len(glob.glob(os.path.join(timing_path,"{0}.std".format(psrname))))>0:
            stdfile = glob.glob(os.path.join(timing_path,"{0}.std".format(psrname)))
        elif len(glob.glob(os.path.join(timing_path,"{0}_p2.std".format(psrname))))>0:
            stdfile = glob.glob(os.path.join(timing_path,"{0}_p2.std".format(psrname)))
        elif len(glob.glob(os.path.join(timing_path,"{0}_p3.std".format(psrname))))>0:
            stdfile = glob.glob(os.path.join(timing_path,"{0}_p3.std".format(psrname)))
        elif len(glob.glob(os.path.join(timing_path,"Gaussian.std")))>0:
            stdfile = glob.glob(os.path.join(timing_path,"Gaussian.std"))



        if len(timingfiles) == len(decimatedfiles):
            sfile.write("TimingFiles: CHECK \n")
        else:
            sfile.write("TimingFiles: FAIL \n")

        if len(parfile) > 0:
            sfile.write("PARFile: CHECK \n")
        else:
            sfile.write("PARFile: FAIL \n")

        if len(stdfile) > 0:
            sfile.write("STDFile: CHECK \n")
        else:
            sfile.write("STDFile: FAIL \n")



        sfile.write("=========== END ========= \n")
        sfile.close()

# secondary cleanup to remove redundant products at the very end of the pipeline for space saving
# similar structure to generate_summary, except now we're deleting things
def secondary_cleanup(output_dir, cparams, psrname, logger):

    logger.info("Now beginning secondary cleanup (if requested)")

    output_dir = str(output_dir)

    # cleanup the data transfer products (numpy and binary files) - moved from cleanup() in order to accomodate image-only re-processing
    numpy_files = sorted(glob.glob(os.path.join(output_dir,"*.npy")))
    config_params_binary = glob.glob(os.path.join(output_dir,"config_params.p"))

    if len(numpy_files) > 0:
        for item in numpy_files:
            os.remove(item)
        logger.info("Removed numpy files")

    if len(config_params_binary) > 0:
        for item in config_params_binary:
            os.remove(item)
        logger.info("Removed binary files")

    # check if this has been requested
    if "red_prod" in cparams:

        # check if the parameter is set to skip
        if not "none" in cparams["red_prod"]:

            # check which files have been requested for deletion

            if "add" in cparams["red_prod"]:
                # delete add file
                add_file = glob.glob(os.path.join(output_dir,"*add"))
                for x in range(0, len(add_file)):
                    logger.info("Removing {}...".format(add_file[x]))
                    os.remove(add_file[x])

            if "calib" in cparams["red_prod"]:
                # delete the calibrated file
                calibrated_path = os.path.join(output_dir,"calibrated")
                calibfile = glob.glob(os.path.join(calibrated_path,"*.calib"))
                for x in range(0, len(calibfile)):
                    logger.info("Removing {}...".format(calibfile[x]))
                    os.remove(calibfile[x])

            if "zap_noflux" in cparams["red_prod"]:
                # delete the cleaned but not flux calibrated files
                # safeguard - if there aren't matching fluxcal files, these files will not be deleted
                cleaned_path = os.path.join(output_dir,"cleaned")
                cleanedfiles = glob.glob(os.path.join(cleaned_path,"J*.ar"))
                fluxfiles = glob.glob(os.path.join(cleaned_path,"J*.fluxcal.ar"))

                # only delete files for which a matching fluxcal file exists
                for x in range(0, len(fluxfiles)):
                    noflux_file = fluxfiles[x].replace('.fluxcal','')
                    if noflux_file in cleanedfiles:
                        logger.info("Removing {}...".format(noflux_file))
                        os.remove(noflux_file)

        else:
            logger.info("No secondary cleanup performed")

    else:
        logger.info("No secondary cleanup performed")


#--------------------------------------------------- Andrew's PSRDB-related utilities -------------------------------

# routine to check the pass/fail status of a processing's summary file
# returns True or False
def check_summary(output_dir, logger):

    output_dir = str(output_dir)

    # get the summary file and check that there is only one
    summaryfile = glob.glob(os.path.join(output_dir,"*.summary"))
    if (len(summaryfile) != 1):
        raise Exception("Checking for summary file in %s\nInvalid number of files (%d) found." % (output_dir, len(summaryfile)))

    # check contents of file - depends on the behaviour of generate_summary()
    # currently, number of lines with "CHECK" should be two less than the total linecount (header and footer)
    passString = "CHECK"
    failString = "FAIL"

    summaryfile_handle = open(summaryfile[0], 'r')
    sumLines = summaryfile_handle.readlines()

    lines_total = 0
    lines_pass = 0
    lines_fail = 0

    for line in sumLines:

        lines_total += 1

        if passString in line:
            lines_pass += 1
        if failString in line:
            lines_fail += 1

    if (lines_pass == lines_total - 2) and (lines_fail == 0):
        retval = True
    else:
        retval = False

    return retval

# calculate the zapped fraction of a file based on the dynspec file
def calc_dynspec_zap_fraction(dynspec_file):

    if (os.path.isfile(dynspec_file)):
        # convert the dynspec file into a numpy array
        data = np.loadtxt(dynspec_file, comments='#')

        # loop through and count
        zap_lines = 0
        for x in range(0, len(data)):

            # check for zap condition
            if (float(data[x][4]) == 0) and (float(data[x][5]) == 0):
                zap_lines = zap_lines + 1

        retval = float(zap_lines)/float(len(data))

    else:
        raise Exception ("File {0} cannot be found".format(dynspec_file))

    return retval

# produce PSRDB images for website upload
# WORK IN PROGRESS!
def generate_images(output_dir, cparams, psrname, logger):
# def generate_images(
#         output_dir,
#         psrname,
#         cparams=None,
#         logger=None,
#     ):
#     # Load logger if no provided
#     if logger is None:
#         logger = setup_logging(
#             filedir=output_dir,
#             console=True,
#         )

    # Note - the functionality of this code is based on the outputs expected by 'generate_summary'
    # Should these expected outputs change, the conditions of this code should be re-assessed

    logger.info("Generating pipeline images - Pipeline PID = {0}".format(cparams["pid"]))
    # update - pid now to be included in the naming structure (type)
    local_pid = cparams["pid"].lower()

    # produce images based on unscrunched, unchopped, cleaned and flux-calibrated archives
    output_dir = str(output_dir)
    cleaned_path = os.path.join(output_dir,"cleaned")
    images_path = os.path.join(output_dir,"images")
    timing_path = os.path.join(output_dir, "timing")
    cleanedfiles = glob.glob(os.path.join(cleaned_path,"J*.ar"))
    fluxcleanedfiles = glob.glob(os.path.join(cleaned_path,"J*fluxcal.ar"))

    # new 27/01/2023 - TOA image file must always be the chopped file, invert logic for selection
    clean_file = None
    toa_file = None
    chop_string = ".ch."

    # try for a fluxcal file first
    if (len(fluxcleanedfiles) == 1):
        clean_file = fluxcleanedfiles[0]
        toa_file = fluxcleanedfiles[0]
    elif (len(fluxcleanedfiles) == 2):
        if (chop_string in fluxcleanedfiles[0]) and (chop_string not in fluxcleanedfiles[1]):
            clean_file = fluxcleanedfiles[1]
            toa_file = fluxcleanedfiles[0]
        elif (chop_string in fluxcleanedfiles[1]) and (chop_string not in fluxcleanedfiles[0]):
            clean_file = fluxcleanedfiles[0]
            toa_file = fluxcleanedfiles[1]

    # if none is available, go for a regular file (e.g. if we have UHF obs)
    if (clean_file == None):
        if (len(cleanedfiles) == 1):
            clean_file = cleanedfiles[0]
            toa_file = cleanedfiles[0]
        elif (len(cleanedfiles) == 2):
            if (chop_string in cleanedfiles[0]) and (chop_string not in cleanedfiles[1]):
                clean_file = cleanedfiles[1]
                toa_file = cleanedfiles[0]
            elif (chop_string in cleanedfiles[1]) and (chop_string not in cleanedfiles[0]):
                clean_file = cleanedfiles[0]
                toa_file = cleanedfiles[1]

    # ideally we would write the pav images directly to destination, but pav won't use overly long file strings
    # instead create locally and move

    if (cparams['slurm'] == "True"):
        logger.info("SLURM environment recognised")
        env_query = 'echo $JOBFS'
        jobfs_dir = str(subprocess.check_output(env_query, shell=True).decode("utf-8")).rstrip()
        logger.info("JOBFS dir = {}".format(jobfs_dir))
        loc_path = jobfs_dir
    else:
        loc_path = "/tmp"

    logger.info("Loc_path = {}".format(loc_path))

    # create empty array for storing image data
    image_data = []

    if (clean_file != None and toa_file != None):
        # we've got the file we want to analyse, now let's make some pretty pictures

        # skip this portion depending on image reprocessing specifications
        if ((cparams["image_flag"] and cparams["image_type"] == "ALL") or not cparams["image_flag"]):

            # get channel number of cleaned file
            comm = "vap -c nchan {0}".format(clean_file)
            args = shlex.split(comm)
            proc = subprocess.Popen(args,stdout=subprocess.PIPE)
            proc.wait()
            info = proc.stdout.read().decode("utf-8").split("\n")
            nchan = int(info[1].split()[1])

            # basic psrplot images - mimicking ingest images
            plot_commands = [
                {
                    'comm': 'psrplot -p flux -jFTDp -jC',
                    'name': 'profile_ftp',
                    'title': 'Stokes I Profile ({0})'.format(cparams["pid"]),
                    'rank': 1,
                    'type': '{0}.profile-int.hi'.format(local_pid),
                },
                {
                    'comm': 'psrplot -p Scyl -jFTD -jC',
                    'name': 'profile_fts',
                    'title': 'Polarisation Profile ({0})'.format(cparams["pid"]),
                    'rank': 2,
                    'type': '{0}.profile-pol.hi'.format(local_pid),
                },
                {
                    'comm': "psrplot -p freq -jTDp -jC -j 'F {0}'".format(int(nchan/2.0)),
                    'name': 'phase_freq',
                    'title': 'Phase vs. Frequency ({0})'.format(cparams["pid"]),
                    'rank': 4,
                    'type': '{0}.phase-freq.hi'.format(local_pid),
                },
                {
                    'comm': 'psrplot -p time -jFDp -jC',
                    'name': 'phase_time',
                    'title': 'Phase vs. Time ({0})'.format(cparams["pid"]),
                    'rank': 3,
                    'type': '{0}.phase-time.hi'.format(local_pid),
                },
                {
                    'comm': 'psrplot -p b -x -jT -lpol=0,1 -O -c log=1',
                    'name': 'bandpass',
                    'title': 'Cleaned bandpass ({0})'.format(cparams["pid"]),
                    'rank': 8,
                    'type': '{0}.bandpass.hi'.format(local_pid),
                },
            ]

            logger.info("Creating psrsplot images...")
            for plot_command in plot_commands:

                # need to protect against unexpected image crashes
                try:

                    logger.info("Creating image type {0}...".format(plot_command['type']))

                    # create / overwrite the image
                    image_name = "{0}.png".format(plot_command['name'])
                    image_file = os.path.join(images_path,image_name)
                    tmp_image_file = os.path.join(loc_path,image_name)

                    if (os.path.exists(image_file)):
                        os.remove(image_file)
                    # comm = "{0} -g {1}/png {2}".format(plot_commands[x]['comm'], image_name, clean_file)
                    comm = "{0} {2} -g 1024x768 -c above:l= -c above:c='{3}' -D {1}/png".format(plot_command['comm'], tmp_image_file, clean_file, plot_command['title'])
                    args = shlex.split(comm)
                    proc = subprocess.Popen(args,stdout=subprocess.PIPE)
                    proc.wait()

                    # move resulting image
                    try:
                        logger.info("Renaming {0} to {1}...".format(tmp_image_file, image_file))
                        os.rename(tmp_image_file, image_file)
                    except:
                        logger.info("Rename attempt failed - copying {0} to {1} instead...".format(tmp_image_file, image_file))
                        os.system("cp {0} {1}".format(tmp_image_file, image_file))

                    # log results to array for later recording
                    image_data.append({'file': image_file, 'rank': plot_command['rank'], 'type': plot_command['type']})

                except:
                    logger.error("Attempt to create image type {0} failed - skipping...".format(plot_command['type']))

        # skip this portion depending on image reprocessing specifications
        if ((cparams["image_flag"] and cparams["image_type"] == "ALL") or not cparams["image_flag"]):

            # psrstat images - snr/time

            logger.info("Creating S/N images...")

            # make scrunched file for analysis
            comm = "pam -Fp -e Fp.temp -u {0} {1}".format(loc_path, clean_file)
            args = shlex.split(comm)
            proc = subprocess.Popen(args,stdout=subprocess.PIPE)
            proc.wait()
            info = proc.stdout.read().decode("utf-8").rstrip().split()
            scrunched_file = info[0]

            # new - psrchive side functionality
            scrunched_arch = ps.Archive_load(scrunched_file)
            zapped_arch = scrunched_arch.clone()

            # get parameters for looping
            comm = "vap -c nsub,length {0}".format(scrunched_file)
            args = shlex.split(comm)
            proc = subprocess.Popen(args,stdout=subprocess.PIPE)
            proc.wait()
            info = proc.stdout.read().decode("utf-8").rstrip().split("\n")
            nsub = int(info[1].split()[1])
            length = float(info[1].split()[2])

            logger.info("Beginning S/N analysis...")
            #logger.info("NSUB = {0} | LENGTH = {1}".format(nsub, length))

            # collect and write snr data
            snr_data = []
            snr_report = os.path.join(images_path, "snr.dat")

            for x in range(0, nsub):
                # new method - October 2022

                #logger.info("Loop {} starting...".format(x))

                # step 1. work backward through the file zapping out one subint at a time
                asub = nsub - x - 1
                if (x > 0):
                    # don't need to zap anything - analysing the complete archive
                    clean_utils.zero_weight_subint(zapped_arch,asub + 1)

                # step 2. scrunch and write to disk
                tscr_arch = zapped_arch.clone()
                tscr_arch.tscrunch()
                zapped_file = os.path.join(loc_path, "zaptemp.ar")
                tscr_arch.unload(zapped_file)

                #comm = "paz -X '0 {0}' -e paz {1}".format(x, scrunched_file)
                #args = shlex.split(comm)
                #proc = subprocess.Popen(args,stdout=subprocess.PIPE)
                #proc.wait()
                #zapped_file = proc.stdout.read().decode("utf-8").rstrip().split()[1]

                # step 2. fully scrunch the file in time
                #comm = "pam -m -T {0}".format(zapped_file)
                #args = shlex.split(comm)
                #proc = subprocess.Popen(args,stdout=subprocess.PIPE)
                #proc.wait()

                # step 3. extract the cumulative snr via psrstat
                comm = "psrstat -j Fp -c snr=pdmp -c snr {0}".format(zapped_file)
                args = shlex.split(comm)
                proc = subprocess.Popen(args,stdout=subprocess.PIPE)
                proc.wait()
                snr_cumulative = float(proc.stdout.read().decode("utf-8").rstrip().split("=")[1])

                # step 4. extract the single snr via psrstat
                #comm = "psrstat -j Fp -c snr=pdmp -c subint={0} -c snr {1}".format(x, scrunched_file)
                comm = "psrstat -j Fp -c snr=pdmp -c subint={0} -c snr {1}".format(asub, scrunched_file)
                args = shlex.split(comm)
                proc = subprocess.Popen(args,stdout=subprocess.PIPE)
                proc.wait()
                snr_single = float(proc.stdout.read().decode("utf-8").rstrip().split("=")[1])

                # step 5. write to file
                #snr_data.append([length*x/nsub, snr_single, snr_cumulative])
                snr_data.append([length*asub/nsub, snr_single, snr_cumulative])

                # cleanup
                os.remove(zapped_file)
                del(tscr_arch)

                #logger.info("Loop {} ending...".format(x))

            np.savetxt(snr_report, snr_data, header = " Time (seconds) | snr (single) | snr (cumulative)", comments = "#")

            logger.info("Analysis complete.")

            # plot results - single subint snr
            matplot_commands = [
                {
                    'x-axis': np.transpose(snr_data)[0],
                    'y-axis': np.transpose(snr_data)[1],
                    'xlabel': 'Time (seconds)',
                    'ylabel': 'SNR',
                    'title': 'Single subint SNR ({0})'.format(cparams["pid"]),
                    'name': 'SNR_single',
                    'rank': 7,
                    'type': '{0}.snr-single.hi'.format(local_pid),
                },
                {
                    'x-axis': np.transpose(snr_data)[0],
                    'y-axis': np.transpose(snr_data)[2],
                    'xlabel': 'Time (seconds)',
                    'ylabel': 'SNR',
                    'title': 'Cumulative SNR ({0})'.format(cparams["pid"]),
                    'name': 'SNR_cumulative',
                    'rank': 6,
                    'type': '{0}.snr-cumul.hi'.format(local_pid),
                },
            ]

            for matplot_command in matplot_commands:

                logger.info("Creating image type {0}...".format(matplot_command['type']))

                # create the plot
                image_name = "{0}.png".format(matplot_command['name'])
                image_file = os.path.join(images_path,image_name)
                plt.clf()
                plt.plot(  matplot_command['x-axis'], matplot_command['y-axis'])
                plt.xlabel(matplot_command['xlabel'])
                plt.ylabel(matplot_command['ylabel'])
                plt.title( matplot_command['title'])
                plt.savefig(image_file)
                plt.clf()

                # log the plot
                image_data.append({'file': image_file, 'rank': matplot_command['rank'], 'type': matplot_command['type']})

            # cleanup
            os.remove(scrunched_file)

        # skip this portion depending on image reprocessing specifications
        if (cparams["image_flag"] and (cparams["image_type"] == "ALL" or cparams["image_type"] == "TOAs") or not cparams["image_flag"]):

            # generate TOA-based images
            # produce toas - need to recall the template used through the toas table
            logger.info("Obtaining templates and ephemerides for generating TOA images...")
            template_list = glob.glob(os.path.join(str(timing_path),"{0}.std".format(psrname)))
            if (len(template_list) > 0):
                template = template_list[0]
            else:
                template = None

            parfile_list = glob.glob(os.path.join(str(timing_path),"{0}.par".format(psrname)))
            if (len(parfile_list) > 0):
                parfile = parfile_list[0]
            else:
                parfile = None

            selfile_list = glob.glob(os.path.join(str(timing_path),"{0}.select".format(psrname)))
            if (len(selfile_list) > 0):
                selfile = selfile_list[0]
            else:
                selfile = None

            toa_archive_name = "image_toas.ar"
            toa_archive_file = os.path.join(images_path, toa_archive_name)
            single_image_name = "toas_single.png"
            single_image_file = os.path.join(images_path,single_image_name)
            global_image_name = "{0}.{1}_global.png".format(local_pid, psrname)
            global_image_file = os.path.join(images_path,global_image_name)

            # slight hack for central frequency
            path_dir = os.path.normpath(str(output_dir))
            split_path = path_dir.split("/")
            path_args = len(split_path)
            path_freq = str(split_path[path_args - 1])

            logger.info("Path freq = {0}".format(path_freq))

            # only build TOAs for L-band data so far - fix this later!
            if (path_freq == "1284" or path_freq == "1283"):
                if ("global_toa_path" in cparams):
                    if (os.path.exists(cparams["global_toa_path"]) == False):
                        os.makedirs(cparams["global_toa_path"])
                    share_path = cparams["global_toa_path"]
                else:
                    share_path = images_path
                share_file = os.path.join(share_path,global_image_name)

                if (build_image_toas(output_dir, toa_file, toa_archive_name, images_path, cparams, psrname, logger)):
                    logger.info("Successfully created {0} - now producing residual images".format(toa_archive_file))

                    # generate single TOA image
                    if (generate_singleres_image(output_dir, toa_archive_file, single_image_name, images_path, parfile, template, selfile, cparams, psrname, logger)):
                        logger.info("Successfully created single observation residual image {0}".format(single_image_file))
                        image_data.append({'file': single_image_file, 'rank': 5, 'type': '{0}.toa-single.hi'.format(local_pid)})
                    else:
                        logger.error("Single observation residual TOA image generation was unsuccessful!")

                    # generate global TOA image
                    if (generate_globalres_image(output_dir, toa_archive_file, global_image_name, images_path, parfile, template, selfile, cparams, psrname, logger)):
                        logger.info("Successfully created global observation residual image {0}".format(global_image_file))
                        image_data.append({'file': global_image_file, 'rank': 11, 'type': '{0}.toa-global.hi'.format(local_pid)})

                        # copy the global TOA image to the shared path
                        if not (global_image_file == share_file):
                            copyfile(global_image_file, share_file)

                    else:
                        logger.error("Global observation residual TOA image generation was unsuccessful!")

                else:
                    logger.error("Generation of TOA archive was unsuccessful.")

            else:
                logger.info("TOA plot generation only currently enabled for L-Band observations.")

            # now do DM measurement
            logger.info("Initiating single-obs DM measurement (attached to TOA production)...")
            dm_archive_name = "dm_toas.ar"
            dm_archive_file = os.path.join(images_path, dm_archive_name)
            dm_nchan = 16
            dm_result = None

            while (dm_result == None and dm_nchan >= 4 and (dm_nchan % 1 == 0)):

                if (build_dm_toas(output_dir, toa_file, dm_archive_name, images_path, dm_nchan, logger)):

                    # dm archive successfully created
                    # send archive to dm measurement function
                    dm_result = measure_dm(dm_archive_file, images_path, parfile, template, selfile, logger)

                    # output will either be a JSON object or None
                else:
                    logger.error("Generation of {} channel DM archive was unsuccssful - dividing nchan by 2 and trying again.".format(dm_nchan))

                dm_nchan = dm_nchan / 2

            # loop complete - report
            if (dm_result == None):
                logger.info("DM fitting failed.")
            else:
                logger.info("DM fitting succesful.")

            # cleanup
            if (os.path.exists(dm_archive_file)):
                os.remove(dm_archive_file)

            # write results to PSRDB
            if cparams["db_flag"]:

                logger.info("PSRDB functionality activated - recording measured DM.")

                # Create client
                db_client = GraphQLClient(cparams["db_url"], False)

                # recall results field and update
                psrdb_results = get_results(cparams["db_proc_id"], db_client, cparams["db_url"], cparams["db_token"])
                logger.info("Recalled results of processing ID {0}".format(cparams["db_proc_id"]))
                logger.info(psrdb_results)
                psrdb_results['dm'] = dm_result
                update_id = update_processing(
                    cparams["db_proc_id"],
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    psrdb_results,
                    db_client,
                    cparams["db_url"],
                    cparams["db_token"]
                )
                if (update_id != cparams["db_proc_id"]) or (update_id == None):
                    logger.error("Failure to update 'processings' entry ID {0} - PSRDB cleanup may be required.".format(cparams["db_proc_id"]))
                else:
                    logger.info("Updated PSRDB entry in 'processings' table, ID = {0}".format(cparams["db_proc_id"]))

        else:
            logger.error("Could not identify suitable file for image generation.")
            logger.error("Skipping generation of relevant images.")

    # skip this portion depending on image reprocessing specifications
    if ((cparams["image_flag"] and cparams["image_type"] == "ALL") or not cparams["image_flag"]):

        # now link to dynamic spectra images
        ds_path = os.path.join(output_dir,"scintillation")
        logger.info("Adding dynamic spectra images found in {0}...".format(ds_path))

        # look for two fixed dynspec images
        dynspec_commands = [
            {
                'ext': 'zap.dynspec',
                'rank': 9,
                'type': '{0}.zap-dynspec.hi'.format(local_pid),
            },
            {
                'ext': 'calib.dynspec',
                'rank': 10,
                'type': '{0}.calib-dynspec.hi'.format(local_pid),
            }
        ]

        for dynspec_command in dynspec_commands:

            # check/recall image and store image_data
            data = glob.glob(os.path.join(ds_path, "*{0}.png".format(dynspec_command['ext'])))
            if (len(data) == 0):
                logger.error("No matches found in {0} for extension {1}".format(ds_path, dynspec_command['ext']))
            elif (len(data) > 1):
                logger.error("Non-unique match found in {0} for extension {1} - skipping".format(ds_path, dynspec_command['ext']))
            else:
                # unique match found
                logger.info("Unique match found in {0} for extension {1}".format(ds_path, dynspec_command['ext']))

                if (cparams["db_flag"]):

                    # BUG FIX - We now need to check on the file size!
                    max_image_size = 750 # kB
                    dimension_factor = 0.95
                    loop_counter = 0
                    size_check = False

                    logger.info("Checking on file size of {0} to determine if downsampling is needed for PSRDB upload...".format(data[0]))

                    while not (size_check):

                        current_factor = dimension_factor**loop_counter

                        if (loop_counter == 0):
                            # initialise the image
                            og_image = Image.open(data[0])
                            og_sizes = og_image.size
                            data_split = os.path.splitext(data[0])
                            small_image_name = "{0}.small.jpg".format(data_split[0])
                            next_image_name = data[0]
                        else:
                            # make a downsized copy
                            small_image = og_image.convert('RGB')
                            small_image = small_image.resize((round(og_sizes[0]*current_factor), round(og_sizes[1]*current_factor)), Image.ANTIALIAS)
                            small_image.save(small_image_name, optimize=True, quality=95)
                            next_image_name = small_image_name

                        # image to be considered is ready - test file size (in KB)
                        image_size = os.stat(next_image_name).st_size / 1024
                        if (image_size <= max_image_size):
                            size_check = True
                            logger.info("Final image {2} downsampled {0} times ({1}% size of original)".format(loop_counter, current_factor*100, next_image_name))

                        loop_counter += 1

                else:
                    next_image_name = data[0]

                image_data.append({'file': next_image_name, 'rank': dynspec_command['rank'], 'type': dynspec_command['type']})

    # write all images to PSRDB
    if (cparams["db_flag"]):


        logger.info("PSRDB functionality activated - recording pipeline images to PSRDB")

        # set up PSRDB functionality
        db_client = GraphQLClient(cparams["db_url"], False)

        for image_d in image_data:

            # test for image creation success and write to PSRDB
            if (os.path.exists(image_d['file'])):
                logger.info("Successfully created {0} - recording to PSRDB.".format(image_d['file']))
                create_pipelineimage(image_d['file'], image_d['type'], image_d['rank'], cparams, db_client, logger)
            else:
                logger.error("Unable to create {0} - no output recorded to PSRDB.".format(image_d['file']))

    logger.info("Image generation & logging complete.")

    return

# builds the toas used for the measurement of DM specific to this observation
def build_dm_toas(output_dir, clean_file, toa_archive_name, toa_archive_path, nchan, logger):

    logger.info("Build DM Toas function entered...")

    # set up paths and filenames
    toa_archive_ext = "temptoa.ar"
    dlyfix_script = "/fred/oz005/users/mkeith/dlyfix/dlyfix"
    toa_archive_file = os.path.join(toa_archive_path,toa_archive_name)

    # remove the DM archive if it already exists
    if (os.path.exists(toa_archive_file)):
        os.remove(toa_archive_file)

    toa_nchan = int(nchan)

    # verify correct relationship between requested nchan and file channel count
    comm = "vap -c nchan {0}".format(clean_file)
    args = shlex.split(comm)
    proc = subprocess.Popen(args,stdout=subprocess.PIPE)
    proc.wait()
    info = proc.stdout.read().decode("utf-8").rstrip().split("\n")
    ar_nchan = int(info[1].split()[1])

    # reduce channel count until integer relationship is met
    while (toa_nchan > 1) and not (ar_nchan % int(toa_nchan) == 0):
        toa_nchan -= 1

    logger.info("Constructing full time-scrunched DM archive with nchan={0} - storing in {1}".format(toa_nchan, toa_archive_path))
    comm = "pam -Tp --setnchn={0} -e {3} -u {1} {2}".format(toa_nchan, toa_archive_path, clean_file, toa_archive_ext)
    args = shlex.split(comm)
    proc = subprocess.Popen(args,stdout=subprocess.PIPE)
    proc.wait()
    toa_archive_temp = proc.stdout.read().decode("utf-8").rstrip().split()[0]

    # rename the file to desired name
    logger.info("Renaming the archive to {0}".format(toa_archive_file))
    comm = "mv -f {0} {1}".format(toa_archive_temp, toa_archive_file)
    args = shlex.split(comm)
    proc = subprocess.Popen(args,stdout=subprocess.PIPE)
    proc.wait()

    # correct the delays
    logger.info("Applying delay corrections via {0}".format(dlyfix_script))
    comm = "{0} -u {1} {2}".format(dlyfix_script, toa_archive_path, toa_archive_file)
    args = shlex.split(comm)
    proc = subprocess.Popen(args,stdout=subprocess.PIPE)
    proc.wait()

    # check if file creation was successful and return
    return os.path.exists(toa_archive_file)


# create dm toas and return DM measurement
def measure_dm(toa_archive, image_path, parfile, template, selfile, logger):

    logger.info("Beginning DM measurement function...")

    # set up paths, filenames and required parameters
    timfile = os.path.join(image_path, "dm_toas.tim")
    retval = None

    # delete file if it already exists
    if (os.path.exists(timfile)):
        os.remove(timfile)

    if not (template == None) and (os.path.exists(template)):

        comm = "vap -c nchan {0}".format(toa_archive)
        args = shlex.split(comm)
        proc = subprocess.Popen(args,stdout=subprocess.PIPE)
        proc.wait()
        info = proc.stdout.read().decode("utf-8").rstrip().split("\n")
        toa_nchan = int(info[1].split()[1])

        # check PORTRAIT toggle
        portrait_str = get_portrait_toggle(template, toa_nchan, logger)

        # create TOAs
        comm = 'pat -jp {2} -f "tempo2 IPTA" -C "chan rcvr snr length subint" -s {0} -A FDM {1}'.format(template, toa_archive, portrait_str)
        args = shlex.split(comm)
        f = open(timfile, "w")
        subprocess.call(args, stdout=f)
        f.close()
        logger.info("DM TOA data generated and stored in {0}".format(timfile))

        if not (parfile == None) and (os.path.exists(parfile)):

            logger.info("Calling meerwatch_tools utility for measuring DM...")
            retval = get_dm_fromtim(timfile, parfile, sel_file=selfile, out_dir=image_path, verb=True)

    if (retval == None):
        logger.info("Unable to successfully measure DM with provided configuration.")

    # cleanup
    if (os.path.exists(timfile)):
        os.remove(timfile)

    return retval

# produce residual image for a single observation
def generate_singleres_image(output_dir, toa_archive, image_name, image_path, parfile, template, selfile, cparams, psrname, logger):

    # set up paths, filenames and required parameters
    timfile = os.path.join(image_path, "toas_single.tim")
    image_file = os.path.join(image_path, image_name)
    local_pid = cparams["pid"].lower()
    files_to_store = []

    comm = "vap -c nchan,bw,freq {0}".format(toa_archive)
    args = shlex.split(comm)
    proc = subprocess.Popen(args,stdout=subprocess.PIPE)
    proc.wait()
    info = proc.stdout.read().decode("utf-8").rstrip().split("\n")
    toa_nchan = int(info[1].split()[1])
    obs_bw = float(info[1].split()[2])
    obs_freq = float(info[1].split()[3])

    if not (template == None) and (os.path.exists(template)):

        # check PORTRAIT toggle
        portrait_str = get_portrait_toggle(template, toa_nchan, logger)

        # ensure this pat comment closely matches the one in generate_toas()
        comm = 'pat -jp {2} -f "tempo2 IPTA" -C "chan rcvr snr length subint" -s {0} -A FDM {1}'.format(template, toa_archive, portrait_str)
        args = shlex.split(comm)
        f = open(timfile, "w")
        subprocess.call(args, stdout=f)
        f.close()
        logger.info("TOA data generated and stored in {0}".format(timfile))

        if not (parfile == None) and (os.path.exists(parfile)):

            # use meerwatch functions to produce residual images for this observation
            logger.info("Calling modified MeerWatch residual generation...")
            residuals = get_res_fromtim(timfile, parfile, sel_file=selfile, out_dir=image_path, verb=True)
            # check for valid output
            if (len(residuals) > 0):
                logger.info("Producing single-obs image from modified MeerWatch residuals...")
                logger.info("{0} {1} {2}".format(obs_bw, obs_freq, toa_nchan))
                plot_toas_fromarr(residuals, pid=cparams["pid"], out_file=image_file, sequential=True, verb=True, bw=obs_bw, cfrq=obs_freq, nchn=toa_nchan)

                # check if file creation was successful and return
                result = os.path.exists(image_file)

                # new - now preferencing the compressed residual file for upload
                residual_file = os.path.join(os.path.dirname(timfile), os.path.basename(timfile).replace('.tim', '_res_comp.txt'))
                if not os.path.exists(residual_file):
                    residual_file = os.path.join(os.path.dirname(timfile), os.path.basename(timfile).replace('.tim', '_res.txt'))
                residual_type = "{0}.single-res".format(local_pid)
                files_to_store.append({'filename': residual_file, 'type': residual_type})

            else:
                logger.error("Insufficient TOAs to generate single-obs image - skipping...")
                result =  False
        else:
            logger.error("No parfile provided! - Skipping single-obs TOA image generation...")
            result = False

        # package the timfile for storage
        if (os.path.exists(timfile)):
            timzip = "{}.gz".format(timfile)
            comm = "gzip -f -9 {0}".format(timfile)
            args = shlex.split(comm)
            proc = subprocess.Popen(args,stdout=subprocess.PIPE)
            proc.wait()
            if (os.path.exists(timzip)):
                ziptype = "{0}.single-tim".format(local_pid)
                files_to_store.append({'filename': timzip, 'type': ziptype})

    else:
        logger.error("No template provided! - Skipping single-obs TOA image generation...")
        result = False

    # new function - store requested files in PSRDB
    # this is a bit of a hack but I just need it to work
    if (cparams["db_flag"]):

        logger.info("PSRDB functionality activated - recording single-obs TOA files to PSRDB")
        db_client = GraphQLClient(cparams["db_url"], False)

        # loop through the listed files and store
        for entry in files_to_store:

            if (os.path.exists(entry['filename'])):
                logger.info("File {0} identified (type {1}) - recording to PSRDB.".format(entry['filename'], entry['type']))
                create_pipelinefile(entry['filename'], entry['type'], cparams, db_client, logger)
            else:
                logger.error("File {0} not located (type {1}) - no output recorded to PSRDB.".format(entry['filename'], entry['type']))

    return result

# produce residual image for all available observations that have completed processing and which match the project code
# assumes that the TOA files have been dlyfix'd
def generate_globalres_image(output_dir, local_toa_archive, image_name, image_path, parfile, template, selfile, cparams, psrname, logger):

    # ADMISSION OF GUILT: Eventually I want to code this section so that it works with PSRDB properly
    # Unfortunately, this requires modifications to PSRDB that are not yet available, and I don't want
    # to wait any longer. Once PSRDB is fixed, I will re-write this section to get the job done correctly.

    # skeleton code for PSRDB

    # this function will only run if DB mode is active - check
    #if (cparams["db_flag"]):

        # set up paths, filenames and required parameters

        # query for all processings run through a specific pipeline

        # compile a TOA file using only those processings logged as 'complete'

    #else:
        #logger.error("PSRDB mode not active - Global TOA residual image will not be generated.")

    # set up paths, filenames and required parameters
    local_pid = cparams["pid"].lower()
    timfile = os.path.join(image_path, "{0}.{1}_global.tim".format(local_pid, psrname))
    public_timfile = os.path.join(image_path, "{0}.{1}_global_public.tim".format(local_pid, psrname))
    image_file = os.path.join(image_path, image_name)
    files_to_store = []

    # scroll through all available observations under the file heirarchy matching the required parameters
    # if they match, build their TOAs into the file
    # assumes that the output path of the current config file contains all neccessary observations
    toa_str = "images/image_toas.ar"
    toa_archives = sorted(glob.glob(os.path.join(cparams["output_path"],"{0}/{1}/*/*/*/{2}".format(cparams["pid"], psrname, toa_str))))
    # get parameters with reference to the local toa_archive
    comm = "vap -c telescop,bw,freq,mjd,nchan {0}".format(local_toa_archive)
    args = shlex.split(comm)
    proc = subprocess.Popen(args,stdout=subprocess.PIPE)
    proc.wait()
    info = proc.stdout.read().decode("utf-8").rstrip().split("\n")
    telescope = str(info[1].split()[1])
    obs_bw = float(info[1].split()[2])
    obs_freq = float(info[1].split()[3])
    obs_mjd = float(info[1].split()[4])
    obs_nchan = float(info[1].split()[5])

    # begin the scroll
    toa_list = ""
    unembargoed_toa_list = ""
    logger.info("Compiling global TOA lists for pulsar {0} and project {1}...".format(psrname, cparams["pid"]))
    for x in range (0, len(toa_archives)):

        # new - check that the archive still exists, and hasn't been deleted
        attempt_counter = 0
        if (os.path.exists(toa_archives[x]) and attempt_counter < 3):

            try:

                # get comparison parameters
                comm = "vap -c telescop,bw,freq {0}".format(toa_archives[x])
                args = shlex.split(comm)
                proc = subprocess.Popen(args,stdout=subprocess.PIPE)
                proc.wait()
                info = proc.stdout.read().decode("utf-8").rstrip().split("\n")
                telescope_comp = str(info[1].split()[1])
                #obs_bw_comp = float(info[1].split()[2])
                #obs_freq_comp = float(info[1].split()[3])

                # flag setup
                allow_toa = False
                embargoed = False

                #if (telescope_comp == telescope) and (obs_bw_comp == obs_bw) and (obs_freq_comp == obs_freq):
                if (telescope_comp == telescope):
                    # we have a match - set the flag to true and see if anything overrides it
                    allow_toa = True

                    # new PSRDB step - check that the TOAs haven't been flagged as bad yet
                    if (cparams["db_flag"]):

                        logger.info("PSRDB functionality activated - checking TOA table for flagged & embargoed TOAs")
                        db_client = GraphQLClient(cparams["db_url"], False)

                        # step 1 - get processing ID matching the location of the data being checked
                        location = os.path.normpath(toa_archives[x].replace(toa_str, ""))
                        toa_proc_id = get_procid_by_location(location, db_client, cparams["db_url"], cparams["db_token"])

                        # step 2 - get the toa entry matching the processing ID
                        if not (toa_proc_id == None):

                            logger.info("Found unique processing ID {0} matching location {1}".format(toa_proc_id, location))
                            toa_id = get_toa_id(toa_proc_id,  db_client, cparams["db_url"], cparams["db_token"])

                            # step 3 - determine if the toa is nominal
                            if not (toa_id == None):

                                logger.info("Found unique TOA ID {0} matching processing ID {1}".format(toa_id, toa_proc_id))

                                # checking quality
                                if (check_toa_nominal(toa_id,  db_client, cparams["db_url"], cparams["db_token"])):
                                    logger.info("TOA quality is NOMINAL.")
                                    allow_toa = True
                                else:
                                    logger.info("TOA quality is not NOMINAL.")
                                    allow_toa = False

                            else:

                                logger.info("Could not find unique TOA ID matching processing ID {0}".format(toa_proc_id))
                                logger.info("Allowing TOAs by default...")

                            # step 4 - check for embargo
                            logger.info("Checking embargo dates for processing {0}".format(toa_proc_id))
                            embargo_date = get_proc_embargo(toa_proc_id, db_client, cparams["db_url"], cparams["db_token"])
                            todays_date = datetime.datetime.now(datetime.timezone.utc)

                            if ((todays_date - embargo_date).total_seconds() > 0):
                                logger.info("TOA is not embargoed.")
                                embargoed = False
                            else:
                                logger.info("TOA is embargoed.")
                                embargoed = True

                        else:
                            logger.info("Could not find unique processing ID matching location {0}".format(location))
                            logger.info("Allowing TOAs by default...")


                if (allow_toa):
                    logger.info("{0} added to global TOA list".format(toa_archives[x]))
                    toa_list = "{0} {1}".format(toa_list, toa_archives[x])

                    # check for embargo
                    if not (embargoed):
                        logger.info("{0} added to unembargoed TOA list".format(toa_archives[x]))
                        unembargoed_toa_list = "{0} {1}".format(unembargoed_toa_list, toa_archives[x])
                    else:
                        logger.info("{0} excluded from unembargoed TOA list".format(toa_archives[x]))

                else:
                    # no match
                    logger.info("{0} excluded from global TOA list".format(toa_archives[x]))

            except:
                logger.error("Encountered problem trying to query {0} - trying again (Attempt #{1})".format(toa_archives[x], attempt_counter))
                attempt_counter += 1

        else:
            logger.info("Unable to locate {} - skipping...".format(toa_archives[x]))

    # toa list generation complete - build TOAs
    logger.info("TOA list generation complete.")

    # prepare TOA and residual lists for generation
    command_list = [
        {'tim': timfile, 'toas': toa_list, 'image': image_file, 'embargo': False, 'type': 'global'},
        {'tim': public_timfile, 'toas': unembargoed_toa_list, 'image': None, 'embargo': True, 'type': 'global-pub'}
    ]

    if not (template == None) and (os.path.exists(template)):

        for x in range (0, len(command_list)):
            logger.info("Embargo setting = {0}".format(command_list[x]['embargo']))

            # check PORTRAIT toggle
            portrait_str = get_portrait_toggle(template, obs_nchan, logger)

            # ensure this pat comment closely matches the one in generate_toas()
            comm = 'pat -jp {2} -f "tempo2 IPTA" -C "chan rcvr snr length subint" -s {0} -A FDM {1}'.format(template, command_list[x]['toas'], portrait_str)
            args = shlex.split(comm)
            f = open(command_list[x]['tim'], "w")
            subprocess.call(args, stdout=f)
            f.close()
            logger.info("Global TOA data generated and stored in {0}".format(command_list[x]['tim']))

            if not (parfile == None) and (os.path.exists(parfile)):

                # use meerwatch functions to produce residual images for this observation
                logger.info("Calling modified MeerWatch residual generation...")
                residuals = get_res_fromtim(command_list[x]['tim'], parfile, sel_file=selfile, out_dir=image_path, verb=True)
                if (len(residuals) > 0):
                    if not (command_list[x]['image'] == None):
                        logger.info("Producing global TOA image from modified MeerWatch residuals...")
                        plot_toas_fromarr(residuals, pid=cparams["pid"], mjd=obs_mjd, out_file=command_list[x]['image'], sequential=False, verb=True, bw=obs_bw, cfrq=obs_freq, outlier_factor=4.0)

                        # check if file creation was successful and return
                        result = os.path.exists(command_list[x]['image'])

                    # new - now preferencing the compressed residual file for upload
                    residual_file = os.path.join(os.path.dirname(command_list[x]['tim']), os.path.basename(command_list[x]['tim']).replace('.tim', '_res_comp.txt'))
                    if not os.path.exists(residual_file):
                        residual_file = os.path.join(os.path.dirname(command_list[x]['tim']), os.path.basename(command_list[x]['tim']).replace('.tim', '_res.txt'))
                    residual_type = "{0}.{1}-res".format(local_pid, command_list[x]['type'])
                    files_to_store.append({'filename': residual_file, 'type': residual_type})

                else:
                    logger.error("Insufficient TOAs to generate global-obs image - skipping...")
                    result = False
            else:
                logger.error("No parfile provided! - Skipping global-obs TOA image generation...")
                result = False

            # package the timfile for storage
            if (os.path.exists(command_list[x]['tim'])):
                timzip = "{}.gz".format(command_list[x]['tim'])
                comm = "gzip -f -9 {0}".format(command_list[x]['tim'])
                args = shlex.split(comm)
                proc = subprocess.Popen(args,stdout=subprocess.PIPE)
                proc.wait()
                if (os.path.exists(timzip)):
                    ziptype = "{0}.{1}-tim".format(local_pid,command_list[x]['type'])
                    files_to_store.append({'filename': timzip, 'type': ziptype})

    else:
        logger.error("No template provided! - Skipping global-obs TOA image generation...")
        result = False

    # new function - store requested file in PSRDB
    # this is a bit of a hack but I just need it to work
    if (cparams["db_flag"]):

        logger.info("PSRDB functionality activated - recording global-obs TOA files to PSRDB")
        db_client = GraphQLClient(cparams["db_url"], False)

        # loop through the listed files and store
        for entry in files_to_store:

            if (os.path.exists(entry['filename'])):
                logger.info("File {0} identified (type {1}) - recording to PSRDB.".format(entry['filename'], entry['type']))
                create_pipelinefile(entry['filename'], entry['type'], cparams, db_client, logger)
            else:
                logger.error("File {0} not located (type {1}) - no output recorded to PSRDB.".format(entry['filename'], entry['type']))

    return result

# echos back a folding entry to PSRDB so as to trigger a resync of the online DB instance
def folding_resync(cparams,logger):

    logger.info("Echoing an update of folding ID {0} to initiate PSRDB online synchronisation...".format(cparams['db_fold_id']))

    # create client
    db_client = GraphQLClient(cparams["db_url"], False)

    update_id = update_folding(
        cparams['db_fold_id'],
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        db_client,
        cparams["db_url"],
        cparams["db_token"]
    )

    if (update_id != cparams["db_fold_id"]) or (update_id == None):
        logger.error("Failure to update 'foldings' entry ID {0} - PSRDB cleanup may be required.".format(cparams["db_fold_id"]))
    else:
        logger.info("Updated PSRDB entry in 'foldings' table, ID = {0}".format(cparams["db_fold_id"]))

    return