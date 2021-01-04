#!/usr/bin/env python

#Import basics
import numpy as np
import psrchive as ps
import os
import glob
import sys
import matplotlib
import matplotlib.pyplot as plt
import argparse
import shutil
import shlex
import subprocess



#Importing pulse portraiture stuff
import ppalign as ppa  # Import ppalign.py
import ppspline as pps  # Import ppspline.py
import pptoas as ppt  # Import pptoas.py
from pplib import write_TOAs  # and the function to write TOAs from pplib.py

#Importing meerpipe functions
from initialize import parse_config, get_outputinfo
from wideband_utils import setup_logging

#Argument parsing
parser = argparse.ArgumentParser(description="Create wideband templates using PP (@tpennucci)")
parser.add_argument("-dirname", dest="dirname", help="Process a specified observation",required=True)
parser.add_argument("-toas", dest="timing", help="Enable wideband ToA generattion", action="store_true")
parser.add_argument("-overwrite", dest="overwrite", help="Overwrite existing results", action="store_true")
parser.add_argument("-verbose", dest="verbose", help="Enable verbose terminal logging",action="store_true")
args = parser.parse_args()

#input_path = "/fred/oz005/timing_processed"
#input_path = "/fred/oz005/users/aparthas/Meertime_Jitter/MeerPipe_SelectedPSRs"
input_path = "/fred/oz005/users/aparthas/reprocessing_MK/PTA/"
#output_path = "/fred/oz005/users/aparthas/Meertime_Jitter/Jitter_top40"

"""
#Creating the wideband timing directory and setting up the logger
wb_path = os.path.join(input_path,"Wideband_timing")
print wb_path
if not os.path.exists(wb_path):
    os.makedirs(wb_path)
    print ("Wideband timing directory created")
else:
    print ("Wideband timing directory exists")


pulsar_wbdir = os.path.join(output_path,"{0}/wideband".format(str(args.dirname)))
if args.overwrite:
    if os.path.exists(pulsar_wbdir):
        shutil.rmtree(pulsar_wbdir)
        print "Overwritten: {0}".format(pulsar_wbdir)

if not os.path.exists(pulsar_wbdir):
    os.makedirs(pulsar_wbdir)
    print ("Pulsar wideband directory for {0} created".format(str(args.dirname)))
else:
    print ("Pulsar wideband directory for {0} exists".format(str(args.dirname)))
"""


#Required arrays of archive files
align_archives = []
toa_archives = [] 
forcorr_archives=[]

#Input pulsar directory - collecting info for wideband timing
psrname = str(args.dirname)
print("Wideband timing for {0}".format(psrname))
pulsar_dir = os.path.join(input_path,psrname)
print("Input directory:{0}".format(pulsar_dir))

obs_list = sorted(glob.glob(os.path.join(pulsar_dir,"2*")))
for obs in obs_list:
    path,utc_name = os.path.split(obs)
    beams = sorted(glob.glob(os.path.join(obs,"*")))
    for beam in beams:
        freqs = sorted(glob.glob(os.path.join(beam,"*")))
        for freq in freqs:
            decimated_dir = os.path.join(freq,"decimated")
            archive = glob.glob(os.path.join(decimated_dir,"*zappT.ar"))[0]
            toa_archive = glob.glob(os.path.join(decimated_dir,"*32ch8s.ar"))[0]
            forcorr_archive = glob.glob(os.path.join(decimated_dir,"*zapp.ar"))[0]
            align_archives.append(archive)
            toa_archives.append(toa_archive)
            forcorr_archives.append(forcorr_archive)
            

print("Align archives:{0}".format(align_archives))
print("ToA archives:{0}".format(toa_archives))
print("For freq correlation(forcorr) archives:{0}".format(forcorr_archives))

pulsar_wbdir_com = os.path.join(input_path,"{0}/wideband".format(psrname))
if not os.path.exists(pulsar_wbdir_com):
    os.makedirs(pulsar_wbdir_com)
    logger = setup_logging(pulsar_wbdir_com,args.verbose,True)
else:
    logger = setup_logging(pulsar_wbdir_com,args.verbose,True)


#Writing out the align_archives and toa_archives as metafiles
if not os.path.exists(os.path.join(pulsar_wbdir_com,"align_meta")):
    with open(os.path.join(pulsar_wbdir_com,"align.meta"),"w") as f:
        for item in align_archives:
            ar = ps.Archive_load(item)
            if ar.get_bandwidth() == 775.75:
                dummy = item
                f.write("{0}\n".format(item))
    f.close()
align_meta = os.path.join(pulsar_wbdir_com,"align.meta")

if not os.path.exists(os.path.join(pulsar_wbdir_com,"toa.meta")):
    with open(os.path.join(pulsar_wbdir_com,"toa.meta"),"w") as f:
        for item in toa_archives:
            ar = ps.Archive_load(item)
            if ar.get_bandwidth() == 775.75:
                f.write("{0}\n".format(item))
    f.close()
toa_meta = os.path.join(pulsar_wbdir_com,"toa.meta")

if not os.path.exists(os.path.join(pulsar_wbdir_com,"forcorr.meta")):
    with open(os.path.join(pulsar_wbdir_com,"forcorr.meta"),"w") as f:
        for item in forcorr_archives:
            ar = ps.Archive_load(item)
            if ar.get_bandwidth() == 775.75:
                f.write("{0}\n".format(item))
    f.close()
twod_meta = os.path.join(pulsar_wbdir_com,"forcorr.meta")

#Creating a constant profile by using a dummy archive and a 1D template
#1d template

constant_archive = os.path.join(pulsar_wbdir_com,"{0}_constant.prof".format(psrname)) #Desired output file - aligned constant archive
if not os.path.exists(constant_archive):
    oned_temp = os.path.join(input_path,"meertime_templates/{0}.std".format(psrname))

    logger.info("Loading 1D template")
    arch = ps.Archive_load(oned_temp) #Loading the 1d template
    arch.pscrunch()
    arch.fscrunch()
    arch.tscrunch()
    profile = arch.get_data()[0,0,0]

    #Creating a constant archive
    arch = ps.Archive_load(dummy) #Loading the dummy archive
    arch.dededisperse()
    arch.set_dispersion_measure(0.0)
    for subint in arch:
        for ipol in xrange(arch.get_npol()):
            for ichan in xrange(arch.get_nchan()):
                subint.set_weight(ichan, 1.0)
                arch_prof = subint.get_Profile(ipol, ichan)
                arch_prof.get_amps()[:] = profile
                
    logger.info("Constant archive for 2D template generated")
    arch.unload(constant_archive)

#Aligning archives using pp align
outfile_archive = os.path.join(pulsar_wbdir_com, "{0}_aligned.ar".format(psrname))
if not os.path.exists(outfile_archive):
    logger.info("Aligning archives")
    ppa.align_archives(metafile=align_meta, initial_guess=constant_archive,
                       tscrunch=True, pscrunch=True, outfile=outfile_archive, 
                       niter=4, norm="rms")

spline_output = os.path.join(pulsar_wbdir_com,"{0}.spline".format(psrname))
if not os.path.exists(spline_output):
    logger.info("Setting modelling parameters for PCA decomposition")
    max_ncomp = None #Set to None if the number of :eigenprofiles are not known apriori
    smooth = True
    snr_cutoff = 150.0 #This is the default value. Used for filter eigenprofiles
    rchi2_tol = 0.1  #default value
    k = 3 #Default. Polynomial degree of the B-splines
    sfac = 1.0 #Default - fudge factor for smoothing the splines

    #Creating the wideband template (using PP)
    logger.info("Making the spline model")
    dp = pps.DataPortrait(outfile_archive)
    dp.normalize_portrait("prof")
    dp.make_spline_model(max_ncomp=max_ncomp, smooth=smooth, 
                          snr_cutoff=snr_cutoff, rchi2_tol=rchi2_tol, 
                          k=k, sfac=sfac, max_nbreak=None,
                          model_name=None, quiet=False)

    #plotting for test

    dp.show_eigenprofiles()
    dp.show_spline_curve_projections()
    
    logger.info("Saving the spline model")
    dp.write_model(os.path.join(pulsar_wbdir_com,"{0}.spline".format(psrname)))
    logger.info("Saving the WB template")
    dp.write_model_archive(os.path.join(pulsar_wbdir_com,"{0}.portrait".format(psrname)))
    logger.info("Dedispersing the portrait using pam -D and scrunching to 32 channels")
    pam_D = 'pam --setnchn=32 -D {0}/{1}.portrait -e 32ch.portrait'.format(pulsar_wbdir_com,psrname)
    args_pamD = shlex.split(pam_D)
    proc_pamD = subprocess.Popen(args_pamD)
    proc_pamD.wait()


#Reading the list of valid ToA paths
if os.path.exists(toa_meta):
    with open(toa_meta) as file:
        paths=file.readlines()
    file.close()

for num,path in enumerate(paths):
    a,b = os.path.split(path)
    freq,d = os.path.split(a)
    e,f = os.path.split(freq)
    g,h = os.path.split(e)
    i,utc_name = os.path.split(g)

    #Creating the wideband directories
    pulsar_wbdir = os.path.join(freq,"wideband")
    if not os.path.exists(pulsar_wbdir):
        os.makedirs(pulsar_wbdir)
        logger.info("Pulsar wbdir created:{0}".format(pulsar_wbdir))
    else:
        logger = setup_logging(pulsar_wbdir,args.verbose,True)
        logger.info("Pulsar wbdir exists:{0}".format(pulsar_wbdir))


    logger.info("Processing {0}".format(pulsar_wbdir))

    #Creating WB ToAs using pat
    tim_name = "{0}_{1}_32ch8s.tim".format(psrname,utc_name)
    if not os.path.exists(os.path.join(pulsar_wbdir,tim_name)):
        logger.info("Creating ToAs from the WB template for {0}_{1}".format(psrname,utc_name))
        WB_temp = os.path.join(pulsar_wbdir_com,"{0}.32ch.portrait".format(psrname))
        pat_WB = 'pat -P -f "tempo2 IPTA" -C "chan rcvr snr length subint" -s {0} -A FDM {1}'.format(WB_temp,path)
        args_patWB = shlex.split(pat_WB)
        f = open("{0}/{1}".format(pulsar_wbdir,tim_name),"w")
        proc_patWB = subprocess.Popen(args_patWB, stdout=f)
        proc_patWB.wait()
        f.close()

    if args.timing:
        tim_name = "{0}_{1}_wb.tim".format(psrname,utc_name)
        if not os.path.exists(os.path.join(pulsar_wbdir,tim_name)):
            logger.info("Generating wideband ToAs for {0}_{1}".format(psrname,utc_name))
            spline_model = os.path.join(pulsar_wbdir_com,"{0}.spline".format(psrname))
            with open (os.path.join(pulsar_wbdir,"temp"),"w") as tmp:
                tmp.write("{0}".format(path))
            tmp.close()
            tmp_file = os.path.join(pulsar_wbdir,"temp")
            gt = ppt.GetTOAs(tmp_file,spline_model)
            gt.get_TOAs()

            logger.info("Saving the wideband ToAs for {0}_{1}".format(psrname,utc_name))
            wb_tim = os.path.join(pulsar_wbdir,tim_name)
            with open(wb_tim,"w") as f1:
                f1.write("FORMAT 1 \n")
            f1.close()
            write_TOAs(gt.TOA_list, SNR_cutoff=0.0, outfile=wb_tim, append=True)
            os.remove(tmp_file)


##################################################################################################################
"""
#Creating a 2D template from full freq resolution profies - not a portrait
if not os.path.exists(os.path.join(pulsar_wbdir,"{0}.2D".format(psrname))):
    #Create the 2D template (using psradd and psrsmooth)
    logger.info("Creating the 2D template")
    logger.info("Adding *.zappT.ar files")
    psradd = 'psradd -T -o {0}/{1}.2D -M {2}'.format(pulsar_wbdir,psrname,align_meta)
    args_psradd = shlex.split(psradd)
    proc_psradd = subprocess.Popen(args_psradd)
    proc_psradd.wait()
    logger.info("Dedispersing the 2D template using pam -D")
    pam_D = 'pam --setnchn=32 -D {0}/{1}.2D -e 32ch.2D'.format(pulsar_wbdir,psrname)
    args_pamD = shlex.split(pam_D)
    proc_pamD = subprocess.Popen(args_pamD)
    proc_pamD.wait()
    logger.info("Smoothing the 2D template using psrsmooth")
    psrsmooth = 'psrsmooth -W {0}/{1}.2D'.format(pulsar_wbdir,psrname)
    args_psrsmooth = shlex.split(psrsmooth)
    proc_psrsmooth = subprocess.Popen(args_psrsmooth)
    proc_psrsmooth.wait()
    logger.info("Smoothing the 32channel 2D template using psrsmooth")
    psrsmooth = 'psrsmooth -W {0}/{1}.32ch.2D'.format(pulsar_wbdir,psrname)
    args_psrsmooth = shlex.split(psrsmooth)
    proc_psrsmooth = subprocess.Popen(args_psrsmooth)
    proc_psrsmooth.wait()
 

#Creating 2D ToAs using pat
tim_name = "{0}_32ch8s.Global2D.tim".format(psrname)
if not os.path.exists(os.path.join(pulsar_wbdir,tim_name)):
    logger.info("Creating ToAs from the 2D template")
    smoothed_2D_temp = os.path.join(pulsar_wbdir,"{0}.32ch.2D.sm".format(psrname))
    pat_2D = 'pat -P -f "tempo2 IPTA" -C "chan rcvr snr length subint" -s {0} -M {1} -A FDM'.format(smoothed_2D_temp,toa_meta)
    args_pat2D = shlex.split(pat_2D)
    f = open("{0}/{1}".format(pulsar_wbdir,tim_name),"w")
    proc_pat2D = subprocess.Popen(args_pat2D, stdout=f)
    proc_pat2D.wait()
    f.close()
#Creating 1D ToAs using pat
tim_name = "{0}_32ch8s.Global1D.tim".format(psrname)
if not os.path.exists(os.path.join(pulsar_wbdir,tim_name)):
    logger.info("Creating ToAs from the 1D template")
    pat_1D = 'pat -f "tempo2 IPTA" -C "chan rcvr snr length subint" -s {0} -M {1} -A FDM'.format(oned_temp,toa_meta)
    args_pat1D = shlex.split(pat_1D)
    f = open("{0}/{1}".format(pulsar_wbdir,tim_name),"w")
    proc_pat1D = subprocess.Popen(args_pat1D, stdout=f)
    proc_pat1D.wait()
    f.close()
"""







