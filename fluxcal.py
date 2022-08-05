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

import getopt

# WARNING - This next instruction is horrible code and I don't condone ever doing it again
# Unfortunately, my hand is forced by a lack of other apparent options
# Andrew Cameron - 10/02/2022

sys.path.insert(1, sys.path[len(sys.path)-1])
import astropy

# end horrible code

from astropy.io import fits
from astropy.coordinates import (SkyCoord, Longitude, Latitude)

#=============================================================================

#Get basic info required for the radiometer equation
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

def get_info(archive):
    """
    Get Tobs, nbin, bandwidth
    """
    #print ("Obtaining info for {0}".format(archive))
    info = 'psrstat -c length,nbin,bw,nchan {0} -jpD -Q'.format(archive)
    arg = shlex.split(info)
    proc = subprocess.Popen(arg,stdout=subprocess.PIPE)
    info = str(proc.stdout.readline().decode("utf-8")).split()
    return info 

def get_freqlist(archive):
    """
    Get a list of frequencies 
    """
    print ("Getting frequency list..")
    info = 'psrstat -c int:freq,nchan {0} -jTD -Q'.format(archive)
    arg = shlex.split(info)
    proc = subprocess.Popen(arg,stdout=subprocess.PIPE)
    info = str(proc.stdout.readline().decode("utf-8")).split()
    return info



#=============================================================================

#Stuff for Tsky
def get_glgb(psrname):
    "Get GL and GB from psrname"
    
    info = 'psrcat -c "GL GB" {0} -all -X'.format(psrname)
    arg = shlex.split(info)
    proc = subprocess.Popen(arg,stdout=subprocess.PIPE)
    info = proc.stdout.readline().split()
    gl = float(info[0])
    gb = float(info[1])
    
    return gl,gb


def get_radec(psrname):
    "Get RAJD and DECJD (in degrees) from psrname"
    
    info = 'psrcat -c "rajd decjd" {0} -all -X -x -o short'.format(psrname)
    arg = shlex.split(info)
    proc = subprocess.Popen(arg,stdout=subprocess.PIPE)
    info = proc.stdout.readline().rstrip().split()
    try:
        rajd = float(info[0])
        decjd = float(info[1])
        print ("RAJD:{0}, DECJD:{1}".format(str(info[0].decode("utf-8")), str(info[1].decode("utf-8"))))
    except ValueError:
        raise(RuntimeError("Cannot convert values {} and {} to floats".format(str(info[0].decode("utf-8")), str(info[1].decode("utf-8")))))
                       
    return rajd, decjd


def get_radec_new(parfile):
    "Get RAJD and DECJD (in degrees) from the par file"
    all_args = "grep {{}} {}".format(parfile)

    # try grabbing RAJ and DECJ directly first
    ra_args = shlex.split(all_args.format("RAJ"))
    proc1 = subprocess.Popen(ra_args, stdout=subprocess.PIPE)
    inb_str = str(proc1.communicate()[0].decode("utf-8")).strip()
    if inb_str != "":
        ra_str = inb_str.split()[1]
        dec_args = shlex.split(all_args.format("DECJ"))
        proc2 = subprocess.Popen(dec_args, stdout=subprocess.PIPE)
        dec_str = str(proc2.communicate()[0].decode("utf-8")).strip().split()[1]
        pos = SkyCoord(Longitude(ra_str, unit='hourangle'),
                       Latitude(dec_str, unit='deg'))
        rajd = pos.ra.to('deg').value
        decjd = pos.dec.to('deg').value

    else: # coords in par file are not RA and Dec
        elong_args = shlex.split(all_args.format("ELONG"))
        proc1 = subprocess.Popen(elong_args, stdout=subprocess.PIPE)
        info = proc1.communicate()[0].decode("utf-8")
        if len(info.split('\n')) > 1:
            for line in info.split('\n'):
                if line.split()[0] == "ELONG":
                    elong_val = float(line.split()[1])
                    break
        elif info != "":
            elong_val = float(info.split()[1])
        else:
            print("Par file contains neither RAJ nor ELONG")
            return(None, None)

        elat_args = shlex.split(all_args.format("ELAT"))
        proc2 = subprocess.Popen(elat_args, stdout=subprocess.PIPE)
        info = proc2.communicate()[0].decode("utf-8")
        if len(info.split('\n')) > 1:
            for line in info.split('\n'):
                if line.split()[0] == "ELAT":
                    elat_val =float(line.split()[1])
                    break
        elif info != "":
            elat_val = float(info.split()[1])

        # convert ecliptic to J2000
        pos = SkyCoord(elong_val, elat_val, unit='deg', frame='geocentricmeanecliptic')
        pos = pos.transform_to('icrs')
        rajd = pos.ra.to('deg').value
        decjd = pos.dec.to('deg').value

    print("RA and Dec from par file: {} {}".format(rajd, decjd))
    return(rajd, decjd)


def get_tsky_updated(rajd, decjd):
    "Get Tsky from Simon's code. Input arguments are RAJD and DECJD"
    "Convert Tsky to Jy and subtact 3372mK as per SARAO specs"

    #TSKY Default (mK)
    tsky_default = 3400.0
    
    #open the fits file and get the data
    # note that (I think) the data cover the entire 0-360 in gl and -90-90 in gb
    # but that the pixels not covered by the survey are set to nan.
    # WARNING: The survey only goes to +25 declination
    # WARNING: The Galactic centre pixels are blanked out, I'm looking into it.
    #CHIPASS_PATH = "/fred/oz005/meerpipe/configuration_files/additional_info" - TEMP SWITCH FOR LOCAL TESTING - ADC
    CHIPASS_PATH = "/fred/oz005/users/acameron/pipeline_stuff/andrew_meerpipe_dev/meerpipe/configuration_files/additional_info"
    hdul = fits.open(os.path.join(CHIPASS_PATH,'CHIPASS_Equ.fits'))
    data = hdul[0].data


    # naxis1 is the number of pixels on axis1
    # crval1 is the longitude of the crpix1 pixel
    # cdelt1 is increment per pixel
    # ditto for axis2
    # note that crval1,crval2 = 0 in this file
    naxis1 = hdul[0].header['NAXIS1']
    crpix1 = hdul[0].header['CRPIX1']
    cdelt1 = hdul[0].header['CDELT1']
    crval1 = hdul[0].header['CRVAL1']
    naxis2 = hdul[0].header['NAXIS2']
    crpix2 = hdul[0].header['CRPIX2']
    cdelt2 = hdul[0].header['CDELT2']
    crval2 = hdul[0].header['CRVAL2']

    # this is the pixel for the gl,gb
    pix1 = (rajd-crval1)/cdelt1 + crpix1
    pix2 = (decjd-crval2)/cdelt2 + crpix2        

    # convert to integer
    ipix1 = np.int(pix1+0.5)
    ipix2 = np.int(pix2+0.5)

    print('Pixel1: {0},Pixel2: {1}'.format(ipix1,ipix2))

    # none of these should ever really happen
    use_default_tsky = False
    if ipix1 < 0:
        print('ERROR:, x-pixel < 0! Using default tsky: {0}'.format(tsky_default))
        use_default_tsky = True
    if ipix1 > naxis1:
        print('ERROR:, x-pixel > npix! Using default tsky: {0}'.format(tsky_default))
        use_default_tsky = True
    if ipix2 < 0:
        print('ERROR:, y-pixel < 0! Using default tsky: {0}'.format(tsky_default))
        use_default_tsky = True
    if ipix2 > naxis2:
        print('ERROR:, y-pixel > npix! Using default tsky: {0}'.format(tsky_default))
        use_default_tsky = True

    # get tsky for the appropriate pixel
    if use_default_tsky == True:
        tsky = tsky_default
    else:
        tsky = data[ipix2,ipix1]

    # check that gl,gb is covered by the survey
    if np.isnan(tsky):
        print('ERROR:, Pixel blanked! Using default tsky: {0}'.format(tsky_default))
        tsky = tsky_default
        
    print ('### Sky Temperature(mK) used for flux calibration: {0} ###'.format(tsky))
        
    #Converting to Jy and subtracting 3372mK as per SARAO specifications
    print ("Converting tsky (mK) to Jy and subtracting 3372mK (SARAO specs)")
    tsky_jy = (tsky-3372.0)*0.019
    print ("### Tsky in Jy: {0} ###".format(tsky_jy))
    
    return tsky_jy



def get_tsky(gl,gb):
    "Get Tsky from Simon's code. Input arguments are GL and GB"
    "Convert Tsky to Jy and subtact 3372mK as per SARAO specs"
    
    if gl > 180.0:
        gl = gl-360.0
        print ("GL: {0},GB: {1}".format(gl,gb))
        
    #open the fits file and get the data
    # note that (I think) the data cover the entire 0-360 in gl and -90-90 in gb
    # but that the pixels not covered by the survey are set to nan.
    # WARNING: The survey only goes to +25 declination
    # WARNING: The Galactic centre pixels are blanked out, I'm looking into it.
    #CHIPASS_PATH = "/fred/oz005/meerpipe/configuration_files/additional_info" - TEMP SWITCH FOR LOCAL TESTING - ADC
    CHIPASS_PATH = "/fred/oz005/users/acameron/pipeline_stuff/andrew_meerpipe_dev/meerpipe/configuration_files/additional_info"
    hdul = fits.open(os.path.join(CHIPASS_PATH,'CHIPASS_Gal.fits'))
    data = hdul[0].data


    # naxis1 is the number of pixels on axis1
    # crval1 is the longitude of the crpix1 pixel
    # cdelt1 is increment per pixel
    # ditto for axis2
    # note that crval1,crval2 = 0 in this file
    naxis1 = hdul[0].header['NAXIS1']
    crpix1 = hdul[0].header['CRPIX1']
    cdelt1 = hdul[0].header['CDELT1']
    crval1 = hdul[0].header['CRVAL1']
    naxis2 = hdul[0].header['NAXIS2']
    crpix2 = hdul[0].header['CRPIX2']
    cdelt2 = hdul[0].header['CDELT2']
    crval2 = hdul[0].header['CRVAL2']

    # this is the pixel for the gl,gb
    pix1 = (gl-crval1)/cdelt1 + crpix1
    pix2 = (gb-crval2)/cdelt2 + crpix2        

    # convert to integer
    ipix1 = np.int(pix1+0.5)
    ipix2 = np.int(pix2+0.5)

    print('Pixel1: {0},Pixel2: {1}'.format(ipix1,ipix2))

    # none of these should ever really happen
    if ipix1 < 0:
        print('ERROR:, location is not in the image !!')
    if ipix1 > naxis1:
        print('ERROR:, location is not in the image !!')
    if ipix2 < 0:
        print('ERROR:, location is not in the image !!')
    if ipix2 > naxis2:
        print('ERROR:, location is not in the image !!')

    # get tsky for the appropriate pixel
    tsky = data[ipix2,ipix1]

    # check that gl,gb is covered by the survey
    if np.isnan(tsky):
        print('Undefined tsky, using default of 3.0')
        tsky = 3.0
    else:
        print ('Sky Temperature(mK): {0}'.format(tsky))
        
    #Converting to Jy and subtracting 3372mK as per SARAO specifications
    print ("Converting tsky to Jy and subtracting 3372mK (SARAO specs)")
    tsky_jy = (tsky-3372.0)*0.019
    print ("Tsky in Jy: {0}".format(tsky_jy))
    
    return tsky_jy


#=============================================================================

#Compute observed RMS, multiplier and flux calibrate the data
def get_Ssys(tsky_jy,Nant):
    "Return Ssys at 1390 MHz"
    SEFD_1390 = 390.0 #SEFD at 1390 MHz = 390 Jy (one dish)
    Ssys_1390 = (SEFD_1390+tsky_jy)/Nant
    print ("Ssys at 1390 MHz: {0}".format(Ssys_1390))
    print ("Number of antennae: {0}".format(Nant))
    return Ssys_1390


def get_expectedRMS(info,ssys):
    "Compute the exepected RMS"
    
    tobs = np.float(info[1]) #Length of the observation
    nbin = np.float(info[2]) #Number of phase bins
    bw = np.float(info[3]) #Observing bandwidth
    nchan = np.float(info[4]) #Number of frequency channels
    
    denom = np.sqrt(2*bw/nchan * tobs/nbin)
    rms = ssys/denom
    
    print ("Expected RMS: {0}".format(rms))
    channel_bw = bw/nchan
    print ("Tobs: {0}, nbin: {1}, nchan: {2}, Obs.BW: {3}, channelBW: {4}".format(tobs,nbin,nchan,bw,channel_bw))
    
    return rms

def get_offrms(archive):
    """
    Compute the offpulse rms for a profile at a particular frequency channel
    """
    print ("Computing off-pulse rms..")
    info = 'psrstat -c off:rms -l chan=0: -jTDp -Q {0}'.format(archive)
    arg = shlex.split(info)
    proc = subprocess.Popen(arg,stdout=subprocess.PIPE)
    
    lines = proc.stdout.readlines()
    offpulse_rms_list = []
    for line in lines:
        sline = str(line.decode("utf-8")).split(" ")
        offpulse_rms_list.append(float(sline[-1].rstrip()))

    return offpulse_rms_list


def get_median_offrms(offrms_freq_dictionary):
    "Select channels centered at 1390 MHz and compute the median of their off-pulse rms values"
    
    print ("Computing median of off-pulse rms values of channels centered at 1390 MHz..")
    selected_offrms = []
    selected_freqs = []
    #for item in offrms_freq_dictionary.keys(): - 2TO3
    for item in list(offrms_freq_dictionary.keys()):
        if float(item) >=1383.0 and float(item) < 1400.0:
            selected_offrms.append(offrms_freq_dictionary[item])
            selected_freqs.append(item)

    print ("Number of channels used: {0}".format(len(selected_offrms)))
    print ("Frequencies used: {0}".format(sorted(selected_freqs)))
    
    median = np.median(selected_offrms)
    print ("Median off-pulse rms: {0}".format(median))
    return median

def fluxcalibrate(archive,multiplier):
    "Applying the multiplier to all the decimated data products"
    
    
    print ("Flux calibrating {0}".format(os.path.split(archive)[-1]))
    info = "pam --mult {0} {1} -e fluxcal".format(multiplier,archive)
    arg = shlex.split(info)
    proc = subprocess.call(arg)
    
    
    
#=============================================================================


parser = argparse.ArgumentParser(description="Flux calibrate MTime data")
parser.add_argument("-psrname", dest="psrname", help="psrname", required=True)
parser.add_argument("-obsname", dest="obsname", help="Observation name", required=True)
parser.add_argument("-obsheader", dest="obsheader", help="obsheader", required=True)
parser.add_argument("-TPfile", dest="tpfile", help="T+P scrunched archive", required=True)
parser.add_argument("-rawfile", dest="rawfile", help="Raw (psradded) archive", required=True)
parser.add_argument("-dec_path", dest="decimated", help="List of decimated directories", required=True)
parser.add_argument("-parfile", help="Path to par file for pulsar", default="None")
args = parser.parse_args()


psr_name = str(args.psrname)
obs_name = str(args.obsname)
obsheader_path = str(args.obsheader)
TP_file = str(args.tpfile)
add_file = str(args.rawfile)
decimated_products = np.load(args.decimated)


print ("Processing {0}:{1}".format(psr_name, obs_name))
print ("============================================")
print ("Reference par file = {0}".format(str(args.parfile)))

#Get Tsky in Jy
#gl,gb = get_glgb(psr_name)
#tsky_jy = get_tsky(gl,gb)

# Get the RA and Dec from the par file if possible
if str(args.parfile) != "None":
    rajd, decjd = get_radec_new(str(args.parfile))

if str(args.parfile) == "None" or rajd is None:
    rajd, decjd = get_radec(psr_name)

tsky_jy = get_tsky_updated(rajd, decjd)

#Get Ssys at 1390 MHz
params = get_obsheadinfo(obsheader_path)
nant = len(params["ANTENNAE"].split(",")) 
ssys_1390 = get_Ssys(tsky_jy, nant)

#Get expected RMS in a single channel at 1390 MHz
info_TP = get_info(TP_file)
expected_rms = get_expectedRMS(info_TP, ssys_1390)

print ("============")
#Get centre-frequencies and off-pulse rms for the .add file - and creating a dictonary
freqinfo = get_freqlist(add_file)
freq_list = freqinfo[-2].split(",")
offrms_list = get_offrms(add_file)                
#offrms_freq = dict(zip(freq_list,offrms_list)) - 2TO3
offrms_freq = dict(list(zip(freq_list, offrms_list)))

#Getting median rms of off-pulse rms values for ~20 channels centered at 1390 MHz
observed_rms = get_median_offrms(offrms_freq)

print ("============")
#Multiplier
multiplier = expected_rms/observed_rms

print ("Multiplier is: {0}".format(multiplier))

print ("============")
#Flux calibrate all the decimated data products
for archive in decimated_products:
    fluxcalibrate(archive, multiplier)

print ("============")
print ("Flux calibrated {0}:{1}".format(psr_name, obs_name))
#sys.exit()


print ("=================================================")
