#!/usr/bin/env python

"""
Code containing utilities adapted from MeerWatch codebase - /fred/oz005/users/meerpipe_dev/MeerWatch

__author__ = ["Andrew Cameron", "Matthew Bailes", "Renee Spiewak", "Daniel Reardon"]
__credits__ = ["Aditya Parthasarathy"]
__maintainer__ = "Andrew Cameron"
__email__ = "andrewcameron@swin.edu.au"
__status__ = "Development"
"""

# Imports
import re, os, sys
import numpy as np
from shlex import split as shplit
import subprocess as sproc
import matplotlib.pyplot as plt
from matplotlib import colors, cm
from astropy.time import Time
from decimal import Decimal,InvalidOperation
from scipy.optimize import fsolve

# Constants
DAYPERYEAR = 365.25

# calculate the rms of the toa format used in this library
# danger: assumes correct residual wrt to zero
def weighted_rms(toas):

    numerator = 0
    denominator = 0
    for x in range (0, len(toas)):

        weight = 1.0 / (toas['err'][x]**2)
        numerator += weight * (toas['res'][x]**2)
        denominator += weight

    return np.sqrt(numerator / denominator)

# calculates weighted mean of a set of toas
def weighted_mean(toas):
    numerator = 0
    demoninator = 0
    for x in range (0, len(toas)):
        weight = 1.0/ (toas['err'][x]**2)
        numerator += weight * toas['res'][x]
        demoninator += weight

    return (numerator/ demoninator)

def get_res_fromtim(tim_file, par_file, sel_file=None, out_dir="./", verb=False):

    # WARNING - The precise format of the output residual file is critical to the
    # correct operation of the Data Portal (pulsars.org.au). Changes to this format
    # will result in pulsr/project specific TOA plots failing to generate, and may
    # result in unexpected crashes. Please liaise on any changes to the format/storage
    # of the residual files with the Data Portal maintenance team.

    tempo2_call = "tempo2 -nofit -set START 40000 -set FINISH 99999 "\
                  "-output general2 -s \"{{bat}} {{post}} {{err}} "\
                  "{{freq}} {{post_phase}} BLAH\n\" -nobs 1000000 -npsr 1 -f {} {}"
    awk_cmd = "awk '{print $1,$2,$3*1e-6,$4,$5}'"
    temp_file = os.path.basename(tim_file).replace('.tim', '.delme')
    res_file = os.path.basename(tim_file).replace('.tim', '_res.txt')
    comp_file = os.path.basename(tim_file).replace('.tim', '_res_comp.txt')
    temp_file = os.path.join(out_dir, temp_file)
    res_file = os.path.join(out_dir, res_file)
    comp_file = os.path.join(out_dir, comp_file)

    # if a select file is given, include it
    if sel_file is not None:
        tempo2_call += " -select {}".format(sel_file)

    # call tempo2 to produce residuals that can be read in
    with open(temp_file, 'w') as f:
        if verb:
            print("Running tempo2 command: {}".replace("\n", "\\n").format(tempo2_call.format(par_file, tim_file)))

        # re-writing to accommodate large PIPE sizes
        p1 = sproc.Popen(shplit(tempo2_call.format(par_file, tim_file)), stdout=sproc.PIPE)
        p1_data = p1.communicate()[0]
        p2 = sproc.Popen(shplit("grep BLAH"), stdin=sproc.PIPE,stdout=sproc.PIPE)
        p2_data = p2.communicate(input=p1_data)[0]
        p3 = sproc.Popen(shplit(awk_cmd), stdin=sproc.PIPE, stdout=f)
        p3.communicate(input=p2_data)

    if verb:
        print("Finished running tempo2")

    # define data formats of products to be handled
    mjd_f = ('mjd','f16')
    doy_f = ('doy', 'f16')
    res_f = ('res', 'f4')
    res_phase_f = ('res_phase', 'f4')
    err_f = ('err', 'f4')
    err_phase_f = ('err_phase', 'f4')
    freq_f = ('freq', 'f8')
    binphase_f = ('binphase', 'f8')

    # load in the toa residuals and cleanup
    toas = np.loadtxt(temp_file, usecols=(0, 1, 2, 3, 4), dtype=[mjd_f, res_f, err_f, freq_f, res_phase_f])
    #os.remove(temp_file)

    if verb:
        print ("Loaded ToAs from file")

    # convert data
    doys = np.zeros(len(toas), dtype=[doy_f])
    doys[doy_f[0]] = calc_doy(toas[mjd_f[0]])

    phase_errors = np.zeros(len(toas), dtype=[err_phase_f])
    phase_errors[err_phase_f[0]] = calc_err_phase(toas[res_f[0]], toas[err_f[0]], toas[res_phase_f[0]])

    # prepare binary phase if required
    bflag = False
    
    try:
        pars = read_par(par_file)
    except:
        print ("Unable to parse parfile ({})".format(par_file))
    else:
        if (is_binary(pars)):
            print ("Binary pulsar detected - calculating binary phases...")
            bflag = True
            binphases = np.zeros(len(toas), dtype=[binphase_f])
            binphases[binphase_f[0]] = get_binphase(toas[mjd_f[0]],pars)

    # concatenate data in the correct order
    dtype_list = [mjd_f, doy_f, res_f, res_phase_f, err_f, err_phase_f, freq_f]
    arr_list = [toas, doys, phase_errors]
    if (bflag):
        dtype_list.append(binphase_f)
        arr_list.append(binphases)

    toas_exp = np.zeros(toas.shape, dtype=dtype_list)
    for x in arr_list:
        for y in x.dtype.names:
            toas_exp[y] = x[y]

    # write out

    if len(toas_exp) == 0:
        if verb:
            print("No ToAs from tempo2 for {}".format(tim_file))
    else:
        if verb:
            print ("Writing out {} residuals to disk...".format(len(toas_exp)))
        raw_str="%s\t%s\t%s\t%s\t%s\t%s\t%s"
        comp_str="%12.6f\t%9.6f\t%.4e\t%.4e\t%.2e\t%.2e\t%9.4f"
        if (bflag):
            raw_str = raw_str + "\t%s"
            comp_str = comp_str + "\t%.8e"
        # write out uncompressed residuals
        np.savetxt(res_file, toas_exp, fmt=raw_str)
        # write out the compressed residuals
        np.savetxt(comp_file, toas_exp, fmt=comp_str)

    if toas_exp.size == 1:
        if verb:
            print("Only one ToA from {}; setting return array to be empty".format(tim_file))
        toas_exp = np.array([])

    return(toas_exp)

# calculate day-of-year given an MJD (credit: Daniel Reardon)
def calc_doy(mjd):

    t = Time(mjd, format='mjd')
    yrs = t.jyear # observation year in J2000.0
    # return 365.2425 * (yrs % 1)
    return DAYPERYEAR * (yrs % 1) # J2000.0 in astropy.time built of year = 365.25

# calculate phase error
def calc_err_phase(res, err, res_phase):

    return (res_phase / res) * err


# determine outliers and remove from plot
def clean_toas(input_toas,outlier_factor=3):
    absvals = np.abs(input_toas['res'])
    mad = np.median(absvals) # this may not be the strict MAD calculation - residuals may be previously offset with regard to mean rather than median
    sigma = 1.4826 * mad
    indices = np.argwhere(absvals<=outlier_factor*sigma)
    new_toas = np.zeros(len(indices), dtype=input_toas.dtype)
    for name in input_toas.dtype.names:
        new_toas[name] = input_toas[name][indices].squeeze()
    return(new_toas)

def plot_toas_fromarr(toas, pid="unk", mjd=None, fs=14, out_file="toas.png", out_dir=None, sequential=True, title=None, verb=False, bw=856, cfrq=1284, nchn=None,
                      outlier_factor=None):

    if outlier_factor is not None:
        toas = clean_toas(toas,outlier_factor=outlier_factor)
        weighted_mean_toa = weighted_mean(toas)
        for k in range (0, len(toas)):
            toas['res'][k]-=weighted_mean_toa

    if out_dir:
        out_file = os.path.join(out_dir, out_file)

    # use semi-fixed color normalisation
    f_min = cfrq-bw/2.0
    f_max = cfrq+bw/2.0

    norm = colors.Normalize(vmin=f_min, vmax=f_max)

    fig = plt.figure(num=1)
    fig.set_size_inches(6, 4.5)
    ax = fig.gca()

    if sequential:
        if verb:
            print("Plotting serial ToAs")

        if nchn is None:
            raise(RuntimeError("Cannot get number of channels"))

        chan = range(nchn)
        freq_mins = [f_min+(i*bw/nchn) for i in chan]
        freq_maxs = [f_min+((i+1)*bw/nchn) for i in chan]
        pulse = 0
        last_chan = -1
        num = []

        for f in toas['freq']:
            for i, mi, ma in zip(chan, freq_mins, freq_maxs):
                if mi < f < ma:
                    if i <= last_chan:
                        pulse += nchn

                    num.append(pulse+i)

                    last_chan = i
                    break

        if len(num) != len(toas['freq']):
            print(num, toas['freq'], freq_mins, freq_maxs)
            raise(RuntimeError("Error determining ToA Number for {}".format(out_file)))

        xdata = np.array(num)
    else:
        if verb:
            print("Plotting against MJD")

        xdata = toas['mjd']

    p2 = ax.scatter(xdata, toas['res']*1e6, s=8, c=toas['freq'], marker='s', norm=norm, cmap='viridis')
    cb = fig.colorbar(p2, ax=ax, fraction=0.1)

    lines = ax.errorbar(xdata, toas['res']*1e6, yerr=1e6*toas['err'], ls='', marker='', ms=1, zorder=0)[2]
    lines[0].set_color(cb.to_rgba(toas['freq']))

    print (xdata)
    spread = xdata.max()-xdata.min()
    xmin = xdata.min()-0.05*spread
    xmax = xdata.max()+0.05*spread
    ax.plot([xmin-0.1*spread, xmax+0.1*spread], [0, 0], ls='--', color='0.5')
    ax.set_xlim(xmin, xmax)

    cb.set_label("Observing frequency (MHz)", rotation=270, size=fs, labelpad=16)
    if sequential:
        ax.set_xlabel("ToA Number", fontsize=fs)
        ax.set_title("Single-observation TOAs ({0})".format(pid), fontsize=fs)
    else:
        ax.set_xlabel("MJD", fontsize=fs)

        # new - include WRMS as part of the plot
        if (len(toas) > 0):
            wrms = weighted_rms(toas)/(1e-6)
            ax.set_title("Global TOAs ({0}) | Wrms={1:.2f}$\mu$s".format(pid, wrms), fontsize=fs)
        else:
            ax.set_title("Global TOAs ({0})".format(pid), fontsize=fs)
        #ax.set_title("Global TOAs ({0})".format(pid), fontsize=fs)

        # new - add vertical line to indicate the MJD of the observation being processed
        if not ( mjd == None ):
            ax.vlines(mjd, ymin=0, ymax=1, color='red', linestyle='dotted', transform=ax.get_xaxis_transform(), zorder=1)

    ax.set_ylabel("residuals ($\mu$s)", fontsize=fs)
    if title is not None and type(title) is str:
        ax.set_title(title, fontsize=fs+2)

    plt.savefig(out_file, bbox_inches='tight')

    plt.clf()

# --- BINARY PHASE CODE ---
# --- Largely adapted from Scintools code provided by Daniel Reardon ---

# reads a par file into a dictionary object
# this functionality is already performed to an extent by PSRDB / util.ephemeris, but
# that code is moreso just to read and store the fields. This code has better control for
# handling/mainpulating/calculating with those fields, and so for the moment I'll retain it.
def read_par(parfile):
    """
    Reads a par file and return a dictionary of parameter names and values
    """

    par = {}
    ignore = ['DMMODEL', 'DMOFF', "DM_", "CM_", 'CONSTRAIN', 'JUMP', 'NITS',
              'NTOA', 'CORRECT_TROPOSPHERE', 'PLANET_SHAPIRO', 'DILATEFREQ',
              'TIMEEPH', 'MODE', 'TZRMJD', 'TZRSITE', 'TZRFRQ', 'EPHVER',
              'T2CMETHOD']

    file = open(parfile, 'r')
    for line in file.readlines():
        err = None
        p_type = None
        sline = line.split()
        if len(sline) == 0 or line[0] == "#" or line[0:2] == "C " or sline[0] in ignore:
            continue

        param = sline[0]
        if param == "E":
            param = "ECC"

        val = sline[1]
        if len(sline) == 3 and sline[2] not in ['0', '1']:
            err = sline[2].replace('D', 'E')
        elif len(sline) == 4:
            err = sline[3].replace('D', 'E')

        try:
            val = int(val)
            p_type = 'd'
        except ValueError:
            try:
                val = float(Decimal(val.replace('D', 'E')))
                if 'e' in sline[1] or 'E' in sline[1].replace('D', 'E'):
                    p_type = 'e'
                else:
                    p_type = 'f'
            except InvalidOperation:
                p_type = 's'

        par[param] = val
        if err:
            par[param+"_ERR"] = float(err)
            
        if p_type:
            par[param+"_TYPE"] = p_type

    file.close()

    return par

def get_binphase(mjds, pars):
    """
    Calculates binary phase for an array of barycentric MJDs and a parameter dictionary
    """
    
    U = get_true_anomaly(mjds, pars)

    OM = get_omega(pars, U)

    # normalise U
    U =np.fmod(U, 2*np.pi)

    return np.fmod(U + OM + 2*np.pi, 2*np.pi)/(2*np.pi)

def get_ELL1_arctan(EPS1, EPS2):
    """
    Given the EPS1 and EPS2 parameters of the ELL1 binary model,
    calculate the arctan(EPS1/EPS2) value accounting for all degeneracies and ambiguities.
    This function has been abstracted as it is needed for calculating both OM and T0
    """

    # check for undefined tan
    if (EPS2 == 0):
        if (EPS1 > 0):
            AT = np.pi/2
        elif (EPS1 < 0):
            AT = -np.pi/2
        else:
            # eccentricity must be perfectly zero - omega is therefore undefined
            AT = 0
    else:
        AT = np.arctan(EPS1/EPS2)
        # check for tan degeneracy
        if (EPS2 < 0):
            AT += np.pi

    return np.fmod(AT + 2*np.pi, 2*np.pi)

def get_omega(pars, U):
    """
    Calculate the instantaneous version of omega (radians) accounting for OMDOT
    per Eq. 8.19 of the Handbook. May be slightly incorrect for relativistic systems
    """

    # get reference omega
    if 'TASC' in pars.keys():
        if 'EPS1' in pars.keys() and 'EPS2' in pars.keys():

            OM = get_ELL1_arctan(pars['EPS1'], pars['EPS2'])
            # ensure OM within range 0..2pi
            OM = np.fmod(OM + 2*np.pi, 2*np.pi)

        else:
            OM = 0
    else:
        if 'OM' in pars.keys():
            OM = pars['OM'] * np.pi/180
        else:
            OM = 0

    # get change in omega
    if 'OMDOT' in pars.keys():
        # convert from deg/yr to rad/day
        OMDOT = pars['OMDOT'] * (np.pi/180) / DAYPERYEAR
    else:
        OMDOT = 0
    
    # calculate current omega
    PB = pars['PB'] # days

    OM = OM + OMDOT*U/(2*np.pi/PB)
    
    return OM

def get_true_anomaly(mjds, pars):
    """
    Calculates true anomalies for an array of barycentric MJDs and a parameter dictionary
    """

    # handle orbital period
    PB = pars['PB']  # days

    if 'PBDOT' in pars.keys():
        PBDOT = pars['PBDOT']
    else:
        PBDOT = 0

    if np.abs(PBDOT) > 1e-6: # adjusted from Daniels' setting
        # correct tempo-format
        PBDOT *= 10**-12

    NB = 2*np.pi/PB

    # handle ELL1 ephemeris
    if 'TASC' in pars.keys():
        if 'EPS1' in pars.keys() and 'EPS2' in pars.keys():
            T0 = pars['TASC'] + get_ELL1_arctan(pars['EPS1'], pars['EPS2'])/NB  # MJD - No PBDOT correction required as referenced to zero epoch
            ECC = np.sqrt(pars['EPS1']**2 + pars['EPS2']**2)
        else:
            T0 = pars['TASC']
            ECC = 0
    else:
        T0 = pars['T0']  # MJD
        if 'ECC' in pars.keys():
            ECC = pars['ECC']
        else:
            ECC = 0

    # mean anomaly
    M = NB*((mjds - T0) - 0.5*(PBDOT/PB) * (mjds - T0)**2)
    M = M.squeeze()

    # eccentric anomaly
    if ECC < 1e-4:
        print('Assuming circular orbit for true anomaly calculation')
        E = M
    else:
        M = np.asarray(M, dtype=np.float64)
        E = fsolve(lambda E: E - ECC*np.sin(E) - M, M)
        E = np.asarray(E, dtype=np.float128)

    # true anomaly
    U = 2*np.arctan2(np.sqrt(1 + ECC) * np.sin(E/2), np.sqrt(1 - ECC) * np.cos(E/2))  # true anomaly

    if hasattr(U,  "__len__"):
        U[np.argwhere(U < 0)] = U[np.argwhere(U < 0)] + 2*np.pi
        U = U.squeeze()
    elif U < 0:
        U += 2*np.pi

    # final change - need to have U count the number of orbits - rescale to match M and E
    E_fac = np.floor_divide(E, 2*np.pi)
    U += E_fac * 2*np.pi

    return U

def is_binary(pars):
    """
    Determine if a set of parameters adequately describes a binary pulsar
    """

    retval = False

    if ('BINARY' in pars.keys() and 'PB' in pars.keys() and ('TASC' in pars.keys() or 'T0' in pars.keys())):
        retval = True

    return retval
