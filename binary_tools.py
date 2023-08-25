#!/usr/bin/env python

"""
Code containing binary utilities adapted from Scintools codebase

__author__ = ["Andrew Cameron", "Daniel Reardon"]
__maintainer__ = "Andrew Cameron"
__email__ = "andrewcameron@swin.edu.au"
__status__ = "Development"
"""

# Imports
import numpy as np
from decimal import Decimal,InvalidOperation
from scipy.optimize import fsolve
import math

# Constants
DAYPERYEAR = 365.25

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
    U = np.fmod(U, 2*np.pi)

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
    OMB = get_OMB(pars)
    OM = OM + OMDOT*U/(OMB)

    return OM

def get_OMB(pars):
    """
    Return a simple, constant value of OMB (rad / days)
    """

    if 'PB' in pars.keys():
        OMB = 2*np.pi/pars['PB']

    elif 'FB0' in pars.keys():
        OMB = 2*np.pi*pars['FB0'] * 86400

    return OMB

def get_ecc(pars):
    """
    Calculate eccentricity depending on binary model
    """

    if 'TASC' in pars.keys():
        if 'EPS1' in pars.keys() and 'EPS2' in pars.keys():
            ECC = np.sqrt(pars['EPS1']**2 + pars['EPS2']**2)
        else:
            ECC = 0
    else:
        if 'ECC' in pars.keys():
            ECC = pars['ECC']
        else:
            ECC = 0

    return ECC

def get_T0(pars):
    """
    Calculate T0 depending on binary model
    """

    if 'TASC' in pars.keys():
        if 'EPS1' in pars.keys() and 'EPS2' in pars.keys():
            OMB = get_OMB(pars)
            T0 = pars['TASC'] + get_ELL1_arctan(pars['EPS1'], pars['EPS2'])/OMB  # MJD - No PBDOT correction required as referenced to zero epoch
        else:
            T0 = pars['TASC']
    else:
        T0 = pars['T0']  # MJD

    return T0

def get_mean_anomaly(mjds, pars):
    """
    Calculates mean anomalies for an array of barycentric MJDs and a parameter dictionary
    """

    # handle conversion of T0/TASC
    T0 = get_T0(pars)

    # determine which type of orbital period encoding we're dealing with
    if 'PB' in pars.keys():

        PB = pars['PB']

        # normal approach
        if 'PBDOT' in pars.keys():
            PBDOT = pars['PBDOT']
        else:
            PBDOT = 0

        if np.abs(PBDOT) > 1e-6: # adjusted from Daniels' setting
            # correct tempo-format
            PBDOT *= 10**-12

        OMB = get_OMB(pars)
        M = OMB*((mjds - T0) - 0.5*(PBDOT/PB) * (mjds - T0)**2)

    elif 'FB0' in pars.keys():

        M = np.zeros(len(mjds))
        i = 0

        # produce integrated Taylor series of FB terms
        while ('FB' + ('%s' % i) in pars.keys()):
            M = M + pars['FB' + ('%s' % i)] * ((mjds - T0)**(i+1))/math.factorial(i + 1)
            i += 1

        M = M * 2*np.pi * 86400

    M = M.squeeze()
    return M

def get_eccentric_anomaly(mjds, pars):
    """
    Calculates eccentric anomalies for an array of barycentric MJDs and a parameter dictionary
    """

    # first obtain mean anomaly
    M = get_mean_anomaly(mjds, pars)

    # handle conversion of EPS/ECC
    ECC = get_ecc(pars)

    # eccentric anomaly
    if ECC < 1e-4:
        print('Assuming circular orbit for true anomaly calculation')
        E = M
    else:
        M = np.asarray(M, dtype=np.float64)
        E = fsolve(lambda E: E - ECC*np.sin(E) - M, M)
        E = np.asarray(E, dtype=np.float128)

    return E

def get_true_anomaly(mjds, pars):
    """
    Calculates true anomalies for an array of barycentric MJDs and a parameter dictionary
    """

    # first obtain eccentric anomaly
    E = get_eccentric_anomaly(mjds, pars)

    # handle conversion of EPS/ECC
    ECC = get_ecc(pars)

    # true anomaly
    U = 2*np.arctan2(np.sqrt(1 + ECC) * np.sin(E/2), np.sqrt(1 - ECC) * np.cos(E/2))

    if hasattr(U,  "__len__"):
        U[np.argwhere(U < 0)] = U[np.argwhere(U < 0)] + 2*np.pi
        #U = U.squeeze()
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

    bflag = 'BINARY' in pars.keys()
    orbflag = 'PB' in pars.keys() or 'FB0' in pars.keys()
    timeflag = 'TASC' in pars.keys() or 'T0' in pars.keys()

    if (bflag and orbflag and timeflag):
        retval = True

    return retval
