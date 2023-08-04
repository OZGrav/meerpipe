"""
Loads all the data required by meerpipe from the data directory.
"""

import os

datadir = os.path.join(os.path.dirname(__file__), 'data')

# Load the files for simon's flux density radiometer equation calculations

# Used for UHF Tsky calcs
UHF_TSKY_FILE = os.path.join(datadir, 'UHF_Tsky.dat')

# Used for LBAND Tsky calcs
CHIPASS_EQU_CSV = os.path.join(datadir, 'CHIPASS_Equ.fits')

# Rotation measure values for MSPs
RM_CAT = os.path.join(datadir, 'meerpipe_rms_msps.txt')

# Delay config file for the PTUSE originally obtained from the dlyfix repo
DELAY_CONFIG = os.path.join(datadir, 'ptuse.dlycfg')
