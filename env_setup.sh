#!/bin/bash

# Andrew Cameron, 25/08/2021 (andrewcameron@swin.edu.au)
# CSH SCRIPT TO SET UP ENVIRONMENT FOR OPERATION OF MEERPIPE_DB - PYTHON3
# SHOULD PROVIDE IDENTICAL INSTRUCTIONS TO ENV_SETUP.CSH

# LOAD REQUIRED MODULES

module purge
module load psrhome/latest
module load psrchive/96b8d4477-python-3.6.4
module load psrdb/latest
module load pandas/0.22.0-python-3.6.4
module load matplotlib/2.2.2-python-3.6.4
module load scipy/1.3.0-python-3.6.4
module load astropy/3.1.2-python-3.6.4

# SET ENVIRONMENT VARIABLES
export COAST_GUARD=/fred/oz002/dreardon/MeerGuard_dev
export COASTGUARD_CFG=$COAST_GUARD/configurations
export PATH=$PATH\:$COAST_GUARD\:$COAST_GUARD/coast_guard
export PYTHONPATH=$PYTHONPATH\:$COAST_GUARD\:$COAST_GUARD/coast_guard
