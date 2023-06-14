#!/bin/bash

# Andrew Cameron, 25/08/2021 (andrewcameron@swin.edu.au)
# CSH SCRIPT TO SET UP ENVIRONMENT FOR OPERATION OF MEERPIPE_DB - PYTHON3
# SHOULD PROVIDE IDENTICAL INSTRUCTIONS TO ENV_SETUP.CSH

if [ ${SYS_ARCH} == "milan" ]; then
    # Using new Ngarrgu Tindebeek (NT) cluster
    module use /apps/users/pulsar/milan/gcc-11.3.0/modulefiles
elif [ ${SYS_ARCH} == "skylake" ]; then
    # Using original ozstar cluster
    module use /apps/users/pulsar/skylake/modulefiles
fi
module purge

# LOAD REQUIRED MODULES
module load psrhome/latest
if [ ${SYS_ARCH} == "milan" ]; then
    # Using new Ngarrgu Tindebeek (NT) cluster
    # module load psrdb/fdb4d06
    module load psrdb/3f70bad
    module load scintools/ba68b84
    module load pandas/1.4.2-scipy-bundle-2022.05
    module load matplotlib/3.5.2
    module load nextflow/23.04.1
elif [ ${SYS_ARCH} == "skylake" ]; then
    # Using original ozstar cluster
    module load psrchive/96b8d4477-python-3.6.4
    module load psrdb/latest
    module load pandas/0.22.0-python-3.6.4
    module load matplotlib/2.2.2-python-3.6.4
    module load scipy/1.3.0-python-3.6.4
    module load astropy/3.1.2-python-3.6.4
fi

# SET ENVIRONMENT VARIABLES
export COAST_GUARD=/fred/oz005/software/MeerGuard
export COASTGUARD_CFG=$COAST_GUARD/configurations
export PATH=$PATH\:$COAST_GUARD\:$COAST_GUARD/coast_guard
export PYTHONPATH=$PYTHONPATH\:$COAST_GUARD\:$COAST_GUARD/coast_guard
