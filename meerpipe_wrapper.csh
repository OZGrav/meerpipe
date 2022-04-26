#!/bin/tcsh

# Andrew Cameron, 26/04/2022
# Wrapper script to be used by ingest_ptuse_folded.py to launch meerpipe in real time
# DO NOT CHANGE THE INTERFACE WITHOUT CHECKING WITH AJ FIRST

# Paths
MEERPATH="/fred/oz005/users/acameron/pipeline_stuff/andrew_meerpipe_dev/meerpipe/"

# Environment setup
source ${MEERPATH}/env_setup.csh
source /home/acameron/virtual-envs/meerpipe_db/bin/activate.csh

# Arguments
IN_UTC=$1
IN_PSR=$2
IN_OBSID=$3
IN_FOLDID=$4

# Run the pipeline
python ${MEERPATH}/db_specified_launcher.py 'LAUNCH PARAMETERS GO HERE' -slurm -runas PIPE

