#!/bin/tcsh

# Andrew Cameron, 26/04/2022
# Wrapper script to be used by ingest_ptuse_folded.py to launch meerpipe in real time
# DO NOT CHANGE THE INTERFACE WITHOUT CHECKING WITH AJ FIRST

# Paths
set MEERPATH = "/fred/oz005/users/acameron/pipeline_stuff/andrew_meerpipe_dev/meerpipe/"

# Environment setup
echo "Configuring environment..."
source ${MEERPATH}/env_setup.csh
source /fred/oz005/pipelines/meerpipe/virtual-envs/meerpipe_db/bin/activate.csh
setenv PSRDB_TOKEN `get_ingest_token.sh`

echo "Parsing input..."
# Arguments
set IN_UTC = $1
set IN_PSR = $2
set IN_OBSID = $3
set IN_FOLDID = $4

# Run the pipeline
echo "Running PSR =" ${IN_PSR} "| UTC =" ${IN_UTC} "| OBS_ID =" ${IN_OBSID} "| FOLD_ID =" ${IN_FOLDID} "..."
python ${MEERPATH}/db_specified_launcher.py -utc1 ${IN_UTC} -utc2 ${IN_UTC} -psr ${IN_PSR} -slurm -runas PIPE -obs_id ${IN_OBSID} # -unprocessed -testrun
echo "Launch complete"
