Creating documentation...to be updated
Contact Aditya Parthasaraty for details - adityapartha3112@gmail.com
Contact Andrew Cameron for details of the forDB branch updates - andrewcameron@swin.edu.au

Two branches : 
main: for the PID processing
forDB: for the database upgrades

Additional notes re: forDB
 * Before launching the script, run `source env_setup.csh; source /home/acameron/virtual-envs/meerpipe_db/bin/activate.csh'. Equivalent .sh files are available depending on your choice of shell.
 * Internal hardcoded software paths may be set differently due to testing requirements. This should be phased out in future with incorporation of `-softpath' options.
 * Subject to the above conditions, the code operates just as it did before. DB-functionality should only be used for testing, and is activated via the `-db_flag' and associated parameters in `run_pipe.py'.
