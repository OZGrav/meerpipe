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

OLD STUFF ABOVE HERE!

## Overview

Meerpipe is a python-based data analysis pipeline that produces the cleaned and calibrated data that forms the basis of all pulsar timing projects with the MeerKAT radio telescope as part of the [MeerTime project](http://www.meertime.org/). For an overview of MeerTime and the various science themes, please refer to [Bailes et al. 2020](https://arxiv.org/abs/2005.14366)  

## Functionalities

The raw data recorded by the Pulsar Timing User Supplied Equipment (PTUSE) is transferred to the OzStar supercomputing facility in Melbourne, Australia where meerpipe is hosted. Each new observation automatically triggers an instance of the pipeline that is launched using the Slurm job manager. The pipeline optimizes the requested memory and wall time based on the length of the observation. 

For each observation, meerpipe implements the following routines:

1. Polarisation calibration: The Jones matrices provided by the South African Radio Astronomy Observatory (SARAO) are used by meerpipe to perform polarisation calibration. A detailed description can be found in [Serylak et al. 2020](https://arxiv.org/pdf/2009.05797.pdf)

2. Cleaning: The radio frequency interference in the calibrated data is removed by utilizing a combination of cleaning algorithms that are part of a customised software package, MeerGuard.  

3. Flux calibration: Meerpipe also uses a bootstrap method to flux calibrate the data. Using the sky map in Calabretta et al. 2014, an initial estimate of the Tsky was computed. However owing to the large disparity in the  sky map as comparted to the more resolved MeerKAT beam, a secondary estimate of Tsky was performed directly from the data. This was done by observing a high-latitude pulsar and comparing its RMS with other pulsars observed in that session. This allowed an estimate of a scaling factor used to flux calibrate the data. A detailed description of this method can be found in the TPA census paper. 

4. User-defined data products: Following the requirements laid out by the various science themes within MeerTime, meerpipe produces the requested decimated data products post-cleaning. These include data products at various frequency and time resolutions along with the dynamic spectra for scintillation studies. S

5. Times-of-arrival: Using the decimated data products, the times-of-arrival per observation are computed using [PSRCHIVE](http://psrchive.sourceforge.net/). 


## Database management

On top of the standard data processing offered by the main branch, the "forDB" branch of this repository links the data products and the pipeline to a central database (PSRDB) that forms the backend for the online data portal. This database can be interacted with both via a CLI and a Python/SQL-based API.

As part of the ingest process from PTUSE to OzStar, all fold-mode observations are automatically recorded to PSRDB along with information including their UTC, telescope configuration and folding ephemeris. The forDB branch is able to leverage the database in a number of ways:

 * Multiple pipeline configurations: PSRDB is able to store records detailing multiple instances of meerpipe, configured for different projects such as TPA, PTA, RelBin, etc. These configurations (stored in the `pipelines' table) are linked to the respective project codes, making for easy recall via query.

 * Matching pulsars to projects: PSRDB also links individual pulsars to specific pipeline configurations (via the `launches' table), such that when meerpipe SLURM jobs are automatically launched after a new observation is ingested, PSRDB can be queried to know which pipeline configurations to use for a particular pulsar. For example, if a pulsar is part of both TPA and PTA, the `launches' table can be used to initiate two separate SLURM jobs, one for each version of the pipeline.

 * Batch job launches based on database queries: The script `db_specified_launcher.py' acts as a wrapper to the rest of the pipeline, and can query the database for observations matching a given UTC range, pulsar name, etc. Jobs can also be directed to use a specific pipeline configuration or the default configurations listed in PSRDB against the pulsar being processed.

 * Writing processing results to the database: As part of meerpipe itself, results of the processing are now automatically stored in the database at several key points. This includes:
  - Processings: stores information about the status of a specific observation being processed by a specific pipeline configuration. Updates in real time to reflect the SLURM status of the job (including the host node and SLURM job ID), whether the job has completed successfully or crashed, etc. A JSON string also includes information on the observation itself (flux density after cleaning, zapped fraction, etc.)
  - Ephemerides & Templates: stores information on the specific ephemerides and templates used to analyse each observation. If the same object is used to process multiple observations, the same PSRDB object ID is referenced. The ephemeris entry includes both a JSON string storing the complete ephemeris, as well as fields noting the value of any external DM or RM used to override the ephemeris values.
  - TOAs: Stores summary information on the TOA quality of a given observation after processing. Includes a quality flag that can be used to isolate bad epochs from later analysis.
  - Pipelineimages & Pipelinefiles: perhaps the most important part of PSRDB accessed by the pipeline, these tables store the output images and files that are then made available by the MeerTime pulsar portal, most notably the diagnostic TOA plots that track the timing of the pulsar over time.

The bulk of the code that interfaces between meerpipe and PSRDB is abstracted in its own software library, db_utils.py, which communicates with PSRDB via the API. A number of helper functions are also includes under the misc_scripts directory, important members of which include:

 * jobstate_query.py - can be used to report the status of all processings ("Complete", "Running", "Crashed", etc.). Useful for tracking the performance of the code and isolating any potential issues.
 * launch_populate.py - can assign a list of pulsars to a specific pipeline in the launches table.
 * toaquality_query.py & toaquality_modify.py - can be used to both check on and adjust the quality flag of TOA entries.

Caution should be used when using some of these scripts however, as they have the potential to make large scale changes to the content of the database very quickly.

In addition to the extra database functionality, the forDB branch also includes a number of small bugfixes (e.g. processing of observations with non-standard channel/bin counts) and quality of life upgrades (e.g. output reporting, customisation of RAM / walltime, etc.).

## Online Data Release

The raw and processed data from meerpipe can be accessed via this [data portal](https://pulsars.org.au/login/). The data portal provides access to both the fold mode and the search mode data recorded with MeerKAT. Observations have a wide range of tags that allow the user to filter and curate them. 

## Dependencies

MeerGuard: A customised version of [CoastGuard](https://github.com/plazar/coast_guard) for wide-bandwidth data is available [here](https://github.com/danielreardon/MeerGuard).

Scintools: Meerpipe uses Scintools for producing the dynamic spectra and the associated plots. The code can be found [here](https://github.com/danielreardon/scintools).

## Running the pipeline

The pipeline uses configuration files to determine how the data is processed. Various science themes have unique configuration files that specify the I/O structure and customised data-products. Note that these configuration files are specific to the machine hosting the pipeline, so please modify them accordingly. 

Both `reprocessing.py` and `run_pipe.py` can be used to launch the pipeline. The former is specifically designed for batch processing. 

```
usage: reprocessing.py [-h] [-cfile CONFIGFILE] [-list_pid LIST_PID]
                       [-list LIST] [-runas RUNAS]

Run MeerPipe automatically on new data - use in conjunction with query_obs.py

optional arguments:
  -h, --help          show this help message and exit
  -cfile CONFIGFILE   Path to the configuration file
  -list_pid LIST_PID  List of PSR and UTCs and PIDs
  -list LIST          List of PSR and UTCs
  -runas RUNAS        Process observation as PID
  ``` 
 A run command could like `python reprocessing.py -cfile <path_to_config> -list <space_separated_list_of_pulsarname_utc> -runas <TPA,RelBin,PTA>`
 
 For more details, queries please contact:
 1) Dr. Aditya Parthasarathy (MPIfR): adityapartha3112@gmail.com
 2) Dr. Andrew Cameron (Swinburne University of Technology): andrewcameron@swin.edu.au

Papers that have used `Meerpipe`:
1. [The MeerTime Pulsar Timing Array: A census of emission properties and timing potential](https://ui.adsabs.harvard.edu/abs/2022PASA...39...27S/abstract)
2. [The thousand-pulsar-array programme on MeerKAT VII: polarisation properties of pulsars in the Magellanic Clouds](https://ui.adsabs.harvard.edu/abs/2022MNRAS.509.5209J/abstract)
3. [The Thousand-Pulsar-Array programme on MeerKAT - VI. Pulse widths of a large and diverse sample of radio pulsars](https://ui.adsabs.harvard.edu/abs/2021MNRAS.508.4249P/abstract)
4. [The thousand-pulsar-array programme on MeerKAT IV: Polarization properties of young, energetic pulsars](https://ui.adsabs.harvard.edu/abs/2021MNRAS.505.4483S/abstract)
5. [The Thousand-Pulsar-Array programme on MeerKAT - II. Observing strategy for pulsar monitoring with subarrays](https://ui.adsabs.harvard.edu/abs/2021MNRAS.505.4456S/abstract)
6. [The relativistic binary programme on MeerKAT: science objectives and first results](https://ui.adsabs.harvard.edu/abs/2021MNRAS.504.2094K/abstract)
7. [The Thousand-Pulsar-Array programme on MeerKAT - V. Scattering analysis of single-component pulsars](https://ui.adsabs.harvard.edu/abs/2021MNRAS.504.1115O/abstract)
8. [Multifrequency observations of SGR J1935+2154](https://ui.adsabs.harvard.edu/abs/2021MNRAS.503.5367B/abstract)
9. [Measurements of pulse jitter and single-pulse variability in millisecond pulsars using MeerKAT](https://ui.adsabs.harvard.edu/abs/2021MNRAS.502..407P/abstract)
10. [The Thousand-Pulsar-Array programme on MeerKAT - I. Science objectives and first results](https://ui.adsabs.harvard.edu/abs/2020MNRAS.493.3608J/abstract)
