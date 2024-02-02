# Meerpipe

![tests](https://github.com/OZGrav/meerpipe/actions/workflows/pytest.yml/badge.svg)
![docker](https://github.com/OZGrav/meerpipe/actions/workflows/docker_build_push.yml/badge.svg)
![documentation](https://readthedocs.org/projects/meerpipe/badge/?version=latest)

## New Nextflow Pipeline

This repository no longer contains the most recent version of meerpipe, it is now the python scripts used by the [nf-core-meerpipe repository](https://github.com/OZGrav/nf-core-meerpipe) and a [docker container](https://hub.docker.com/repository/docker/nickswainston/meerpipe/general) that can be used by the pipeline (mostly for testing).


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

 * Multiple pipeline configurations: PSRDB is able to store records detailing multiple instances of meerpipe, configured for different projects such as TPA, PTA, RelBin, etc. These configurations (stored in the _pipelines_ table) are linked to the respective project codes, making for easy recall via query.

 * Matching pulsars to projects: PSRDB also links individual pulsars to specific pipeline configurations (via the _launches_ table), such that when meerpipe SLURM jobs are automatically launched after a new observation is ingested, PSRDB can be queried to know which pipeline configurations to use for a particular pulsar. For example, if a pulsar is part of both TPA and PTA, the _launches_ table can be used to initiate two separate SLURM jobs, one for each version of the pipeline.

 * Batch job launches based on database queries: The script `db_specified_launcher.py` acts as a wrapper to the rest of the pipeline, and can query the database for observations matching a given UTC range, pulsar name, etc. Jobs can also be directed to use a specific pipeline configuration or the default configurations listed in PSRDB against the pulsar being processed.

 * Writing processing results to the database: As part of meerpipe itself, results of the processing are now automatically stored in the database at several key points. This includes writing data to the following tables:
    * _Processings_: stores information about the status of a specific observation being processed by a specific pipeline configuration. Updates in real time to reflect the SLURM status of the job (including the host node and SLURM job ID), whether the job has completed successfully or crashed, etc. A JSON string also includes information on the observation itself (flux density after cleaning, zapped fraction, etc.)
    * _Ephemerides_ & _Templates_: stores information on the specific ephemerides and templates used to analyse each observation. If the same object is used to process multiple observations, the same PSRDB object ID is referenced. The ephemeris entry includes both a JSON string storing the complete ephemeris, as well as fields noting the value of any external DM or RM used to override the ephemeris values.
    * _TOAs_: Stores summary information on the TOA quality of a given observation after processing. Includes a quality flag that can be used to isolate bad epochs from later analysis.
    * _Pipelineimages_ & _Pipelinefiles_: perhaps the most important part of PSRDB accessed by the pipeline, these tables store the output images and files that are then made available by the MeerTime pulsar portal, most notably the diagnostic TOA plots that track the timing of the pulsar over time.

The bulk of the code that interfaces between meerpipe and PSRDB is abstracted in its own software library, `db_utils.py`, which communicates with PSRDB via the API. A number of helper functions are also includes under the misc_scripts directory, important members of which include:

 * `jobstate_query.py` - can be used to report the status of all processings ("Complete", "Running", "Crashed", etc.). Useful for tracking the performance of the code and isolating any potential issues.
 * `launch_populate.py` - can assign a list of pulsars to a specific pipeline in the launches table.
 * `toaquality_query.py` & `toaquality_modify.py` - can be used to both check on and adjust the quality flag of TOA entries.

Caution should be used when using some of these scripts however, as they have the potential to make large scale changes to the content of the database very quickly.

In addition to the extra database functionality, the forDB branch also includes a number of small bugfixes (e.g. processing of observations with non-standard channel/bin counts) and quality of life upgrades (e.g. output reporting, customisation of RAM / walltime, etc.).

## Online Data Release

The raw and processed data from meerpipe can be accessed via this [data portal](https://pulsars.org.au/login/). The data portal provides access to both the fold mode and the search mode data recorded with MeerKAT. Observations have a wide range of tags that allow the user to filter and curate them.

## Dependencies

MeerGuard: A customised version of [CoastGuard](https://github.com/plazar/coast_guard) for wide-bandwidth data is available [here](https://github.com/danielreardon/MeerGuard).

Scintools: Meerpipe uses Scintools for producing the dynamic spectra and the associated plots. The code can be found [here](https://github.com/danielreardon/scintools).

## Running the pipeline

As the "forDB" branch is intrinsically linked with OzStar and its local implementation of PSRDB, it is likely not possible for the code to be run externally with the database functionality activated. Furthermore, in order to control the integrity of PSRDB, operators wishing to use the PSRDB functionality of meerpipe within OzStar should contact Andrew Cameron (details below) before proceding as various permissions will need to be established beforehand. The following information is primarily intended to provide context regarding the pipeline's operation for those using the resulting data products.

The primary manual launch script for the "forDB" branch is `db_specified_processing.py`. This script is some senses a wrapper script which interfaces with `run_pipe.py`, but contains its own specific functionality to launch new jobs using the PSRDB database.

```
usage: db_specified_launcher.py [-h] [-utc1 UTC1] [-utc2 UTC2] [-psr PULSAR]
                                [-obs_pid PID] [-list_out LIST_OUT]
                                [-list_in LIST_IN] [-runas RUNAS] [-slurm]
                                [-unprocessed] [-job_limit JOBLIMIT]
                                [-forceram FORCERAM] [-forcetime FORCETIME]
                                [-errorlog ERRORLOG] [-testrun]
                                [-obs_id OBSID]

Launches specific observations to be processed by MeerPipe. Provide either a set of searchable parameters or a list of observations. If both inputs are provided, the provided search parameters will be used to filter the entries provided in the list.

optional arguments:
  -h, --help            show this help message and exit
  -utc1 UTC1            Start UTC for PSRDB search - returns only observations after this UTC timestamp.
  -utc2 UTC2            End UTC for PSRDB search - returns only observations before this UTC timestamp.
  -psr PULSAR           Pulsar name for PSRDB search - returns only observations with this pulsar name. If not provided, returns all pulsars.
  -obs_pid PID          Project ID for PSRDB search - return only observations matching this Project ID. If not provided, returns all observations.
  -list_out LIST_OUT    Output file name to write the list of observations submitted by this particular search. Does not work in secondary mode as it would simply duplicate the input list.
  -list_in LIST_IN      List of observations to process, given in standard format. These will be crossmatched against PSRDB before job submission. List format must be:
                        * Column 1 - Pulsar name
                        * Column 2 - UTC
                        * Column 3 - Observation PID
                        Trailing columns may be left out if needed, but at a minimum the pulsar name must be provided.
  -runas RUNAS          Specify an override pipeline to use in processing the observations.
                        Options:
                        'PIPE' - launch each observation through multiple pipelines as defined by the 'launches' PSRDB table (default).
                        'OBS' - use the observation PID to define pipeline selection.
                        <int> - specify a specific PSRDB pipeline ID.
                        <pid> - specify a MeerTIME project code (e.g. 'PTA', 'RelBin'), which will launch a default pipeline.
  -slurm                Processes all jobs using the OzStar Slurm queue.
  -unprocessed          Launch only those observations which have not yet been processed by the specified pipelines.
  -job_limit JOBLIMIT   Max number of jobs to accept to the queue at any given time - script will wait and monitor for queue to reduce below this number before sending more.
  -forceram FORCERAM    Specify RAM to use for job execution (GB). Recommended only for single-job launches.
  -forcetime FORCETIME  Specify time to use for job execution (HH:MM:SS). Recommended only for single-job launches.
  -errorlog ERRORLOG    File to store information on any failed launches for later debugging.
  -testrun              Toggles test mode - jobs will not actually be launched.
  -obs_id OBSID         Specify a single PSRDB observation ID to be processed. Observation must also be either specified via UTC range or list input. Typically only for use by real-time launch script
```

The `-utc1`, `-utc2`, `-psr` and `obs_pid` flags will be used to construct a query for PSRDB. All observations matching these criteria will be processed. Alternatively, a `query_obs.py` format observation list can be provided by `-list_in`, but its content will be cross-matched against PSRDB to ensure all is correct before the relevant jobs are launched. A `query_obs.py` style list of the processed observations can also be written to disk with the `list_out` flag.

The `-runas` flag can be used to specify which pipeline configurations to use when processing. If a project's shorthand identifier (PTA, RelBin, etc.) is provided, all observations will be processed against the default pipeline configuration using the ID linked to that code in `db_utils.py`. Each entry in _pipelines_ gives the location of the configuration file used to describe the custom processing for that project. Alternatively, the ID of the relevant entry in _pipelines_ can be given directly. Specifying `-runas PIPE` will check the pulsar being processed against the _launches_ table of PSRDB to determine which pipelines should be launched; each will then be processed in turn. Specifying `-runas OBS` will use the project the observation was recorded under in its pipeline selection.

The remaining flags are reasonably self-explanatory with reference to the provided help menu. An example run instruction might look like:

`python db_specified_launcher.py -utc1 2022-08-01-00:00:00 -utc2 2022-09-01-00:00:00 -psr J1535-5848 -runas TPA -slurm -unprocessed`

This would process all observations of PSR J1535-5848 from the month of August 2022 through the TPA pipeline configuration, using the OzStar SLURM HPC queue system, but only if they had not been previously processed. If you wish to overwrite the results of previous processings, do not use the `-unprocessed` flag.

**Note:** The "forDB" code can still be run in the same way as the "main" branch, without using `db_specified_launcher.py` and without turning on any of the PSRDB functionality. Launched jobs will not be checked against PSRDB for correctness, results will not be written to PSRDB and the user will need to specify their own configuration file. This may be beneficial, as the "forDB" branch contains a number of minor bugfixes and quality of life improvements not yet migrated to the "main" branch. For further details, refer to the README file from the "main" branch.

## Further information

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
