## Overview

Meerpipe is a python-based data analysis pipeline that produces the cleaned and calibrated data that forms the basis of all pulsar timing projects with the MeerKAT radio telescope as part of the [MeerTime project](http://www.meertime.org/). For an overview of MeerTime and the various science themes, please refer to [Bailes et al. 2020](https://arxiv.org/abs/2005.14366)  

The meerpipe repository can be found here: https://github.com/aparthas3112/meerpipe

## Functionalities

The raw data recorded by the Pulsar Timing User Supplied Equipment (PTUSE) is transferred to the OzStar supercomputing facility in Melbourne, Australia where meerpipe is hosted. Each new observation automatically triggers an instance of the pipeline that is launched using the Slurm job manager. The pipeline optimizes the requested memory and wall time based on the length of the observation. 

For each observation, meerpipe implements the following routines:

1. Polarisation calibration: The Jones matrices provided by the South African Radio Astronomy Observatory (SARAO) are used by meerpipe to perform polarisation calibration. A detailed description can be found in [Serylak et al. 2020](https://arxiv.org/pdf/2009.05797.pdf)

2. Cleaning: The radio frequency interference in the calibrated data is removed by utilizing a combination of cleaning algorithms that are part of a customised software package, MeerGuard.  

3. Flux calibration: Meerpipe also uses a bootstrap method to flux calibrate the data. Using the sky map in Calabretta et al. 2014, an initial estimate of the Tsky was computed. However owing to the large disparity in the  sky map as comparted to the more resolved MeerKAT beam, a secondary estimate of Tsky was performed directly from the data. This was done by observing a high-latitude pulsar and comparing its RMS with other pulsars observed in that session. This allowed an estimate of a scaling factor used to flux calibrate the data. A detailed description of this method can be found in the TPA census paper. 

4. User-defined data products: Following the requirements laid out by the various science themes within MeerTime, meerpipe produces the requested decimated data products post-cleaning. These include data products at various frequency and time resolutions along with the dynamic spectra for scintillation studies. S

5. Times-of-arrival: Using the decimated data products, the times-of-arrival per observation are computed using [PSRCHIVE](http://psrchive.sourceforge.net/). 


## Database management: 
The "forDB" branch of this repository links the data products and the pipeline to a central database that forms the backend for the online data portal. More information on this can be found in the relevant [README file](https://github.com/aparthas3112/meerpipe/tree/forDB) in the "forDB" branch. 


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

