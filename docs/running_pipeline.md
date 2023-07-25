# How to launch the pipeline

## Picking what observations to run


Say for example you wanted to rerun some observations for pulsar J1811-2405 and you wanted to work out which observations are available.
You can do this using `psrdb.py observations list` like so

```
psrdb.py observations list --utcstart_gte 2023-04-01T00:00:00 --utcstart_lte 2023-05-01T00:00:00 --target_name J1811-2405
```
which will output
```
id      target_name     calibration_location    telescope_name  instrumentConfig_name   project_code    utcStart        duration        nant    nantEff suspect comment
47257   J1811-2405      None    MeerKAT PTUSE   SCI-20180516-MB-05      2023-04-15T02:49:42+00:00       255.494131      63      63      False
48004   J1811-2405      None    MeerKAT PTUSE   SCI-20180516-MB-05      2023-04-22T02:34:13+00:00       255.494131      63      63      False
48033   J1811-2405      None    MeerKAT PTUSE   SCI-20180516-MB-05      2023-04-29T01:50:06+00:00       255.494131      62      62      False
48034   J1811-2405      None    MeerKAT PTUSE   SCI-20180516-MB-05      2023-04-29T02:20:49+00:00       255.575686      62      62      False
```

You know have a rough idea of the observations you will run if you used the above pulsar and UTC start and end times.
If you only wanted to use one observation just use the `utcStart` as both the UTC start and end time like I will in the next example.


## Making your own configuration


If you don't want to write the pipeline outputs to the main directory while you are testing,
you can create your on config to use while testing.

```
psrdb.py pipelines create <name> <description> <revision> <created_at> <created_by> <configuration>
```

where the configuration should be a json in the format:
```
{
    "pid": "SCI-20180516-MB-05", # project ID
    "path": "/path/to/meerpipe/run_pipe.py", # Path to software
    "config": "/path/to/meerpipe/test_config.cfg" # Path to configuration file
}
```


## Running the pipeline

Once you have decided on the observations to run you can launch the jobs with a command like so:
```
python db_specified_launcher.py -utc1 <utc_start> -utc2 <utc_end> -psr <pulsar_jname> -runas PTA -slurm
```
e.g.:
```
python db_specified_launcher.py -utc1 2023-04-01-00:00:00 -utc2 2023-05-01-00:00:00 -psr J1811-2405 -runas PTA -slurm
```

This command tells MeerPipe to launch jobs for all observations of J1811-2405 between April 1st and May 1st, 2023.
Jobs should be processed under the PTA project specification, and should run on the SLURM queue (allowing for multiple parallel jobs to run) rather than on the host node (for which each job would complete sequentially).

The steps that the launched pipeline will take are described in [pipeline workflow](pipeline_workflow.md#pipeline-workflow)

## Inspecting the pipeline

The pipeline run will be output to

```
/fred/oz005/timing_processed/<project>/<beam_number>/<centre_freq_MHz>
```

In this directory you can find a log file which you can inspect to find if any warnings or errors occurred.

For large scale check of processing stats you can use the command:

```
python misc_scripts/jobstate_query.py -outdir monitoring/ -outfile monitoring.dat -allStates
```

This instruction queries PSRDB for the status of every MeerPipe processing in the database.
These are broken down and written to separate files according to their Job State (Configuring / Pending / Running / Complete / Failure / Crashed) so that problems can be identified and resolved.
Running this script daily when new processing has been completed is a good idea, particularly if changes to MeerPipe have recently been rolled out.

WARNING: Not all problems are detectable via this technique, as it is only meant to catch significant problems. This will only identify if a job has outright crashed due to a code error, or if the state of a job that has ended (Complete vs. Failure) has changed. But it is a useful port of call to check if there immediate problems.

## Finding a failed pipeline run based on SLURM ID

If you have included your email address in the configuration file, if a SLURM job failes it will send you an email which includes the SLURM job ID and pulsar name.
To get the observations from this informations, use the command:

```
psrdb.py processedobservations list --target_name <psr> | grep <slurm ID>
```

The UTC comes from the filepath of the processing (e.g. 2020-03-08-12:34:56), the pulsar name is the J name, and the pipe is PTA / RelBin / TPA / GC.
With the output information you can run:

```
python db_specified_launcher.py -utc1 <utc> -utc2 <utc> -psr <psr> -runas <pipe> -slurm
```

