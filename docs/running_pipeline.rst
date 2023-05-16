How to run the pipeline
=======================

Picking what observations to run
--------------------------------

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


Making your own configuration
-----------------------------

If you don't want to write the pipeline outputs to the main directory while you are testing,
you can create your on config to use while testing.



Running the pipeline
--------------------

```
python db_specified_launcher.py -utc1 2023-04-15-02:49:42 -utc2 2023-04-15-02:49:42 -psr J1811-2405 -runas PTA -slurm
```
