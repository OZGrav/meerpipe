# Pipeline workflow

The following describes the steps that each observations launched by the pipeline takes.

## Get archives and metadata

The pipeline uses `psrdb.py` to get the metadata of an observation including, the UTC start time, the pulsar PSRJ name, the project short code (PTA, RelBin, TPA or GC), the beam number and the centre frequency.
This information is used to find the location of the archive files on OzSTAR:

```
/fred/oz005/timing/<pulsar_jname>/<UTC_start>/<beam_number>/<centre_frequency_MHz>/
```

These archives come out of the MeerKat pulsar backend (the Pulsar Timing User Supplied Equipment (PTUSE) [Bailes et al. 2020](https://ui.adsabs.harvard.edu/abs/2020PASA...37...28B/abstract)) as individual eight second archive files.
We use the `psradd` command (a `psrchive` script) to combine these archives into a single archive for further processing.

## Calibrate

We calibrate the polarisation using the  `pac` command (a `psrchive` script).


## Clean

The archives are cleaned of RFI using the command `clean_archive.py` (a `MeerGuard` script).
The cleaning is described as:
The surgical cleaner reads in a template, which is subtracted from the data to form profile residuals.
The template can be frequency-dependent if required (e.g. if there is substantial profile evolution) and is used to identify an off-pulse region.
The statistics used by the surgical cleaner are calculated only using this off-pulse region.


## Flux calibrate

The flux calibration is performed by the `fluxcal.py` command (a `meerpipe` script).
It adjusts the archive units so they are in flux units by multiplying them by the expected RMS and dividing them by the observed RMS.
The observed RMS is the median RMS of the off pulse values of the centre channels (1383-1400 MHz for LBAND and 795-805 MHz for UHF band).
The expected RMS ({math}`\mathrm{RMS}_{exp}`) using the expected sky temperatures from (TODO SAY WHERE THEY ARE FROM)

```{math}
\mathrm{RMS}_{exp} = \frac{\mathrm{SEFD}+T_{\mathrm{sky}}}{ N_{\mathrm{ant}} \sqrt{2 \times \nu_{\mathrm{BW}}/N_{\mathrm{chan}} \times t_{\mathrm{obs}}/N_{\mathrm{bin}}}}
```
Where {math}`\mathrm{SEFD}` is the known system equivalent flux density for a single dish, {math}`T_{\mathrm{sky}}` is the expected sky temperature, {math}`N_{\mathrm{ant}}` is the number of antenna, {math}`\nu_{\mathrm{BW}}` is the frequency bandwidth, {math}`N_{\mathrm{chan}}` is the number of frequency channels, {math}`t_{\mathrm{obs}}` is the length of the observation and {math}`N_{\mathrm{bin}}` is the number of phase bins in the pulse profile.

## Create TOAs


## Make images

### Make dynamic spectrum


## Upload