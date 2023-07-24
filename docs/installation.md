# Installation




## Dependencies

### Python

The following dependencies are required by `meerpipe` and can easily be installed with a python dependency manger such as `pip`

```
numpy
scipy
astropy
pandas
matplotlib
```

### Pulsar software

The following pulsar software packages are required:

```
sigpyproc
calceph
psrxml
psrdada
psrchive
dspsr
fftw
presto
psr_cfitsio
psrcat
dedisp
sigproc
tempo
tempo2
```

[This guide](https://ozgrav.github.io/meerkat_pulsar_docs/software_installation/) explains how to install most of the above software

### psrdb

Install PSRDB to interact with the MeerKAT database.
```
git clone git@github.com:gravitationalwavedc/meertime_dataportal.git psrdb
cd psrdb/backend/cli
poetry install
```

To use the database you need a download token to read it or an ingest token to read and write/upload to it.

To get a read token run
```
get_token.sh
```

To get an ingest token run
```
get_ingest_token.sh
```

The output token should be set as an environment variable using
```
export PSRDB_TOKEN=tokenhere
```

### scintools

SCINTOOLS (SCINtillation TOOLS) is a package for the analysis and simulation of pulsar scintillation data. This code can be used for: processing observed dynamic spectra, computing secondary spectra and ACFs, measuring scintillation arcs, simulating dynamic spectra, and modelling pulsar transverse velocities through scintillation arcs or diffractive timescales.

It can be downloaded from https://github.com/danielreardon/scintools and installed with `pip install .`


### MeerGuard

The MeerTime copy of coast_guard: https://github.com/plazar/coast_guard

The code has been stripped for only RFI excision, and modified for use on wide-bandwidth data.

The surgical cleaner can now read in a template, which it subtracts from the data to form profile residuals. The template can be frequency-dependent if required (e.g. if there is substantial profile evolution) and is used to identify an off-pulse region. The statistics used by the surgical cleaner are calculated only using this off-pulse region.

It can be downloaded from https://github.com/danielreardon/MeerGuard and installed with `pip install .`