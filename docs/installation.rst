Installation
============



Dependencies
------------

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