# Data-Collection

## Overview

Each sync script has accepts a `sync` and `test` command and is reponsible for syncing one or more tables for a particular data source.

The output of each sync script is a [Singer](https://www.singer.io/) stream, which can be paired with a Singer target to load into a number of destinations, including postgres. Singer automatically manages table schemas and schema changes.

### Installation

Python 3 and pip need to be installed.

```sh
pip install virtualenv
virtualenv -p python3 env
source env/bin/activate
pip install -r requirements.txt
```

### Extracting Data

The `sync` command performs a sync, extracting data, usually for the current day.

```sh
python path/to/main.py sync
```

### Testing

[TODO: Instructions for testing data]

### Loading Data

[TODO: Intructions on loading data using Singer]
