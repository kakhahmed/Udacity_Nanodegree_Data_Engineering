# Song Play Analysis

ETL for creating and storing data in Postgres database with song and user activity data for Sparkify music streaming app.
The Postgres database tables are designed to optimize analyzing queries on song play analysis.


## Song Play content

The Postgres database has 5 tables: songplays, users, songs, artists, and time. The 5 tables have data loaded from song_data and log_data datasets residing in JSON files at `data/song_data` and `data/log_data`. The JSON files consist of available song data and user log activity, respectively.

There are two Jupyter notebooks:

* `etl.ipynb`: It is used for developing the ETL pipeline.

* `test.ipynb`: It is used for testing the database.

There are three python modules:

* `etl.py`: It is main ETL module.

* `sql_queries.py`: It consists of all ETL SQL queries that are used in the Song Play ETL pipeline.

* `create_tables.py`: It is the module to run to create the database tables.


## How to run Song Play ETL

This ETL runs using Python 3.6+. All below steps are done using linux terminal after loading python3 environment

1. Run ```python create_tables.py``` to create the Postgres database tables.

2. Run ```python etl.py``` to load data from song and load datasets' json files into the Postgres optimized tables.
