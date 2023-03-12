# Song Play Analysis

ETL using AWS Redshift to create 5 tables S3 staging tables. The database is staged using Redshift and transformed into a star schema optimized for queries on song play analysis. This includes the following tables.

The database schema as follows:
![schema](./ERD.PNG)

Fact Table

    songplays - records in event data associated with song plays i.e. records with page NextSong
        songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables

    users - users in the app
        user_id, first_name, last_name, gender, level
    songs - songs in music database
        song_id, title, artist_id, year, duration
    artists - artists in music database
        artist_id, name, location, lattitude, longitude
    time - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday


## Song Play content

The Redshift database has 5 tables: songplays, users, songs, artists, and time. The 5 tables have data loaded from song_data and log_data datasets residing in JSON files at `data/song_data` and `data/log_data`. The JSON files consist of available song data and user log activity, respectively.

- There are four python modules:

* `redshift_cluster.py`: IoC module to create the needed resources. EC2, IAM role, and Redshift cluster. This module has two options: `--create` to create the resources and `--delete` to delete the resources.

* `sql_queries.py`: It consists of all ETL SQL queries that are used in the Song Play ETL pipeline.
  * Create tables.
  * Copying staging tables.
  * Inserting into dimension and fact tables.

* `create_tables.py`: It is the module to run to create the database tables.

* `etl.py`: It is main ETL module. It copies all the staging tables to insert values in the fact and dimension tables.

Then there is the `requirements.txt` that contains the required packages for this project.

For configuring the AWS resources. `dwh.cfg` is used.

## How to run Song Play ETL

This ETL runs using Python 3.6+. All below steps are done using Linux terminal after loading python3 environment. `us-west-2` region is used in this project.

1. Install all required packages. `pip install -e requirements.txt`

2. Run `python redshift_cluster.py --create` to create all needed resources and redshift cluster.

3. Run ```python create_tables.py``` to create the Postgres database tables.

4. Run ```python etl.py``` to load data from song and load datasets' JSON files into the Postgres optimized tables.

5. At the end you need to clean up all resources by running `python redshift_cluster.py --delete` 
