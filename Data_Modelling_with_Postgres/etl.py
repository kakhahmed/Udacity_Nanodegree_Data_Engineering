import glob
import os

import pandas as pd
import psycopg2

from sql_queries import *
import pdb

def process_song_file(cur, filepath):
    """Insert song and artist data from json song file into
    song and artist postgreSQL tables.

    Args:
        cur (psycopg2.extensions.cursor): postgreSQL cursor object.
        filepath (str): json file path.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[[
        "song_id", "title", "artist_id", "year", "duration"]].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[
        ["artist_id", "artist_name", "artist_location",
         "artist_latitude", "artist_longitude"]].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Transform and Insert time data from json song file into
    song and artist postgreSQL tables.

    Args:
        cur (psycopg2.extensions.cursor): postgreSQL cursor object.
        filepath (str): json file path.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'])
    
    # insert time data records
    time_data = [
        (pd.to_datetime(ts, unit='ms'), h, d, w, m, y, wd) 
        for ts, h, d, w, m, y, wd in zip(
            df["ts"], t.dt.month, t.dt.hour, t.dt.day,
            t.dt.weekofyear, t.dt.year, t.dt.weekday
        )
    ]

    column_labels = (
        "timestamp", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            pd.to_datetime(row.ts, unit='ms'), row.userId,
            row.level, songid, artistid, row.sessionId,
            row.location, row.userAgent,
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Use input process function on indicated filepath to process all
    json files within on the connected database.

    Args:
        cur (psycopg2.extensions.cursor): postgreSQL cursor object.
        conn (psycopg2.extensions.connection): PostgreSQL database connection.
        filepath (str): The path for json files to process.
        func (func): The process function to run.
    """
    # get all files matching extension from directory
    all_files = []
    for root, _, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
