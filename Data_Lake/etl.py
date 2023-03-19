import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import (
    year,
    month,
    monotonically_increasing_id,
    dayofmonth,
    hour,
    weekofyear,
    dayofweek,
    date_format,
)

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['SECRETS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['SECRETS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data, song_data_path):
    # get filepath to song data file
    song_data = input_data + song_data_path
    
    # read song data file
    song_df = spark.read.json(song_data)

    # extract columns to create songs table
    # song_id, title, artist_id, year, duration
    songs_table = song_df.select(
        'song_id', 'title', 'artist_id', 'year', 'duration')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(
        "year", "artist_id").mode("overwrite").parquet(output_data + "/songs")

    # extract columns to create artists table
    # artist_id, name, location, latitude, longitude
    artists_table = song_df.select(
        'artist_id',
        'artist_name',
        'artist_location',
        'artist_latitude',
        'artist_longitude',
    )

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "/artists")


def process_log_data(spark, input_data, output_data, log_data_path, song_data_path):
    # get filepath to log data file
    log_data = input_data + log_data_path

    # read log data file
    log_df = spark.read.json(log_data)

    # filter by actions for song plays
    df = log_df.filter(log_df.page=="NextSong")

    # extract columns for users table 
    # user_id, first_name, last_name, gender, level
    users_table = log_df.select(
        col("userId"),
        col("firstName"),
        col("lastName"),
        col("gender"),
        col("level"),
    )

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "/users")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(int(x)/1000).strftime('%Y-%m-%d %H:%M:%S'))
    log_df = log_df.withColumn('timestamp', get_timestamp(log_df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    log_df = log_df.withColumn('datetime', get_datetime(log_df.ts))

    # extract columns to create time table
    time_table = log_df.withColumn(
        'start_time', col('timestamp')
    ).withColumn(
        'hour', dayofmonth('timestamp')
    ).withColumn(
        'day', dayofmonth('timestamp')
    ).withColumn(
        'week', weekofyear('timestamp')
    ).withColumn(
        'month', month('timestamp')
    ).withColumn(
        'year', year('timestamp')
    ).withColumn(
        'weekday', dayofweek('timestamp')
    )

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(
        "year", "month").mode("overwrite").parquet(output_data + "/time_table")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + song_data_path)

    # extract columns from joined song and log datasets
    # to create songplays table
    log_df = log_df.alias('log_df')
    song_df = song_df.alias('song_df')

    songplays_table = log_df.join(
        song_df, col('log_df.artist') == col('song_df.artist_name')).select(
        col("log_df.datetime").alias("start_time"),
        col("log_df.userId").alias("user_id"),
        col("log_df.level"),
        col("song_df.song_id"),
        col("song_df.artist_id"),
        col("log_df.sessionId").alias("session_id"),
        col("log_df.location").alias("location"),
        col("log_df.userAgent").alias("user_agent"),
    ).withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(
        "year", "month").mode(
        "overwrite").parquet(output_data + "/songplays_table")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://aws-bucket-udacity-de"
    song_data_path = "song_data/A/A/A/*.json"
    process_song_data(
        spark,
        input_data,
        output_data,
        song_data_path=song_data_path,
    )    
    process_log_data(
        spark,
        input_data,
        output_data,
        log_data_path="log_data/*/*/*.json",
        song_data_path=song_data_path,
    )


if __name__ == "__main__":
    main()
