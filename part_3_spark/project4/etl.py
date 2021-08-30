import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, desc, first, from_unixtime, to_timestamp, hour, year, month, dayofmonth, weekofyear, dayofweek, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Creates the spark session."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Processes the song data.

    The song data is read from S3, two tables (songs_table and artist_table) are generated from the data
    and saved to S3."""
    # read song data file
    df = spark.read.json(f"{input_data}/song_data/A/A/*")

    # extract columns to create songs table
    songs_table = df.select(
        "song_id",
        "title",
        "artist_id",
        "year",
        "duration"
    ).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(f"{output_data}/songs_table.parquet", mode='overwrite')

    # extract columns to create artists table
    artists_table = df.select(
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude"
    ).distinct()
    
    # write artists table to parquet files
    artists_table.write.parquet(f"{output_data}/artists_table.parquet", mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """Processes the log data.

    The log data is read from S3, three tables (users_table, time_table and songplays_table) are generated from the data
    and saved to S3."""
    # read log data file
    df = spark.read.json(f"{input_data}/log_data/2018/11/2018-11-0*")
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # calculates start_time from unixtime in milliseconds
    df = df.withColumn("start_time", to_timestamp(from_unixtime(df.ts / 1000)))

    # extract columns for users table    
    w = Window().partitionBy("userId").orderBy(desc("ts"))
    users_table = df.select(
        col("userId").cast(IntegerType()),
        "firstName",
        "lastName",
        "gender",
        first("level").over(w).alias("level")
    ).distinct()
    
    # write users table to parquet files
    users_table.write.parquet(f"{output_data}/users_table.parquet", mode='overwrite')

    # extract columns to create time table
    time_table = df.select(
        "start_time",
        hour(col("start_time")).alias("hour"),
        dayofmonth(col("start_time")).alias("day"),
        weekofyear(col("start_time")).alias("week"),
        month(col("start_time")).alias("month"),
        year(col("start_time")).alias("year"),
        dayofweek(col("start_time")).alias("weekday"),
    ).distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(f"{output_data}/time_table.parquet", mode='overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(f"{input_data}/song_data/A/A/*")

    # extract columns from joined song and log datasets to create songplays table
    join_condition = [
        df.song == song_df.title,
        df.artist == song_df.artist_name,
        df.length == song_df.duration,
    ]
    songplays_table = df.join(song_df, join_condition, 'left').select(
        monotonically_increasing_id().alias("songplay_id"),
        "start_time",
        col("userId").cast(IntegerType()),
        "level",
        "song_id",
        "artist_id",
        "sessionId",
        "location",
        "userAgent",
        month(col("start_time")).alias("month"),
        year(col("start_time")).alias("year")
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(f"{output_data}/songplays_table.parquet", mode='overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "s3a://as-udacity-3"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
