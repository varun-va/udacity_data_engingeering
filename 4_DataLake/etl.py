import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,monotonically_increasing_id, desc,row_number
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,dayofweek
from pyspark.sql.types import TimestampType, DateType, IntegerType
from pyspark.sql.window import Window

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['CREDENTIALS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['CREDENTIALS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Create or retrieve a Spark Session.
        Creates SparkSession and returns it. If SparkSession is already created it returns
        the currently running SparkSession.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description:
        Process the songs data files and create extract songs table and artist table data from it.
    :param spark: a spark session instance
    :param input_data: input file path
    :param output_data: output file path
    """

    print("--- Starting Process Song_Data ---")

    # get filepath to song data file
    #song_data = input_data + 'song_data/*/*/*/*.json'
    song_data = "data/song-data/song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data,mode = 'PERMISSIVE', columnNameOfCorruptRecord='corrput_record').drop_duplicates()
    
    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").drop_duplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, 'songs'),mode="overwrite", partitionBy=['year', 'artist_id'])
    print("(1/2) | songs.parquet completed")

    # extract columns to create artists table
    artists_table = df.select("artist_id",col("artist_name").alias("name"),col("artist_location").alias("location")\
,col("artist_latitude").alias("latittude"),col("artist_longitude").alias("longitude")).drop_duplicates()

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'),mode="overwrite")
    print("(2/2) | artists.parquet completed")

    print("--- Completed Process Song_Data ---\n\n")


def process_log_data(spark, input_data, output_data):
    """
    Description:
        Processing song_data from S3 to local directory.
        Creates dimension tables "users" and "time" and also the fact table "songplays"

    :param spark: a spark session instance
    :param input_data: input file path
    :param output_data: output file path
    """

    print("--- Starting Process Log_Data ---")

    # get filepath to log data file
    #log_data = os.path.join(input_data, 'log_data', '*', '*')
    log_data ="data/log-data/*.json"

    # read log data file
    df = spark.read.json(log_data)
    df = df.withColumn('user_id', df.userId.cast(IntegerType()))

    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table
    # keep only last user record for every user_id to capture dimension changes over time
    users_table = df.selectExpr(['user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level', 'ts'])
    users_window = Window.partitionBy('user_id').orderBy(desc('ts'))
    users_table = users_table.withColumn('row_number', row_number().over(users_window))
    users_table = users_table.where(users_table.row_number == 1).drop('ts', 'row_number')

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'),mode="overwrite")
    print("(1/3) | users.parquet completed")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts/1000).isoformat())
    df = df.withColumn('start_time', get_timestamp('ts').cast(TimestampType()))

    # extract columns to create time table
    time_table = df.select('start_time')
    time_table = time_table.withColumn('hour', hour('start_time'))
    time_table = time_table.withColumn('day', dayofmonth('start_time'))
    time_table = time_table.withColumn('week', weekofyear('start_time'))
    time_table = time_table.withColumn('month', month('start_time'))
    time_table = time_table.withColumn('year', year('start_time'))
    time_table = time_table.withColumn('weekday', dayofweek('start_time'))

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, 'time'), mode="overwrite",partitionBy=['year', 'month'])
    print("(2/3) | time.parquet completed ")

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, 'song-data/song_data', '*', '*', '*'))

    # extract columns from joined song and log datasets to create songplays table
    df = df.orderBy('ts')
    df = df.withColumn('songplay_id', monotonically_increasing_id())
    song_df.createOrReplaceTempView('songs')
    df.createOrReplaceTempView('events')

    # include year and month to allow parquet partitioning
    songplays_table = spark.sql("""
        SELECT
            e.songplay_id,
            e.start_time,
            e.user_id,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionId as session_id,
            e.location,
            e.userAgent as user_agent,
            year(e.start_time) as year,
            month(e.start_time) as month
        FROM events e
        LEFT JOIN songs s ON
            e.song = s.title AND
            e.artist = s.artist_name AND
            ABS(e.length - s.duration) < 2
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays'), mode="overwrite",partitionBy=['year', 'month'])
    print("(3/3) | songplays.parquet completed")
    print("--- Completed Process Log_Data ---")

def main():
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    input_data ="data/"
    output_data = "results"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)
    spark.stop()

if __name__ == "__main__":
    main()
