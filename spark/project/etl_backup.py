import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType as St, StructField as Sf, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Dt, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data_files = input_data + "*/*/*/*.json"
    
    # read song data file
    song_data_df = spark.read.json(song_data_files).dropDuplicates()

    # extract columns to create songs table
    songs_columns = ["title", "artist_id", "year", "duration"]
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = song_data_df.select(songs_columns).dropDuplicates().withColumn("song_id", monotonically_increasing_id())
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/')

    # extract columns to create artists table
    artists_columns = ["artist_id", "name", "location", "latitude", "longitude"]
    
    # write artists table to parquet files
    artists_table = song_data_df.selectExpr(artists_columns).dropDuplicates()
    artists_table.write.parquet(output_data + 'artists/')

    song_data_df.createOrReplaceTempView("song_table_tmp_df")

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    # log_data = input_data + "*/*/*.json"
    log_data_files = input_data + "/*.json"

    # read log data file
    log_data_df = spark.read.json(log_data_files)
    
    # filter by actions for song plays
    log_data_df = log_data_df.filter(log_data_df.page == 'NextSong')

    # extract columns for users table    
    users_columns = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = log_data_df.selectExpr(users_columns).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    log_data_df = log_data_df.withColumn("timestamp", get_timestamp(col("ts")))
    
    # create datetime column from original timestamp column
    log_data_df = log_data_df.withColumn("start_time", get_timestamp(col("ts")))
    
    # extract columns to create time table
    log_data_df = log_data_df.withColumn("hour", hour("timestamp"))
    log_data_df = log_data_df.withColumn("day", dayofmonth("timestamp"))
    log_data_df = log_data_df.withColumn("month", month("timestamp"))
    log_data_df = log_data_df.withColumn("year", year("timestamp"))
    log_data_df = log_data_df.withColumn("week", weekofyear("timestamp"))
    log_data_df = log_data_df.withColumn("weekday", dayofweek("timestamp"))

    time_table = log_data_df.select(col("start_time"), col("hour"), col("day"), col("month"), col("year"), col("week"), col("weekday")).distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + 'time/')

    # read in song data to use for songplays table
    song_df = spark.sql("select distinct song_id, artist_id, name from song_table_tmp_df")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = log_data_df.join(song_df, song_df.name == log_data_df.artist, "inner").distinct() \
        .select(col("start_time"), col("userId"), col("level"), col("sessionId"), col("location"), \
            col("userAget"), col("song_id"), col("artist_id")) \
        .withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays_table/')


def main():
    spark = create_spark_session()

    song_input_data = "home/workspace/data/song-data/"
    log_input_data = "home/workspace/data/log-data/"
    output_data = "home/workspace/data/output_data"


    # song_input_data = "s3a://udacity-dend/song-data/"
    # log_input_data = "s3a://udacity-dend/log-data/"

    # output_data = "s3://btdend/output_data"

    process_song_data(spark, song_input_data, output_data)    
    process_log_data(spark, log_input_data, output_data)


if __name__ == "__main__":
    main()
