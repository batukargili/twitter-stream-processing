from pyspark.sql import SparkSession
from pyspark.sql.functions import window, collect_list, regexp_replace, lit
from config import app_name, spark_loglevel, tcp_host, tcp_port, mongo_database, mongo_collection, covid_source_url
from logger import logger
from webscrapper import get_covid_count


def create_spark_session():
    """
    Creates and returns SparkSession for Structured Streaming, get additional connection conf for mongo sink.

    Returns
    -------
    SparkSession object
    """
    mongodb_output_uri = "mongodb://" + tcp_host + ":" + str(tcp_port) + "/" + mongo_database + "." + mongo_collection
    spark_session = SparkSession \
        .builder \
        .appName(app_name) \
        .config("spark.mongodb.output.uri", mongodb_output_uri) \
        .getOrCreate()
    spark_session.sparkContext.setLogLevel(spark_loglevel)
    logger.info(f"""SparkSession Object created Successfully""")
    return spark_session


def listen_tcp_server(spark_session):
    """
    listens socket with given host and port for new tweets and return streaming dataframe with timestamp column

    Returns
    -------
    Streaming Dataframe struct: [value, timestamp]
    """
    tweets = spark_session \
        .readStream \
        .format("socket") \
        .option("host", tcp_host) \
        .option("port", tcp_port) \
        .option("includeTimestamp", True) \
        .load()

    return tweets


def filter_tweets(tweets):
    """
    Removes unwanted chars and urls with regex expressions
    ! chars to remove are static and embedded in the function ('#' and 'RT:') -> Todo: more dynamic solution

    Returns
    -------
    Streaming Dataframe struct: [tweet, timestamp]
    """
    url_regex_to_replace = '((?<=[^a-zA-Z0-9])(?:(http|https)?\:\/\/|[a-zA-Z0-9]{1,}\.{1}|\b)(?:\w{1,}\.{1}){1,5}(?:com|org|edu|gov|uk|net|ca|de|jp|fr|au|us|ru|ch|it|nl|se|no|es|mil|iq|io|ac|ly|ai){1}(?:\/[a-zA-Z0-9]{1,})*)'
    filtered_tweets = tweets.select((tweets.value).alias("tweet"), (tweets.timestamp).alias("timestamp")) \
        .withColumn('tweet', regexp_replace('tweet', '#', '')) \
        .withColumn('tweet', regexp_replace('tweet', 'RT', '')) \
        .withColumn('tweet', regexp_replace('tweet', url_regex_to_replace, ''))

    return filtered_tweets


def groupby_window_tweets(tweets):
    """
    Creates Tumbling Windows for 20 second intervals, use 1 second watermark for delayed streams.
    Collects tweets in the window to content list with agg function after group by operation

    Returns
    -------
    Streaming Dataframe struct: [content[], timestamp]
    """
    buffered_tweets = tweets \
        .withWatermark("timestamp", "1 seconds") \
        .groupBy(window("timestamp", "20 seconds").alias("timestamp")) \
        .agg(collect_list("tweet").alias("content"))

    return buffered_tweets


def sink_tweets(tweets):
    """ Writes stream and send them to foreachBatch function for covid case data merge and mongo sink operation """
    sink_query = tweets \
        .writeStream \
        .outputMode("append") \
        .option("truncate", False) \
        .format("console") \
        .foreachBatch(foreach_batch_processor) \
        .start()
    return sink_query


def foreach_batch_processor(df, epoch_id):
    """
    1. Gets covid cases value with get_covid_count() function
    2. Creates a non-streaming dataframe for each batch with covid case value
    3. Sink last dataframe to mongo writer
    Parameters
    ----------
    df
    epoch_id
    """
    # if batch is not empty execute merge and mongo sink
    try:
        if df.head(1) != []:
            # get covid cases value
            covid_cases = get_covid_count()
            # add covid cases as df column with literal value
            df_with_covid = df.withColumn("covid_cases", lit(covid_cases))
            # write df to mongo
            logger.info(f"""Microbatch DataFrame Processed Successfully""")
            mongo_sink_tweets(df_with_covid)
    except Exception as e:
        logger.info(f"""!!! Microbatch DataFrame Process Failed with Exception: {e}""")



def mongo_sink_tweets(tweets):
    """Append Spark DF to mongo collection"""
    tweets.write \
        .format("mongodb") \
        .mode("append") \
        .option("database", mongo_database) \
        .option("collection", mongo_collection) \
        .save()
    logger.info(f"""New DataFrame appended to MongoDB collection {mongo_database}.{mongo_collection} Successfully""")


if __name__ == '__main__':
    # creates SparkSession for structured streaming
    spark_session = create_spark_session()
    # connects socket and read messages
    tweets = listen_tcp_server(spark_session)
    # remove chars and url values from stream value
    filtered_tweets = filter_tweets(tweets)
    # create windows for 20 second and collect values to a list
    buffered_tweets = groupby_window_tweets(filtered_tweets)
    # merge covid data with stream dataframe and sink to mongo collection
    sink_query = sink_tweets(buffered_tweets)

    sink_query.awaitTermination()
