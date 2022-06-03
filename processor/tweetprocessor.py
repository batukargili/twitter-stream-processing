from bs4 import BeautifulSoup
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, collect_list, regexp_replace, lit

app_name = "tweet-read"
app_loglevel = "ERROR"
tcp_host = "0.0.0.0"
tcp_port = 5555
url_regex_to_replace = '((?<=[^a-zA-Z0-9])(?:(http|https)?\:\/\/|[a-zA-Z0-9]{1,}\.{1}|\b)(?:\w{1,}\.{1}){1,5}(?:com|org|edu|gov|uk|net|ca|de|jp|fr|au|us|ru|ch|it|nl|se|no|es|mil|iq|io|ac|ly|ai){1}(?:\/[a-zA-Z0-9]{1,})*)'

url = "https://www.worldometers.info/coronavirus/"


def get_covid_count():
    """

    Returns
    -------

    """
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')

    corona_cases_value = soup \
        .find("div", attrs={"id": "maincounter-wrap"}) \
        .find("div", attrs={"class": "maincounter-number"}) \
        .find("span").text

    return corona_cases_value


def listen_tcp_server(spark_session):
    """ listen socket for new tweets and return dataframe"""
    tweets = spark_session \
        .readStream \
        .format("socket") \
        .option("host", tcp_host) \
        .option("port", tcp_port) \
        .option("includeTimestamp", True) \
        .load()
    return tweets


def filter_tweets(tweets):
    """ ???? """
    filtered_tweets = tweets.select((tweets.value).alias("tweet"), (tweets.timestamp).alias("timestamp")) \
        .withColumn('tweet', regexp_replace('tweet', '#', '')) \
        .withColumn('tweet', regexp_replace('tweet', 'RT', '')) \
        .withColumn('tweet', regexp_replace('tweet', url_regex_to_replace, ''))
    return filtered_tweets


def groupby_window_tweets(tweets):
    """
    ??????
    Parameters
    ----------
    tweets

    Returns
    -------
    ????
    """
    buffered_tweets = tweets \
        .withWatermark("timestamp", "2 seconds") \
        .groupBy(window("timestamp", "20 seconds").alias("timestamp")) \
        .agg(collect_list("tweet").alias("content"))
    return buffered_tweets


def sink_tweets(tweets):
    """ ??? """
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
    ???
    Parameters
    ----------
    df
    epoch_id

    Returns
    -------
    ??
    """
    # get covid cases
    covid_cases = get_covid_count()
    print("Covid Cases:" + str(covid_cases))
    df_with_covid = df.withColumn("covid_cases", lit(covid_cases))
    # write mongo
    df_with_covid.printSchema()
    mongo_sink = mongo_sink_tweets(df_with_covid)


def mongo_sink_tweets(tweets):
    """ ???? """
    mongo_writer = tweets \
        .write \
        .format("mongodb") \
        .mode("append") \
        .option("database", "test") \
        .option("collection", "tweets") \
        .save()
    return mongo_writer


if __name__ == '__main__':
    """
    Spark Structered Streaming
    Returns
    -------
    """
    spark_session = SparkSession \
        .builder \
        .appName(app_name) \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/test.tweets") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/test.tweets") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.2") \
        .getOrCreate()
    spark_session.sparkContext.setLogLevel(app_loglevel)
    tweets = listen_tcp_server(spark_session)
    filtered_tweets = filter_tweets(tweets)
    buffered_tweets = groupby_window_tweets(filtered_tweets)
    sink_query = sink_tweets(buffered_tweets)
    sink_query.awaitTermination()
