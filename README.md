# Twitter Stream Processing

This project processes Twitter's live tweat stream, merges covid cases with tweet batches and sink data to MongoDB with Spark Structured Streaming.

## Requirements

| Software      | Version                                       |
|---------------|---------------------------------------------- |
| Python        | 3.8.6                                         |
| Spark         | 3.2.1                                         |
| MongoDB       | 4.0.0                                         |

## Installation
### Apache Spark
 > Mac: https://medium.com/beeranddiapers/installing-apache-spark-on-mac-os-ce416007d79f
 
 > Windows: https://phoenixnap.com/kb/install-spark-on-windows-10
 
 > Ubuntu:  https://phoenixnap.com/kb/install-spark-on-ubuntu

### MongoDB
    docker pull mongo

    docker run -d  --name mongodb -p 27017:27017 mongo

    docker exec -it mongodb bash

    mongo 

    use ultimate-demo

## Streaming Flow
1. **listen_tcp_server** function creates readStream in Socket format and starts fetching new tweets from socket.
   
2. **filter_tweets** removes '#', 'RT' and URL's from tweet streams.
   
3. **groupby_window_tweets** creates a 20 second tumbling window by timestamp column, aggregates tweets to one content list column and create batches for each window.
   
4. **sink_tweets** gets the buffered batches and send them to foreach function for covid data merge and mongo sink.
   
5. **foreach_batch_processor** scrape covid data from https://www.worldometers.info/coronavirus/ and add as a new column to final dataframe.
   
6. **mongo_sink_tweets** writes dataframe to desired mongoDB collection.

![Alt text](docs/sparkstructuredstream_lineage.png?raw=true "Architecture")


## Running Locally
### Config
**processor_config.json** file under the processor/config module has the config parameters. 

```
{
  "spark_loglevel" : "ERROR",
  "tcp_host" : "0.0.0.0",
  "tcp_port" : 5555,
  "mongo_host" : "127.0.0.1",
  "mongo_port" : "27017",
  "mongo_database" : "ultimate-demo",
  "mongo_collection" : "tweets",
  "covid_source_url" : "https://www.worldometers.info/coronavirus/"
}
```



### Create Environment:

    python3 -m venv pyspark_venv

    source pyspark_venv/bin/activate

    pip install -r requirements.txt

### Run Tweet Producer in a separate terminal:

    python tweetproducer/twitter_stream_simulator.py 

### Run pyspark code with virtual environments python version and mongo packages:  

    PYSPARK_PYTHON=python spark-submit --packages org.mongodb.spark:mongo-spark-connector:10.0.2 processor/tweetprocessor.py localhost 9999




