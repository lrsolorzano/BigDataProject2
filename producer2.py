#---------------------------------------
# Producer que actualiza cada 10 minutos
#---------------------------------------
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar pyspark-shell'
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer
from operator import add
import sys
import json
from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream

def read_credentials():
    file_name = "/usr/local/credentials.json"
    try:
        with open(file_name) as data_file:
            return json.load(data_file)
    except:
        print ("Cannot load "+data_file)
        return None

def producer1():
    sc = SparkContext(appName="Producer 2")
    ssc = StreamingContext(sc, 600)
    brokers = "localhost:9092"
    kvs = KafkaUtils.createDirectStream(ssc, ["test"], {"metadata.broker.list": brokers})
    kvs.foreachRDD(send)
    producer.flush()
    ssc.start()
    ssc.awaitTermination()

def send(message):
    iterator = twitter_stream.statuses.sample()
    count=0
    for tweet in iterator:
        producer.send('twitter2', bytes(json.dumps(tweet, indent=6), "ascii"))
        count+=1
        if(count==22000):
            break
     
if __name__ == "__main__":
    print("Stating to read tweets")
    credentials = read_credentials() 
    oauth = OAuth(credentials['ACCESS_TOKEN'], credentials['ACCESS_SECRET'], credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
    twitter_stream = TwitterStream(auth=oauth)
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    producer1()
    
