#----------
# Parte 4.a
#----------
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar pyspark-shell'
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer
import sys
from pyspark.sql import Row, SparkSession
try:
    import json
except ImportError:
    import simplejson as json


def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config(conf=sparkConf).enableHiveSupport().getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def consumer():
    #context = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext)
    context = StreamingContext(sc, 10)
    dStream = KafkaUtils.createDirectStream(context, ["twitter2"], {"metadata.broker.list": "localhost:9092"})
    
    
    #Start Question 1
    dStream.foreachRDD(p1)
    #End Question 1
    
    context.start()
    context.awaitTermination()

def p1(time,rdd):
    #remove field [0] -> none and convert data str in dict
    rdd=rdd.map(lambda x: json.loads(x[1]))
    records=rdd.collect()  
    records = [element["entities"]["hashtags"] for element in records if "entities" in element] #select only hashtags part
    records = [x for x in records if x] #remove empty hashtags
    records = [element[0]["text"] for element in records]
    if records:
        rdd = sc.parallelize(records)
        #rdd.map(lambda x : (x,1)).reduceByKey(lambda x,y: x+y)
        
        spark = getSparkSessionInstance(rdd.context.getConf())
        # Convert RDD[String] to RDD[Row] to DataFrame
        hashtagsDataFrame = spark.createDataFrame(rdd.map(lambda x: Row(hashtag=x, date=time)))
        hashtagsDataFrame.createOrReplaceTempView("hashtags")
        hashtagsDataFrame = spark.sql("select hashtag, count(*) as total, date from hashtags group by hashtag, date order by total desc limit 10")
        hashtagsDataFrame.write.mode("append").saveAsTable("hashtag2")
        #ds=spark.sql("select hashtag, sum(total) as suma from hashtagq1 group by hashtag order by suma desc limit 5")
        #ds.show()
        #hashtagsDataFrame.show()
    

    #for record in records:
        #if(record["entities"]["hashtags"]["text"] in record):
        #    print("has hashtags")
        #else:
        #    print("has not hashtags")

if __name__ == "__main__":
    sc = SparkContext(appName="Consumer 1")
    consumer()
    
