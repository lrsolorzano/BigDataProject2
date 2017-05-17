#----------
# Parte 4.c
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
    dStream = KafkaUtils.createDirectStream(context, ["twitter1"], {"metadata.broker.list": "localhost:9092"})
    
    
    #Start Question 3
    dStream.foreachRDD(p1)
    #End Question 3
    
    context.start()
    context.awaitTermination()

def p1(time,rdd):
    #remove field [0] -> none and convert data str in dict
    rdd=rdd.map(lambda x: json.loads(x[1]))
    records=rdd.collect()  
    records = [element["user"]["screen_name"] for element in records if "text" in element] #select only screen name

    if records:
        rdd = sc.parallelize(records)
        #rdd.map(lambda x : (x,1)).reduceByKey(lambda x,y: x+y)
        spark = getSparkSessionInstance(rdd.context.getConf())
        # Convert RDD[String] to RDD[Row] to DataFrame
        hashtagsDataFrame = spark.createDataFrame(rdd.map(lambda x: Row(screen_name=x, date=time)))
        hashtagsDataFrame.createOrReplaceTempView("screen_names")
        hashtagsDataFrame = spark.sql("select screen_name, count(*) as total, date from screen_names group by screen_name, date order by total desc limit 10")
        hashtagsDataFrame.write.mode("append").saveAsTable("screen_nametag")
        #ds=spark.sql("select screen_name, sum(total) as suma from screen_nametag group by screen_name order by suma desc limit 10")
        #ds.show()
        #hashtagsDataFrame.show()
        
    #for record in records:
        #if(record["entities"]["hashtags"]["text"] in record):
        #    print("has hashtags")
        #else:
        #    print("has not hashtags")

if __name__ == "__main__":
    sc = SparkContext(appName="Consumer 3")
    consumer()
    
