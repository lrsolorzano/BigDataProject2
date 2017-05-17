#--------
# Parte 5
#--------
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
    
    
    #Start Question 4
    dStream.foreachRDD(p1)
    #End Question 4
    
    context.start()
    context.awaitTermination()

def p1(time,rdd):
    #remove field [0] -> none and convert data str in dict
    rdd=rdd.map(lambda x: json.loads(x[1]))
    records=rdd.collect()  
    records = [element["text"] for element in records if "text" in element] #select only key word

    if records:
        rdd = sc.parallelize(records)
        #rdd.map(lambda x : (x,1)).reduceByKey(lambda x,y: x+y)
        rdd=rdd.flatMap(lambda tweet:tweet.split(" "))
        rdd=rdd.map(lambda x:x.upper()).filter(lambda tweet:tweet=="TRUMP" or tweet=="MAGA" or tweet=="DICTATOR" or tweet=="IMPEACH" or \
        tweet=="DRAIN" or tweet=="SWAMP")
        spark = getSparkSessionInstance(rdd.context.getConf())
        #for r in rdd.collect():
        #  print(r)
        # Convert RDD[String] to RDD[Row] to DataFrame
        hashtagsDataFrame = spark.createDataFrame(rdd.map(lambda x: Row(keyword=x, date=time)))
        hashtagsDataFrame.createOrReplaceTempView("trumpkeywords")
        hashtagsDataFrame = spark.sql("select keyword, count(*) as total, date from trumpkeywords group by keyword, date order by total desc limit 10")
        hashtagsDataFrame.write.mode("append").saveAsTable("trumpkeyword")
        #ds=spark.sql("select keyword, sum(total) as suma from trumpkeyword group by keyword order by suma desc limit 10")
        #ds.show()
        #hashtagsDataFrame.show()
        
    #for record in records:
        #if(record["entities"]["hashtags"]["text"] in record):
        #    print("has hashtags")
        #else:
        #    print("has not hashtags")

if __name__ == "__main__":
    sc = SparkContext(appName="Consumer 4")
    consumer()
    
