#----------
# Parte 4.b
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
    
    
    #Start Question 2
    dStream.foreachRDD(p1)
    #End Question 2
    
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
        rdd=rdd.map(lambda x:x.upper()).filter(lambda tweet:tweet!="A" and tweet!="AN" and tweet!="AND" and tweet!="ARE" and tweet!="AS" and tweet!="AT" and tweet!="BE" and \
        tweet!="BY" and tweet!="FOR" and tweet!="FROM" and tweet!="HAS" and tweet!="HE" and tweet!="IN" and tweet!="IS" and tweet!="IT" and \
        tweet!="ITS" and tweet!="OF" and tweet!="ON" and tweet!="THAT" and tweet!="THE" and tweet!="TO" and tweet!="WAS" and tweet!="WERE" and \
        tweet!="WILL" and tweet!="WITH" and tweet!="HIS" and tweet!="NOT" and tweet!="THIS" and tweet!="WHAT" and tweet!="ABOUT" and tweet!="ES" and \
        tweet!="SON" and tweet!="PARA" and tweet!="DE" and tweet!="FUE" and tweet!="ACERCA" and tweet!="NO" and tweet!="RT" and tweet!="YOU" and tweet!="ME" and \
        tweet!="I" and tweet!="EN" and tweet!="ME" and tweet!="QUE" and tweet!="EL" and tweet!="LA" and tweet!="WHO" and tweet!="MY" and tweet!="I" and tweet!="Y" and \
        tweet!="O" and tweet!="-" and tweet!="YOUR" and tweet!="SE" and tweet!="DEL" and tweet!="ALL" and tweet!="SO")
        spark = getSparkSessionInstance(rdd.context.getConf())
        #for r in rdd.collect():
        #  print(r)
        # Convert RDD[String] to RDD[Row] to DataFrame
        hashtagsDataFrame = spark.createDataFrame(rdd.map(lambda x: Row(keyword=x, date=time)))
        hashtagsDataFrame.createOrReplaceTempView("keywordtags")
        hashtagsDataFrame = spark.sql("select keyword, count(*) as total, date from keywordtags group by keyword, date order by total desc limit 10")
        hashtagsDataFrame.write.mode("append").saveAsTable("keywordtag2")
        #ds=spark.sql("select keyword, sum(total) as suma from keywordtag group by keyword order by suma desc limit 10")
        #ds.show()
        #hashtagsDataFrame.show()
        
    #for record in records:
        #if(record["entities"]["hashtags"]["text"] in record):
        #    print("has hashtags")
        #else:
        #    print("has not hashtags")

if __name__ == "__main__":
    sc = SparkContext(appName="Consumer 2")
    consumer()
    
