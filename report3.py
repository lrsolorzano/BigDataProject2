#-------------
# Pregunta 4.c
#-------------
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import sys

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionInstance' not in globals()):
        globals()['sparkSessionInstance'] = SparkSession.builder.config(conf=sparkConf) \
                                            .enableHiveSupport().getOrCreate()
    return globals()['sparkSessionInstance']

def visualization():
    spark = getSparkSessionInstance(sc.getConf())
    date, time = sys.argv[1:]
    datetime=date+" "+time
    query="select screen_name, sum(total) as suma from screen_nametag where \
            date between cast('{}' as timestamp)- INTERVAL 1 DAY and cast('{}' as timestamp) \
            group by screen_name order by suma desc limit 10".format(datetime, datetime)
    ds=spark.sql(query)    
    df = ds.toPandas()
    pie=plt.pie( df['suma'],labels=df['screen_name'],shadow=False, startangle=90,autopct='%1.1f%%')
    df['legend']=df.screen_name.astype(str).str.cat(df.suma.astype(str), sep=':     ')
    plt.legend(labels=df['legend'], loc="upper right")
    plt.axis('equal')
    plt.tight_layout()
    plt.show()      
    #actualFigure = plt.figure(figsize = (16,10))
    #actualFigure.suptitle("Twitter Report 4", fontsize = 16)

if __name__ == "__main__":
    sc = SparkContext(appName="Twitter Report 3")
    visualization()
