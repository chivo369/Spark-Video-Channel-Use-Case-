#Spark UseCase Solution

from __future__ import print_function
from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession,Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.readwriter import DataFrameWriter
from pyspark import SparkContext

if __name__ == "__main__":
    warehouse_location = abspath('spark-warehouse')
    
    spark = SparkSession.builder.appName("You Tube Data Loading").\
    config("spark.sql.warehouse.dir", warehouse_location).\
    enableHiveSupport().getOrCreate()

    # Data filtering stage

    # Here the youtube data set is read into df using the read function
    # Columns are delimited on tabs 
    df=spark.read.format("com.databricks.spark.csv").option("delimiter","\t").\
        option("header","false").load("/home/tom/youtubedata.txt")
    # Initial df contained 23 columns ,On that _c9 to _c22 are related videos
    # They are concatenated to a single column _c9
    df=df.select("_c0","_c1","_c2","_c3","_c4","_c5","_c6","_c7","_c8",\
                  concat("_c9","_c10","_c11","_c12","_c13","_c14","_c15","_c16","_c17","_c18","_c19","_c20","_c21","_c22",).\
                 alias('_c9'))
    # Each columns in df is reanamed and type cast is applied 
    data = df.select(col("_c0").alias("videoid"),col("_c1").alias("uploader"),\
                      col("_c2").alias("time_interval").cast("int"),col("_c3").\
                      alias("category"),col("_c4").alias("length").cast("int"),\
                      col("_c5").alias("view").cast("int"),col("_c6").\
                      alias("rating").cast("float"),col("_c7").alias("ratingnum").\
                      cast("int"),col("_c8").alias("comments").cast("int"),\
                      col("_c9").alias("related"))




    # New data frame data is saved to the hive table location

    data.write.mode('overwrite').format("parquet").save("hdfs://localhost/home/tom/videodata")
    
    spark.stop()
