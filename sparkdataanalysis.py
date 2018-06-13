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
    
    spark = SparkSession.builder.appName("You Tube Data Analysis").\
    config("spark.sql.warehouse.dir", warehouse_location).\
    enableHiveSupport().getOrCreate()
    df=spark.read.format("com.databricks.spark.csv").option("delimiter","\t").\
        option("header","false").load("/home/tom/youtubedata.txt")

    df=df.select("_c0","_c1","_c2","_c3","_c4","_c5","_c6","_c7","_c8",\
                  concat("_c9","_c10","_c11","_c12","_c13","_c14","_c15","_c16","_c17","_c18","_c19","_c20","_c21","_c22",).\
                 alias('_c9'))
    
    data = df.select(col("_c0").alias("videoid"),col("_c1").alias("uploader"),\
                      col("_c2").alias("time_interval").cast("int"),col("_c3").\
                      alias("category"),col("_c4").alias("length").cast("int"),\
                      col("_c5").alias("view").cast("int"),col("_c6").\
                      alias("rating").cast("float"),col("_c7").alias("ratingnum").\
                      cast("int"),col("_c8").alias("comments").cast("int"),\
                      col("_c9").alias("related"))

    # question 1
    # grouped the category field and counted them
    a = data.select("category").groupBy("category").count().sort("count",ascending=False).limit(5)
    # column count renamed to num_videos and type casted to integers, the previous case was throwing two error while running the result on hive
    temp = a.select(col("category"),col("count").alias("num_videos").cast("int"))
    temp.show()
    # temp is saved to the hive table location
    temp.write.mode('overwrite').format("parquet").save("hdfs://localhost/home/tom/cat")

    # question 2
    # selected the videoid and rating fields and sorted  them
    
    b = data.select(("videoid"),("rating")).sort("rating",ascending=False).limit(10)
    b.show()
    # b is saved to the hive table location
    b.write.mode('overwrite').format("parquet").save("hdfs://localhost/home/tom/rat")
    spark.stop()
