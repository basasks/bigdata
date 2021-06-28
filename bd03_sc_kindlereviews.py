###############################################################################################
# PySpark - Kindle Reviews Data Dump
# Author: BASASKS
# Source: json file (https://www.kaggle.com/bharadwaj6/kindle-reviews)
# Transformation: 
# TGT: Hive Table with Partition
# Environment: HDP2.6.5, Spark 2
# Usage: spark-submit bd02_sc_kindlereviews.py
###############################################################################################


##### MODULES

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring


##### VARIABLES

file_input = "/user/maria_dev/proj03/kindle_reviews.json"


##### MAIN

# CREATE SPARK SESSION

spark = SparkSession.builder \
    .appName("YelpBusiness") \
    .enableHiveSupport() \
    .getOrCreate()

# LOAD JSON AS DF WITH IMPLICIT SCHEMA    

df1 = spark.read.json(file_input)

# UPDATE COLUMN NAMES

df2 = df1.drop("helpful") \
    .drop("unixReviewTime") \
    .withColumnRenamed("asin","bookid") \
    .withColumnRenamed("overall","rating") \
    .withColumnRenamed("reviewText","reviewtext") \
    .withColumnRenamed("reviewTime","reviewtime") \
    .withColumnRenamed("reviewerID","reviewerid") \
    .withColumnRenamed("reviewerName","reviewername") \
    .withColumnRenamed("summary","summary") \
    .withColumn("reviewyear", substring(col("reviewTime"),-4,4).cast("integer"))

# SAVE DATAFRAME TO HIVE TABLE WITH PARTITION

spark.sql("DROP DATABASE IF EXISTS proj03 CASCADE")
spark.sql("CREATE DATABASE proj03 LOCATION '/apps/hive/warehouse/proj03.db'")
df2.write.mode("Append").partitionBy("reviewyear").saveAsTable("proj03.stage_kindlereviews")
