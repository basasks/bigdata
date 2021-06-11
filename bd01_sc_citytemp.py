###############################################################################################
# PySpark - Monthly Temperatures per City
# Author: BASASKS
# Source: csv file (https://www.kaggle.com/sudalairajkumar/daily-temperature-of-major-cities)
# Transformation: dataframe with aggregated ave, max, min per month, per city
# TGT: Hive table
# Environment: HDP2.6.5, Spark 2
# Usage: spark-submit hd01_sc_avetemp.py
###############################################################################################


##### MODULES

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


##### VARIABLES

file_input = "/user/maria_dev/proj01/citytemp.csv"
path_output = "/user/maria_dev/proj01/output/"


##### MAIN

# CREATE DATA FRAME FROM CSV FILE

spark = SparkSession.builder \
    .appName("CityTempAggregate") \
    .enableHiveSupport() \
    .getOrCreate()

schema = StructType([
    StructField("Region", StringType()),
    StructField("Country", StringType()),
    StructField("State", StringType()),
    StructField("City", StringType()),
    StructField("Month", IntegerType()),
    StructField("Day", IntegerType()),
    StructField("Year", IntegerType()),
    StructField("AvgTemperature", FloatType())
    ])

df1 = spark.read \
    .option("header",True) \
    .option("delimiter",",") \
    .schema(schema) \
    .csv(file_input)
    
# DATAFRAME TRANSFORMATIONS

df2 = df1.withColumn("CityCountry", f.concat(f.col("City"),f.lit(", "),f.col("Country")))

df3 = df2.filter(df2.Region == "Europe") \
    .groupBy("CityCountry","Month") \
    .agg( f.round(f.min("AvgTemperature"),1), f.round(f.max("AvgTemperature"),1), f.round(f.avg("AvgTemperature"),1) ) \
    .sort("CityCountry", "Month") \
    .withColumnRenamed("round(min(AvgTemperature), 1)", "MinTemp") \
    .withColumnRenamed("round(max(AvgTemperature), 1)", "MaxTemp") \
    .withColumnRenamed("round(avg(AvgTemperature), 1)", "AvgTemp") 
    
# SAVE DATAFRAME TO HIVE TABLE

df3.write.mode("overwrite").saveAsTable("proj01.citytempaggregate")

# WRITE HIVE TABLE TO CSV

df4 = spark.sql("SELECT * FROM proj01.citytempaggregate")

df4.coalesce(1).write.mode("overwrite") \
    .option("header",True) \
    .csv(path_output)



