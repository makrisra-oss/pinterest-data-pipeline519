# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
import urllib

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- # Disable format checks during the reading of Delta tables
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false
# MAGIC
# MAGIC

# COMMAND ----------

df_pin = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0affc56add51-pin') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

display(df_pin)

# COMMAND ----------

df_pin_updated = df_pin.selectExpr("CAST(data as STRING)")

display(df_pin_updated)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import from_json
import pyspark.sql.functions as F

schema_pin = StructType([
    StructField("index", IntegerType(), True),
    StructField("unique_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("poster_name", StringType(), True),
    StructField("follower_count", StringType(), True),
    StructField("tag_list", StringType(), True),
    StructField("is_image_or_video", StringType(), True),
    StructField("image_src", StringType(), True),
    StructField("downloaded", IntegerType(), True),
    StructField("save_location", StringType(), True),
    StructField("category", StringType(), True)
])

mapped_df_pin = df_pin_updated.withColumn("data", from_json(df_pin_updated["data"], schema_pin))\
    .select(col('data.*'))

#Drop all null values
df_pin_cleaned = mapped_df_pin.dropna(how='all')

#Drop duplicates
df_pin_cleaned = df_pin_cleaned.dropDuplicates()

#Replace empty strings and irrelevant data with null
df_pin_cleaned = mapped_df_pin.replace('', None)
df_pin_cleaned = df_pin_cleaned.replace('No description available Story format', None)

#Perform the necessary transformations on the follower_count to ensure every entry is a number. Make sure the data type of this column is an int.

# df_pin = df.withColumn("follower_count_cleaned", 
df_pin_cleaned = df_pin_cleaned.withColumn("follower_count", regexp_replace("follower_count", "k" , "000"))
df_pin_cleaned = df_pin_cleaned.withColumn("follower_count", regexp_replace("follower_count", "K" , "000"))
df_pin_cleaned = df_pin_cleaned.withColumn("follower_count", regexp_replace("follower_count", "m" , "000000"))
df_pin_cleaned = df_pin_cleaned.withColumn("follower_count", regexp_replace("follower_count", "M" , "000000"))

#Cast string to int
df_pin_cleaned = df_pin_cleaned.withColumn("follower_count", df_pin_cleaned["follower_count"].cast("integer"))

#Clean up "No Tags Available"
df_pin_cleaned = df_pin_cleaned.withColumn("tag_list", 
    regexp_replace(col("tag_list"), r"N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e", "No Tags Available")
)

# Ensure that each column containing numeric data has a numeric data type
df_pin_cleaned = df_pin_cleaned.withColumn("downloaded", df_pin_cleaned["downloaded"].cast("integer"))
df_pin_cleaned = df_pin_cleaned.withColumn("index", df_pin_cleaned["index"].cast("integer"))

# Clean the data in the save_location column to include only the save location path

df_pin_cleaned = df_pin_cleaned.withColumn("save_location", regexp_replace("save_location", "Local save in " , ""))

# Rename the index column to ind
df_pin_cleaned = df_pin_cleaned.withColumnRenamed("index", "ind")


# Reorder the DataFrame columns to have the following column order:
# ind
# unique_id
# title
# description
# follower_count
# poster_name
# tag_list
# is_image_or_video
# image_src
# save_location
# category

df_pin_cleaned = df_pin_cleaned.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category")

display(df_pin_cleaned)
# Check data type of a specific column
print(df_pin_cleaned.schema['follower_count'].dataType)
print(df_pin_cleaned.schema['ind'].dataType)


# COMMAND ----------

df_geo = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0affc56add51-geo') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

display(df_geo)

# COMMAND ----------

df_geo_updated = df_geo.selectExpr("CAST(data as STRING)")

display(df_geo_updated)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import from_json
import pyspark.sql.functions as F

schema_geo = StructType([
    StructField("ind", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("country", StringType(), True)
])

mapped_df_geo = df_geo_updated.withColumn("data", from_json(df_geo_updated["data"], schema_geo))\
    .select(col('data.*'))

#Drop all null values
df_geo_cleaned = mapped_df_geo.dropna(how='all')

#Drop duplicates
df_geo_cleaned = df_geo_cleaned.dropDuplicates()

# Cast latitude and longitude to coordinates
df_geo_cleaned = df_geo_cleaned.withColumn("latitude", df_geo_cleaned["latitude"].cast("double"))
df_geo_cleaned = df_geo_cleaned.withColumn("longitude", df_geo_cleaned["longitude"].cast("double"))

# Create a new column coordinates that contains an array based on the latitude and longitude columns

df_geo_cleaned = df_geo_cleaned.withColumn("coordinates", array("latitude", "longitude"))

# Drop the latitude and longitude columns from the DataFrame

df_geo_cleaned = df_geo_cleaned.drop("latitude", "longitude")

# Convert the timestamp column from a string to a timestamp data type

df_geo_cleaned = df_geo_cleaned.withColumn("timestamp", df_geo_cleaned["timestamp"].cast("timestamp"))

# Reorder the DataFrame columns to have the following column order:
# ind
# country
# coordinates
# timestamp

df_geo_cleaned = df_geo_cleaned.select("ind", "country", "coordinates", "timestamp")


display(df_geo_cleaned)
print(df_geo_cleaned.schema['timestamp'].dataType)


# COMMAND ----------

df_user = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0affc56add51-user') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

display(df_user)

# COMMAND ----------

df_user_updated = df_user.selectExpr("CAST(data as STRING)")

display(df_user_updated)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import from_json
import pyspark.sql.functions as F

schema_user = StructType([
    StructField("ind", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("date_joined", StringType(), True)
])

mapped_df_user = df_user_updated.withColumn("data", from_json(df_user_updated["data"], schema_user))\
    .select(col('data.*'))

#Drop all null values
df_user_cleaned = mapped_df_user.dropna(how='all')

#Drop duplicates
df_user_cleaned = df_user_cleaned.dropDuplicates()

# Create a new column user_name that concatenates the information found in the first_name and last_name columns

df_user_cleaned = df_user_cleaned.withColumn("user_name", concat("first_name", lit(" "), "last_name"))

# Drop the first_name and last_name columns from the DataFrame

df_user_cleaned = df_user_cleaned.drop("first_name", "last_name")

# Convert the date_joined column from a string to a timestamp data type

df_user_cleaned = df_user_cleaned.withColumn("date_joined", df_user_cleaned["date_joined"].cast("timestamp"))

# Reorder the DataFrame columns to have the following column order:
# ind
# user_name
# age
# date_joined

df_user_cleaned = df_user_cleaned.select("ind", "user_name", "age", "date_joined")
                             
display(df_user_cleaned)
print(df_user_cleaned.schema['date_joined'].dataType)

# COMMAND ----------

df_pin_cleaned.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0affc56add51_pin_table")

# COMMAND ----------

df_geo_cleaned.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0affc56add51_geo_table")

# COMMAND ----------

df_user_cleaned.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0affc56add51_user_table")

# COMMAND ----------

dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True)
