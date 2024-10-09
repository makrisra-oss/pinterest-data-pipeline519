# Databricks notebook source
# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib

# COMMAND ----------

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
# MAGIC --Disable format checks during the reading of Delta tables
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

file_location = "s3a://user-0affc56add51-bucket/topics/0affc56add51.pin/partition=0/*.json"
file_type = "json"
infer_schema = "true"
df_pin = spark.read.format("json") \
.option("inferSchema", infer_schema) \
.load(file_location)
display(df_pin)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Disable format checks during the reading of Delta tables
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

file_location = "s3a://user-0affc56add51-bucket/topics/0affc56add51.geo/partition=0/*.json"
file_type = "json"
infer_schema = "true"
df_geo = spark.read.format("json") \
.option("inferSchema", infer_schema) \
.load(file_location)
display(df_geo)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Disable format checks during the reading of Delta tables
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

file_location = "s3a://user-0affc56add51-bucket/topics/0affc56add51.user/partition=0/*.json"
file_type = "json"
infer_schema = "true"
df_user = spark.read.format("json") \
.option("inferSchema", infer_schema) \
.load(file_location)
display(df_user)
