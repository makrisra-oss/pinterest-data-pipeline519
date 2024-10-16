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

# COMMAND ----------

# Milestone 7 Task 2

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, regexp_replace


#Replace empty strings and irrelevant data with null
df_pin = df_pin.replace('', None)
df_pin = df_pin.replace('No description available Story format', None)

#Perform the necessary transformations on the follower_count to ensure every entry is a number. Make sure the data type of this column is an int.

# df_pin = df.withColumn("follower_count_cleaned", 
df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "k" , "000"))
df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "K" , "000"))
df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "m" , "000000"))
df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "M" , "000000"))

#Cast string to int
df_pin = df_pin.withColumn("follower_count", df_pin["follower_count"].cast("integer"))

# Ensure that each column containing numeric data has a numeric data type
df_pin = df_pin.withColumn("downloaded", df_pin["downloaded"].cast("integer"))
df_pin = df_pin.withColumn("index", df_pin["index"].cast("integer"))

# Clean the data in the save_location column to include only the save location path

df_pin = df_pin.withColumn("save_location", regexp_replace("save_location", "Local save in " , ""))

# Rename the index column to ind
df_pin = df_pin.withColumnRenamed("index", "ind")


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

df_pin = df_pin.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category")

display(df_pin)
# Check data type of a specific column
print(df_pin.schema['follower_count'].dataType)
print(df_pin.schema['ind'].dataType)



# COMMAND ----------

# Milestone 7 Task 2

# Create a new column coordinates that contains an array based on the latitude and longitude columns

df_geo = df_geo.withColumn("coordinates", array("latitude", "longitude"))

# Drop the latitude and longitude columns from the DataFrame

df_geo = df_geo.drop("latitude", "longitude")

# Convert the timestamp column from a string to a timestamp data type

df_geo = df_geo.withColumn("timestamp", df_geo["timestamp"].cast("timestamp"))

# Reorder the DataFrame columns to have the following column order:
# ind
# country
# coordinates
# timestamp

df_geo = df_geo.select("ind", "country", "coordinates", "timestamp")


display(df_geo.head(10))
print(df_geo.schema['timestamp'].dataType)

# COMMAND ----------

# Milestone 7 Task 3

# Create a new column user_name that concatenates the information found in the first_name and last_name columns

df_user = df_user.withColumn("user_name", concat("first_name", lit(" "), "last_name"))

# Drop the first_name and last_name columns from the DataFrame

df_user = df_user.drop("first_name", "last_name")

# Convert the date_joined column from a string to a timestamp data type

df_user = df_user.withColumn("date_joined", df_user["date_joined"].cast("timestamp"))

# Reorder the DataFrame columns to have the following column order:
# ind
# user_name
# age
# date_joined

df_user = df_user.select("ind", "user_name", "age", "date_joined")
                             
display(df_user.head(10))
print(df_user.schema['date_joined'].dataType)

# COMMAND ----------

# # Milestone 7 Task 4


from pyspark.sql import functions as F

# Find the most popular Pinterest category people post to based on their country.


# Your query should return a DataFrame that contains the following columns:

# country
# category
# category_count, a new column containing the desired query output

joined_df = df_pin.join(df_geo, "ind", "inner")

joined_df = joined_df.select("ind","country", "category")

grouped_df = joined_df.groupBy("country", "category") \
                      .agg(F.count("category").alias("category_count"))

grouped_df = grouped_df.orderBy(F.desc("category_count"))

display(grouped_df)


# COMMAND ----------

#Milestone 7 Task 5

# Find how many posts each category had between 2018 and 2022.


# Your query should return a DataFrame that contains the following columns:

# post_year, a new column that contains only the year from the timestamp column
# category
# category_count, a new column containing the desired query output

joined_df = df_pin.join(df_geo, "ind", "inner")

joined_df = joined_df.withColumn("post_year", F.year("timestamp"))

joined_df = joined_df.filter((F.col("post_year") >= 2018) & (F.col("post_year") <= 2022))

joined_df = joined_df.groupBy("post_year", "category") \
                      .agg(F.count("category").alias("category_count"))

joined_df = joined_df.orderBy(F.desc("category_count"))

display(joined_df)

# COMMAND ----------

# #Milestone 7 Task 6

# Step 1: For each country find the user with the most followers.

# Your query should return a DataFrame that contains the following columns:

# country
# poster_name
# follower_count

from pyspark.sql import functions as F

joined_df = df_pin.join(df_geo, "ind", "inner")

joined_df = joined_df.select("country", "poster_name", "follower_count")

# grouped_df = combined_df.groupBy("Gender").agg({"Salary": "avg"})

joined_df = joined_df.groupBy("country", "poster_name", "follower_count").agg(F.max("follower_count"))

joined_df = joined_df.orderBy(F.desc("follower_count"))

# Step 2: Based on the above query, find the country with the user with most followers.


# Your query should return a DataFrame that contains the following columns:

# country
# follower_count
# # This DataFrame should have only one entry.

from pyspark.sql.window import Window
from pyspark.sql.functions import avg

joined_df = joined_df.select("country", "poster_name", "follower_count")

# Define window specification
window_spec = Window.partitionBy("country").orderBy(F.desc("follower_count"))

# Add rank column based on follower count within each country
ranked_df = joined_df.withColumn("rank", F.row_number().over(window_spec))

# Filter to get the user with the most followers in each country
top_users_per_country = ranked_df.filter(F.col("rank") == 1).drop("rank")

# To find the country with the user with the most followers, you can simply order and take the first row
country_with_top_user = top_users_per_country.orderBy(F.desc("follower_count")).limit(1)

# Display the result
display(country_with_top_user)

# COMMAND ----------

# Milestone 7 Task 7:

# What is the most popular category people post to based on the following age groups:

# 18-24
# 25-35
# 36-50
# +50
# Your query should return a DataFrame that contains the following columns:

# age_group, a new column based on the original age column
# category
# category_count, a new column containing the desired query output

from pyspark.sql import functions as F
from pyspark.sql.window import Window

joined_df = df_pin.join(df_user, "ind", "inner")

# Step 1: Create the age_group column based on the age ranges
df_with_age_group = joined_df.withColumn("age",
                                      F.when(F.col("age").between(18, 24), "18-24")
                                      .when(F.col("age").between(25, 35), "25-35")
                                      .when(F.col("age").between(36, 50), "36-50")
                                      .when(F.col("age") > 50, "+50")
)

# Step 2: Group by age_group and category, and count the number of posts in each category for each age group

category_count_df = df_with_age_group.groupBy("age", "category") \
                                      .agg(F.count("category").alias("category_count"))

# Step 3: Create a window to rank the categories by the count in each age group
window_spec = Window.partitionBy("age").orderBy(F.desc("category_count"))

# Step 4: Use row_number() to find the most popular category per age group
ranked_df = category_count_df.withColumn("rank", F.row_number().over(window_spec))
                                         
# Step 5: Filter to get the most popular category in each age group
most_popular_category_df = ranked_df.filter(F.col("rank") == 1).drop("rank")

# Step 6: Display the result

most_popular_category_df = most_popular_category_df.withColumnRenamed("age", "age_group")

most_popular_category_df.select("age_group", "category", "category_count")

most_popular_category_df_ordered  = most_popular_category_df.orderBy(F.desc("category_count"))

display(most_popular_category_df_ordered)
print(most_popular_category_df.columns)

# COMMAND ----------

# Milestone 7 What is the median follower count for users in the following age groups:

18-24
25-35
36-50
# +50
# Your query should return a DataFrame that contains the following columns:

# age_group, a new column based on the original age column
# median_follower_count, a new column containing the desired query output

from pyspark.sql import functions as F

joined_df = df_pin.join(df_user, "ind", "inner")

df_with_age_group = joined_df.withColumn("age",
                                      F.when(F.col("age").between(18, 24), "18-24")
                                      .when(F.col("age").between(25, 35), "25-35")
                                      .when(F.col("age").between(36, 50), "36-50")
                                      .when(F.col("age") > 50, "+50")
)
most_popular_category_df = most_popular_category_df.withColumnRenamed("age", "age_group")

df_with_age_group = df_with_age_group.withColumnRenamed("age", "age_group")

# df_with_age_group = df_with_age_group.withColumn("median_follower_count", F.col("follower_count"))

df_with_age_group = df_with_age_group.groupBy("age_group") \
    .agg(F.expr('percentile_approx(follower_count, 0.5)').alias('median_follower_count'))





# COMMAND ----------

df_with_age_group_ordered = df_with_age_group.orderBy(F.desc("median_follower_count"))

display(df_with_age_group_ordered)

# COMMAND ----------


