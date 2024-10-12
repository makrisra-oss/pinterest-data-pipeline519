Pinterest Data Pipeline Readme

Milestone 1 to 3

Milestone 1 and 2 initially aim to set up the environment for the project and lay the groundwork to get started. In Milestone 1 the setting up of Github is done creating a new repo for the project of which this document resides. Next, we create and login to an AWS account so there is a cloud environment to use with the rest of the integration for the project.

In Milestone 2 a Pinterest infrastructure is downloaded. This is the user_posting_emulation.py file found in the project which includes:

-pinterest_data
-geolocation_data
-user_data

A seperate db_creds.yaml file is created to store the database credentials (HOST, USER, PASSWORD), which isn't uploaded to Github for security reasons (it's ignored in the .gitignore file).

The user_posting_emulation.py file is run and it prints out the pin_result, geo_result and user_result, which each represent one entry in the corresponding table.

The next step in Milestone 2, is to login in to the AWS account with the credentials we are given from the project. The password is then changed for security reasons.

In Milestone 3, a .pem file is created with a key pair combination this file eventually helps connect to the EC2 instance setup. Then the EC2 instance is setup, thereafter, Kafka is setup on the EC2 instance whilst the following Kafka topics are also created:

-0affc56add51.pin
-0affc56add51.geo
-0affc56add51.user

Milestone 4 accomplishes connecting an MSK cluster to an S3 bucket. First, a custom plugin is created with MSK connect, then a connector is created via MSK connect to link with the S3 bucket.

Milestone 5, finally, creates an API gateway that sends data to the MSK cluster, which in turn is stored in the S3 bucket using the connector that has been built in the previous milestone.

With the pipeline created in Milestones 1 to 5, it is now possible to setup the Databricks account which reads data from AWS into Databricks which is accomplished in Milestone 6.

Milestone 7 serves the purpose of batch processing the data using Spark on Databricks and covers the following tasks which have been completed:

Task 1:

To clean the df_pin DataFrame you should perform the following transformations:

Replace empty entries and entries with no relevant data in each column with Nones
Perform the necessary transformations on the follower_count to ensure every entry is a number. Make sure the data type of this column is an int.
Ensure that each column containing numeric data has a numeric data type
Clean the data in the save_location column to include only the save location path
Rename the index column to ind.
Reorder the DataFrame columns to have the following column order:
ind
unique_id
title
description
follower_count
poster_name
tag_list
is_image_or_video
image_src
save_location
category

Task 2:

To clean the df_geo DataFrame you should perform the following transformations:

Create a new column coordinates that contains an array based on the latitude and longitude columns
Drop the latitude and longitude columns from the DataFrame
Convert the timestamp column from a string to a timestamp data type
Reorder the DataFrame columns to have the following column order:
ind
country
coordinates
timestamp

Task 3:

To clean the df_user DataFrame you should perform the following transformations:

Create a new column user_name that concatenates the information found in the first_name and last_name columns
Drop the first_name and last_name columns from the DataFrame
Convert the date_joined column from a string to a timestamp data type
Reorder the DataFrame columns to have the following column order:
ind
user_name
age
date_joined

Task 4:

Find the most popular Pinterest category people post to based on their country.


Your query should return a DataFrame that contains the following columns:

country
category
category_count, a new column containing the desired query output

Task 5:

Find how many posts each category had between 2018 and 2022.


Your query should return a DataFrame that contains the following columns:

post_year, a new column that contains only the year from the timestamp column
category
category_count, a new column containing the desired query output

Task 6:

Step 1: For each country find the user with the most followers.


Your query should return a DataFrame that contains the following columns:

country
poster_name
follower_count
Step 2: Based on the above query, find the country with the user with most followers.


Your query should return a DataFrame that contains the following columns:

country
follower_count
This DataFrame should have only one entry.

Task 7:

What is the most popular category people post to based on the following age groups:

18-24
25-35
36-50
+50
Your query should return a DataFrame that contains the following columns:

age_group, a new column based on the original age column
category
category_count, a new column containing the desired query output

Task 8:

What is the median follower count for users in the following age groups:

18-24
25-35
36-50
+50
Your query should return a DataFrame that contains the following columns:

age_group, a new column based on the original age column
median_follower_count, a new column containing the desired query output

Task 9:

Find how many users have joined between 2015 and 2020.


Your query should return a DataFrame that contains the following columns:

post_year, a new column that contains only the year from the timestamp column
number_users_joined, a new column containing the desired query output

Task 10:

Find the median follower count of users have joined between 2015 and 2020.


Your query should return a DataFrame that contains the following columns:

post_year, a new column that contains only the year from the timestamp column
median_follower_count, a new column containing the desired query output

Task 11:

Find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of.


Your query should return a DataFrame that contains the following columns:

age_group, a new column based on the original age column
post_year, a new column that contains only the year from the timestamp column
median_follower_count, a new column containing the desired query output