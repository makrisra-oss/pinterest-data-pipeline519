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

