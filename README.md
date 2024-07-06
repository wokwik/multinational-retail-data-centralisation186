# multinational-retail-data-centralisation186

# Table of content
- Description
- Installation instructions
- Usage instructions
- File structure of the project
- License information
  
# Description
This project is about creating a set of python classes for collecting data in multiple formats present in multiple data sources for the Multinational Retail Store and transforming it to a well structured tables in a PostreSQL Data Warehouse using Star Schema so that it is earily querable for Analytics and Insights generation.

This project collects raw data stored in the following formats:
- CSV files
- API endpints
- PDF files
- AWS RDS tables
- JSON files

The classes perform data cleaning tasks on the source data to ensure the data is free from wrong formatted, missing or erroneous cells. It then stores the clean data in a PostgreSQL data warehouse tables.


# Installation instruction
You will need Python 3+ installed on your machine

You will need GitHub installed on your machine if you want to clone this repo, otherwise you can just download it from GitHub as zip file.

You can run the data_cleaning.py file locally on your machine, which then uses the DatabaseConnector, DataExtractor, and DataCleaning classes to connect to and fetch the raw the data from source, clean it, and finally store in the a local database:

1- open your temrinal or cmd window

2- navigate to the location of the data_cleaning.py file

3- run data_cleaning.py using the following command:

> python data_cleaning.py

# File structure
The directory includes three files:

- data_utils.py: This file contains the DatabaseConnector class. It is responsible for connecting to the AWS RDS and PostgreSQL Data Warehouses.
- data_extraction.py: This file contains the DataExtractor class. It is responsible for extracting the raw data from different data sources, such as S3 buckets, API end points, and PDF files.
- data_cleaning.py: This file contains the DataCleaning class. It is responsible for cleaning the extracted raw data.
- db_creds.yaml: for local and remote databases. It includes credentials needed to connect to the AWS RDS and PostgreSQL data Warehouses. These files are excluded in the .gitignore file. but a empty template file is present to be repurposed by users.
- README.md: Is where the documentation of this project is noted and kept.

# License information
This code is released under the Creative Common CC BY 4.0 DEED License. 
