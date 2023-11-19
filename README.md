# Multinational Retail Data Centralisation Project

This project aims to centralize and manage sales data for a fictitious multinational company that stores related data across various formats and sources. The scattered data makes it challenging to access specific datasets and perform comprehensive analysis. The goal is to create a unified data repository, providing easier accessibility and serving as the single source of truth for the company's sales data.

## Overview

This repository contains the code and documentation for an ETL (Extract, Transform, Load) project focused on handling and analyzing sales data. The project involves extracting data from diverse sources, performing data cleaning and transformation, and loading it into a PostgreSQL database named `sales_data`. Data sources include CSV files, an API, AWS S3, and an AWS RDS database.

## Project Steps

1. **Data Extraction**: Extract data from various sources, including CSV files, an API, AWS S3, and an AWS RDS database.

2. **Data Cleaning and Transformation**: Clean and transform the extracted data to ensure consistency and reliability.

3. **Data Loading**: Load the cleaned data into the `sales_data` PostgreSQL database.

## Files

- **data_extraction.py**: This script contains the `DataExtractor` class, which is responsible for extracting data from multiple sources, including CSV files, an AWS RDS database, AWS S3, and an API. It fetches data and stores it in pandas DataFrames.

- **database_utils.py**: The `DatabaseConnector` class defined in this script manages the interaction with the PostgreSQL database. It reads the database credentials from a `db_creds.yaml` file, initializes the database engine, lists tables, and uploads dataframes into the database.

- **data_cleaning.py**: In this script, the `DataCleaning` class contains methods for cleaning and transforming the extracted data. It handles data cleaning, error handling, and standardization.

- **db_creds.yaml & to_sql.yaml**: These configuration files store sensitive database credentials. It's listed in the `.gitignore` file to ensure privacy.

### Milestones 1 & 2

The first two milestones are to get the data through the ETL process:

Data is able to be extracted through the various sources, with the use of:

- `psycopg2` & `sqlalchemy` to create an engine that connects to AWS RDS.
- `tabula.py` to extract datasets from a PDF.
- `requests` to extract datasets from an API.
- `boto3` to extract datasets from an AWS S3 bucket.

The datasets are then made into Pandas Dataframes before being analysed within specific functions provided by the Pandas library, such as:

- `.info()` which gave the information for each row & their datatypes, this gave visualisation of being able to cast columns such as `join_date` or `country_code` into datetime and categorical datatypes respectively.
- `.duplicated().sum()` which allowed insight to see if any of the rows within the datasets were duplicated and could potentially be dropped.
- `.dropna()` allowing to instantly drop any rows that had null values.

After the initial cleaning was done, the dataset was then uploaded to the `uncleaned` database located on pgadmin4, which allowed for better visualisation that verified the datasets were clean, by querying each column using `WHERE` statements that would select rows that proved to be anomalies.

Once the datasets were cleaned using the Pandas library, the finalised data was taken to the `DatabaseConnector` class were it was uploaded to the database `sales_data`, where it was centralised and ready for querying in postgreSQL.

### Milestone 3 & 4

Milestones 3 & 4 were both performed using postgreSQL inside pgAdmin4. As there is no recollection of them on VSCode, there is `.txt` & `.ipynb` files that are named respectively which have the queries inside that were executed, as well as a detail of what was being requested.

Below is a brief explanation of both:

**Milestone 3** : This milestone entailed development of the schema, and ensuring each column within each table has the correct datatype set to the column in order to query the data correctly.

**Milestone 4** : After the schema and corrected datatypes were in place, the fictional company began to set analysis tasks to be able to utilise the SQL queries.

### Improvements

Improvements will be marked with bullet points, with priority of the best improvements to make, to ones of least importance:

- Within `data_cleaning.py` there is a lot of lines of code, specifically with methods that are used within each of the 6 main methods, I would like to move the 'extra' methods into a different file to be called upon when needed, which allows for easier readability when trying to see what is being cleaned where and then it can be referred back to.

- Allowing errors to be caught easier; each part of the code could be ran by try/except blocks to be able to catch any error that surfaces and can then be tackled at the route, rather than having to sieve through the default error messages to locate the issue.

- Allowing the program to be more versatile; as this program is intended for usage of singular data, it may be the case that the table could be the same, but the extraction method is different, e.g. being able to have the store details be taken from a pdf, and processed within the same cleaning method. This would be achieved through the addition of more user inputs.

- The implementation of unit tests as this will ensure the accuracy of the process, catching errors early on will promote efficiency when writing the code.

- The final improvement would be within the phone numbers section, as I have put regex formulae to be able to standardise phone numbers, and a way to ensure US phone numbers are corrected, however this could be improved by creating further regex for the DE & GB numbers. Upon further analysis, it would also be possible to obtain US area codes to ensure the numbers within the table are actually part of the US system.