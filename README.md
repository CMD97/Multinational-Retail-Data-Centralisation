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
