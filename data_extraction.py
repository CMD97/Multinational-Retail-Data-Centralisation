from sqlalchemy import inspect
from dotenv import load_dotenv
import pandas as pd
import tabula
import requests
import boto3

class DataExtractor:

    def list_db_table(self, engine):
        """
        List the names of tables in the given SQLAlchemy engine.

        Parameters:
        - engine (sqlalchemy.engine.Engine): SQLAlchemy engine object.

        Returns:
        list: List of table names in the database.
        """
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        return table_names
    
    def read_rds_table(self, table_name, engine):
        """
        Extract a database table to a Pandas DataFrame.

        Parameters:
        - table_name (str): Name of the table to extract.
        - engine (sqlalchemy.engine.Engine): SQLAlchemy engine object.

        Returns:
        pandas.DataFrame: DataFrame containing the table data.
        """
        df_rds_table = pd.read_sql_table(table_name, engine)
        return df_rds_table
    
    def retrieve_pdf_data(self, pdf_path):
        """
        Retrieve card data from a PDF file using tabula.

        Parameters:
        - pdf_path (str): Path to the PDF file.

        Returns:
        pandas.DataFrame: DataFrame containing the extracted card data.
        """
        card_details_df = tabula.read_pdf(pdf_path, pages='all', stream=False)
        card_details_df = pd.concat(card_details_df, ignore_index=True)
        return card_details_df

    def list_number_of_stores(self, url, headers):
        """
        List the number of stores from an API.

        Parameters:
        - url (str): API endpoint URL.
        - headers (dict): Headers for the API request.

        Returns:
        int: Number of stores retrieved from the API.
        """
        number_of_stores_api = requests.get(url, headers=headers).json() 
        number_of_stores = number_of_stores_api['number_stores']
        return number_of_stores
    
    def retrieve_stores_data(self, number_of_stores, url, headers):
        """
        Retrieve store details from an API and create a DataFrame.

        Parameters:
        - number_of_stores (int): Number of stores to retrieve.
        - url (str): API endpoint URL.
        - headers (dict): Headers for the API request.

        Returns:
        pandas.DataFrame: DataFrame containing store details.
        """
        store_details = []
        for store_number in range(0, number_of_stores):
            store_data = requests.get(f'{url}/{store_number}', headers=headers)
            if store_data.status_code == 200:
                store_details.append(store_data.json())
            else:
                print(f'Error fetching data for store {store_number}')

        store_details_df = pd.DataFrame(store_details)
        return store_details_df
    
    def extract_from_s3(self, bucket, object, local_name):
        """
        Extract data from an S3 bucket and read it into a Pandas DataFrame.

        Parameters:
        - bucket (str): S3 bucket name.
        - object (str): S3 object key.
        - local_name (str): Local file name to save the downloaded file.

        Returns:
        pandas.DataFrame: DataFrame containing the extracted data.
        """
        s3 = boto3.client('s3')
        s3.download_file(bucket, object, local_name)

        # Allowing all file formats to be read within the single method.
        if local_name.endswith('.csv'):
            df = pd.read_csv(local_name)
        elif local_name.endswith('.json'):
            df = pd.read_json(local_name)
        else:
            raise ValueError(f"Unsupported file format: {local_name}")
        return df
