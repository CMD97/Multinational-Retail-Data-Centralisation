from sqlalchemy import inspect
from dotenv import load_dotenv
import pandas as pd
import tabula
import requests
import boto3

class DataExtractor:

    # Lists names of the the table from engine in DatabaseConnector
    def list_db_table(self, engine):
        inspector = inspect(engine)
        table_name = inspector.get_table_names()
        return table_name
    
    #Extract database table to a Pandas Dataframe
    def read_rds_table(self, table_name, engine):
        df_rds_table = pd.read_sql_table(table_name, engine)
        return df_rds_table
    
    # Retrieving card data from a pdf file using tabula
    def retrieve_pdf_data(self, pdf_path):
        card_details_df = tabula.read_pdf(pdf_path, pages='all', stream=False)
        card_details_df = pd.concat(card_details_df, ignore_index=True)
        return card_details_df

    def list_number_of_stores(self, url, headers):
        number_of_stores_api = requests.get(url, headers=headers).json() 
        number_of_stores = number_of_stores_api['number_stores']
        return number_of_stores
    
    # Retrieving the information from the API, uses the number of stores to ensure each line gets brought in, and creates a dataframe.
    def retrieve_stores_data(self, number_of_stores, url, headers):
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