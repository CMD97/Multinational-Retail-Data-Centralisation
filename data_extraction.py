from sqlalchemy import inspect
from database_utils import DatabaseConnector
import pandas as pd
import tabula
import requests
import boto3

dc = DatabaseConnector()

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

    def list_number_of_stores(self, headers):
        number_of_stores_api = requests.get('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', headers=headers).json() 
        number_of_stores = number_of_stores_api['number_stores']
        return number_of_stores
    
    def retrieve_stores_data(self, number_of_stores, headers):
        store_details = []
        for store_number in range(0, number_of_stores):
            store_data = requests.get(f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}', headers=headers)
            if store_data.status_code == 200:
                store_details.append(store_data.json())
            else:
                print(f'Error fetching data for store {store_number}')

        store_details_df = pd.DataFrame(store_details)
        return store_details_df
    
    def extract_from_s3(self, bucket, object, local_name):
        s3 = boto3.client('s3')
        s3.download_file(bucket, object, local_name)
        products_df = pd.read_csv(local_name)
        return products_df
    
    def extract_from_s3_json(self, bucket, object, local_name):
        s3 = boto3.client('s3')
        s3.download_file(bucket, object, local_name)
        date_details_df = pd.read_json(local_name)
        return date_details_df