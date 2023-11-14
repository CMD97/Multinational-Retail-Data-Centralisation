from sqlalchemy import inspect
from database_utils import DatabaseConnector
import pandas as pd
import tabula
import requests
import boto3

class DataExtractor:
    def __init__(self):
        dc = DatabaseConnector()
        self.engine = dc.init_db_engine()
        # self.headers = dc.read_api_creds()
        # self.table_names = self.list_db_table()
        # self.df_rds_table = self.read_rds_table(self.table_names)
        # self.card_details_df = self.retrieve_pdf_data()
        # self.number_of_stores = self.list_number_of_stores()
        # self.store_details_df = self.retrieve_stores_data()

    # Lists names of the the table from engine in DatabaseConnector
    def list_db_table(self):
        self.engine = self.engine.connect()
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        return table_names[2]
    
    #Extract database table to a Pandas Dataframe
    def read_rds_table(self, table_names):
        df_rds_table = pd.read_sql_table(table_names, self.engine)
        return df_rds_table
    
    # Retrieving card data from a pdf file using tabula
    def retrieve_pdf_data(self):
        pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        card_details_df = tabula.read_pdf(pdf_path, pages='all', stream=False)
        card_details_df = pd.concat(card_details_df, ignore_index=True)
        return card_details_df

    def list_number_of_stores(self):
        number_of_stores_api = requests.get('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', headers=self.headers).json() 
        number_of_stores = number_of_stores_api['number_stores']
        return number_of_stores
    
    def retrieve_stores_data(self):
        store_details = []
        for store_number in range(0, self.number_of_stores):
            store_data = requests.get(f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}', headers=self.headers)
            if store_data.status_code == 200:
                store_details.append(store_data.json())
            else:
                print(f'Error fetching data for store {store_number}')

        store_details_df = pd.DataFrame(store_details)
        return store_details_df
    
    def extract_from_s3_csv(self):
        s3 = boto3.client('s3')
        s3.download_file('data-handling-public', 'products.csv', 'products.csv')
        products_df = pd.read_csv('products.csv')
        return products_df
    
    def extract_from_s3_json(self):
        s3 = boto3.client('s3')
        s3.download_file('data-handling-public', 'date_details.json', 'date_details.json')
        date_details_df = pd.read_json('date_details.json')
        return date_details_df