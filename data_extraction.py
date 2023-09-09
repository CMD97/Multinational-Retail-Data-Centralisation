from sqlalchemy import inspect
from database_utils import DatabaseConnector
import pandas as pd
import tabula

class DataExtractor:
    def __init__(self):
        db = DatabaseConnector()
        self.engine = db.init_db_engine()
        self.table_names = self.list_db_table()
        self.df_rds_table = self.read_rds_table(self.table_names)
        self.card_details_df = self.retrieve_pdf_data()
    #lists names of the the table from engine in DatabaseConnector
    def list_db_table(self):
        self.engine = self.engine.connect()
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        return table_names[1]
    
    #Extract database table to a Pandas Dataframe
    def read_rds_table(self, table_names):
        df_rds_table = pd.read_sql_table(table_names, self.engine)
        return df_rds_table
    
    # Retrieving card data from a pdf file using tabula
    def retrieve_pdf_data(self):
        pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        card_details_df = tabula.read_pdf(pdf_path, pages='all', stream=True)
        card_details_df = pd.concat(card_details_df, ignore_index=True)
        card_details_df.drop("card_number expiry_date", axis=1, inplace=True)
        card_details_df.drop("Unnamed: 0", axis=1, inplace=True)
        return card_details_df