from sqlalchemy import inspect
from database_utils import DatabaseConnector
import pandas as pd

class DataExtractor:
    def __init__(self):
        db = DatabaseConnector()
        self.engine = db.init_db_engine()
        self.table_names = self.list_db_table()
        self.df_rds_table = self.read_rds_table(self.table_names)

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