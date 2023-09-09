from sqlalchemy import create_engine, inspect
import yaml

class DatabaseConnector:
    def __init__(self):
        self.rds_dict = self.read_db_creds()
        self.sql_dict = self.read_upload_creds()

# Reading in the database credentials for the AWS database.
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as creds:
            rds_dict=yaml.safe_load(creds)
        return rds_dict

# engine creation to AWS using the credentials of rds_dict.
    def init_db_engine(self):     
        extraction_engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{self.rds_dict['RDS_USER']}:{self.rds_dict['RDS_PASSWORD']}@{self.rds_dict['RDS_HOST']}:{self.rds_dict['RDS_PORT']}/{self.rds_dict['RDS_DATABASE']}")
        return extraction_engine

# Reading in credentials to upload the cleaned data to postgreSQL
    def read_upload_creds(self):
        with open('to_sql.yaml', 'r') as sqlcreds:
            sql_dict=yaml.safe_load(sqlcreds)
        return sql_dict

# engine creation to SQL using the credentials of sql_dict.
    def upload_to_db(self, card_details_df):      
        upload_engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{self.sql_dict['USER']}:{self.sql_dict['PASSWORD']}@{self.sql_dict['HOST']}:{self.sql_dict['PORT']}/{self.sql_dict['DATABASE']}")
        card_details_df.to_sql('dim_card', upload_engine, if_exists='replace', index=False)