from sqlalchemy import create_engine
import yaml

class DatabaseConnector:

# Reading in the database credentials for the AWS database.
    def read_db_creds(self, creds_file):
        with open(creds_file, 'r') as creds:
            creds=yaml.safe_load(creds)
        return creds

# Reading in the headers needed for the API request.
    def read_api_creds(self, api_key):
        with open(api_key, 'r') as apikey:
            api = yaml.safe_load(apikey)
        if 'x-api-key' in api:
            api_key_value = api['x-api-key']
            headers = {'x-api-key': api_key_value}
            return headers
        
# engine creation to AWS using the credentials of rds_dict.
    def init_db_engine(self, creds):
        engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{creds['USER']}:{creds['PASSWORD']}@{creds['HOST']}:{creds['PORT']}/{creds['DATABASE']}")
        return engine

# Uploading to pgAdmin.
    def upload_to_db(self, df, sql_table_name, engine):
        df.to_sql(sql_table_name, engine, index=False)

# Takes in the above methods to allow for an easier way to upload without repeating steps.
    def upload_sequence(self, clean_df, table_name):
        sql_creds = self.read_db_creds('to_sql.yaml')
        upload_engine = self.init_db_engine(sql_creds)
        self.upload_to_db(clean_df, table_name, upload_engine)