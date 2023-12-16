from sqlalchemy import create_engine
import yaml

class DatabaseConnector:

    def read_db_creds(self, creds_file):
        """
        Read database credentials from a YAML file.

        Parameters:
        - creds_file (str): Path to the YAML file containing database credentials.

        Returns:
        dict: Dictionary containing database credentials.
        """
        with open(creds_file, 'r') as creds:
            creds = yaml.safe_load(creds)
        return creds

    def read_api_creds(self, api_key):
        """
        Read API credentials from a YAML file and construct headers.

        Parameters:
        - api_key (str): Path to the YAML file containing API key.

        Returns:
        dict: Headers for API request.
        """
        with open(api_key, 'r') as apikey:
            api = yaml.safe_load(apikey)
        if 'x-api-key' in api:
            api_key_value = api['x-api-key']
            headers = {'x-api-key': api_key_value}
            return headers

    def init_db_engine(self, creds):
        """
        Initialize a SQLAlchemy engine for connecting to a PostgreSQL database.

        Parameters:
        - creds (dict): Dictionary containing database credentials.

        Returns:
        sqlalchemy.engine.Engine: SQLAlchemy engine object.
        """
        engine = create_engine(
            f"{'postgresql'}+{'psycopg2'}://{creds['USER']}:{creds['PASSWORD']}@"
            f"{creds['HOST']}:{creds['PORT']}/{creds['DATABASE']}"
        )
        return engine

    def upload_to_db(self, df, sql_table_name, engine):
        """
        Upload a Pandas DataFrame to a PostgreSQL database table.

        Parameters:
        - df (pandas.DataFrame): DataFrame to upload.
        - sql_table_name (str): Name of the target SQL table.
        - engine (sqlalchemy.engine.Engine): SQLAlchemy engine object.
        """
        df.to_sql(sql_table_name, engine, index=False)

    def upload_sequence(self, clean_df, table_name):
        """
        Execute a sequence of methods to upload a DataFrame to a PostgreSQL database.

        Parameters:
        - clean_df (pandas.DataFrame): DataFrame to upload.
        - table_name (str): Name of the target SQL table.
        """
        sql_creds = self.read_db_creds('to_sql.yaml')
        upload_engine = self.init_db_engine(sql_creds)
        self.upload_to_db(clean_df, table_name, upload_engine)
