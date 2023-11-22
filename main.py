from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

def upload_dim_users():

    # Getting credentials for the engine to the RDS Database.
    rds_creds = du.read_db_creds('db_creds.yaml')

    # Initialising the connection to the database
    extract_engine = du.init_db_engine(rds_creds)
    
    # Retrieving the table name for the users data
    list_of_db_tables = de.list_db_table(extract_engine)

    # The 2nd table is for the users data.
    users_table_name = list_of_db_tables[1]

    # Retrieving the SQL table into a Dataframe within the DataExtractor
    users_df = de.read_rds_table(users_table_name, extract_engine)

    # Cleaning the Dataframe within the DataCleaning class.
    clean_df = dc.clean_user_data(users_df)

    # Uploading to the SQL Database
    du.upload_sequence(clean_df, table_name='dim_users')

def upload_dim_card_details():

    # Retrieving the card details from the PDF document inside the AWS S3 bucket.
    card_details_df = de.retrieve_pdf_data(pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")

    # Cleaning the card details through the method in the DataCleaning class.
    clean_df = dc.clean_card_data(card_details_df)

    # Uploading to the SQL Database
    du.upload_sequence(clean_df, table_name='dim_card_details')

def upload_dim_store_details():

    # As the store details are stored within an API, the headers need to be read in from the .yaml file.
    headers = du.read_api_creds(api_key='api.yaml')
    
    # Finding how many stores are present within the table.
    number_of_stores = de.list_number_of_stores(headers)

    # Now the number of stores has been found, it's now possible to use this as a range to return all the stores from within the API.
    store_details_df = de.retrieve_stores_data(number_of_stores, headers)

    # Cleaning the store details through the DataCleaning class.
    clean_df = dc.clean_store_data(store_details_df)

    # Utilising method in DatabaseConnector to upload store details the SQL Database.
    du.upload_sequence(clean_df, table_name='dim_store_details')

def upload_dim_products():

    # Retrieving the csv from the S3 using boto3.
    products_df = de.extract_from_s3(bucket='data-handling-public', object='products.csv', local_name='products.csv')

    # Cleaning the products df taken from the S3 bucket in the DataCleaning class.
    clean_df = dc.clean_products_data(products_df)

    # Uploading to SQL with DatabaseConnector class.
    du.upload_sequence(clean_df, table_name='dim_products')

def upload_orders_table():

    # Getting credentials for the engine to the RDS Database.
    rds_creds = du.read_db_creds('db_creds.yaml')

    # Initialising the connection to the database
    extract_engine = du.init_db_engine(rds_creds)
    
    # Retrieving the table name for the users data
    list_of_db_tables = de.list_db_table(extract_engine)

    # The 3rd table is the orders table.
    orders_table_name = list_of_db_tables[2]

    # Retrieving the SQL table into a Dataframe within the DataExtractor
    orders_df = de.read_rds_table(orders_table_name, extract_engine)

    # Cleaning the Dataframe within the DataCleaning class.
    clean_df = dc.clean_orders_data(orders_df)

    # Uploading to the SQL Database
    du.upload_sequence(clean_df, table_name='orders_table')

def upload_dim_date_times():
    # Retrieving the csv from the S3 using boto3.
    date_times_df = de.extract_from_s3_json(bucket='data-handling-public', object='date_details.json', local_name='date_details.json')

    # Cleaning the products df taken from the S3 bucket in the DataCleaning class.
    clean_df = dc.clean_date_details(date_times_df)

    # Uploading to SQL with DatabaseConnector class.
    du.upload_sequence(clean_df, table_name='dim_date_times')

def choose_upload():
    while True:
        choose_table = input('''You can insert into 6 tables from:

        users                                    
        card details
        store details
        products details
        orders table
        date times
                            
        select one or type `exit` to end program:  ''' ).lower()

        if choose_table == 'users':
            upload_dim_users()
            print('\nUpload Successful. Run `main.py` again to upload another.')
            break

        elif choose_table == 'card details':
            upload_dim_card_details()
            print('\nUpload Successful. Run `main.py` again to upload another.')
            break

        elif choose_table == 'store details':
            upload_dim_store_details()
            print('\nUpload Successful. Run `main.py` again to upload another.')
            break

        elif choose_table == 'products details':
            upload_dim_products()
            print('\nUpload Successful. Run `main.py` again to upload another.')
            break

        elif choose_table == 'orders table':
            upload_orders_table()
            print('\nUpload Successful. Run `main.py` again to upload another.')
            break

        elif choose_table == 'date times':
            upload_dim_date_times()
            print('\nUpload Successful. Run `main.py` again to upload another.')
            break
        
        elif choose_table == 'exit':
            print('\nYou have quit the program, run `main.py` again to upload a table to the database.')
            break

        else:
            print('\nMake sure you\'re inputting the exact name as it is displayed.\n')



if __name__ == '__main__':

    de = DataExtractor()
    dc = DataCleaning()
    du = DatabaseConnector()

    choose_upload()

