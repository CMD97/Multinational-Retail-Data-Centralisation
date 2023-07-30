from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd

class DataCleaning:
    def __init__(self):
        dc = DatabaseConnector()
        de = DataExtractor()
        self.df = de.df_rds_table
        self.cleandata = self.clean_user_data()
        print(self.cleandata)

    def clean_user_data(self):
        # Dropping duplicates & null values
        cleaned_df = self.df.set_index('index')
        # cleaned_df = cleaned_df.apply(self.remove_rows_with_numbers)
        cleaned_df = self.df.dropna()
        cleaned_df = cleaned_df.drop_duplicates()
        
        # Setting required types to category
        cleaned_df['country'] = cleaned_df['country'].astype('category')
        cleaned_df['country_code'] = cleaned_df['country_code'].astype('category')

        # Matching country codes with countries
        cleaned_df.loc[cleaned_df['country_code'] == 'DE', 'country'] = 'Germany'
        cleaned_df.loc[cleaned_df['country'] == 'Germany', 'country_code'] = 'DE'

        cleaned_df.loc[cleaned_df['country_code'] == 'GB', 'country'] = 'United Kingdom'
        cleaned_df.loc[cleaned_df['country'] == 'United Kingdom', 'country_code'] = 'GB'

        cleaned_df.loc[cleaned_df['country_code'] == 'US', 'country'] = 'United States'
        cleaned_df.loc[cleaned_df['country'] == 'United States', 'country_code'] = 'US'

        # Phone Number formatting done in a different function
        # cleaned_df['phone_number'] = cleaned_df['phone_number'].apply(self.standardise_phone_number)

        # Setting required columns to timedate
        cleaned_df['date_of_birth'] = pd.to_datetime(cleaned_df['date_of_birth'], errors='coerce').dt.date
        cleaned_df['join_date'] = pd.to_datetime(cleaned_df['join_date'], errors='coerce').dt.date
        return cleaned_df

        # .dt.date look at this. think about how to encompass on more than 1 column
        # did I see the null values check datatypes.
        # print(set(cleaned_df["phone_number"].head(25)))

    def standardise_phone_number(self, phone_number): #check regex
        pass

if __name__ == '__main__':
    DataCleaning()