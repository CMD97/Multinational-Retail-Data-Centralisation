from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd
import re

class DataCleaning:
    def __init__(self):
        de = DataExtractor()
        self.df = de.df_rds_table
        self.cleandata = self.clean_user_data()
        dc = DatabaseConnector
        dc().upload_to_db(self.cleandata)
        # print(self.cleandata)

    def clean_user_data(self):
        # Dropping duplicates & null values
        cleaned_df = self.df.dropna()
        cleaned_df = cleaned_df.drop_duplicates()
        
        # Setting required types to category
        cleaned_df['country'] = cleaned_df['country'].astype('category')
        cleaned_df['country_code'] = cleaned_df['country_code'].astype('category')

        # Ensuring data which has a `country code` of the specified country has the corresponding country in the `country` column.
        cleaned_df.loc[cleaned_df['country_code'] == 'DE', 'country'] = 'Germany'
        cleaned_df.loc[cleaned_df['country'] == 'Germany', 'country_code'] = 'DE'

        cleaned_df.loc[cleaned_df['country_code'] == 'GB', 'country'] = 'United Kingdom'
        cleaned_df.loc[cleaned_df['country'] == 'United Kingdom', 'country_code'] = 'GB'

        cleaned_df.loc[cleaned_df['country_code'] == 'US', 'country'] = 'United States'
        cleaned_df.loc[cleaned_df['country'] == 'United States', 'country_code'] = 'US'

        # Dropping rows that do not have a country code as DE/GB/US - in turn dropping other NULL values
        incorrect_rows_with_wrong_country_codes = ~cleaned_df['country_code'].isin(['US','GB','DE'])
        cleaned_df = cleaned_df[~incorrect_rows_with_wrong_country_codes]

        # Phone Number formatting done in a different function
        cleaned_df['phone_number'] = cleaned_df['phone_number'].apply(self.standardise_phone_number)
        
        # Setting required columns to timedate
        cleaned_df['date_of_birth'] = pd.to_datetime(cleaned_df['date_of_birth'], errors='coerce').dt.date
        cleaned_df['join_date'] = pd.to_datetime(cleaned_df['join_date'], errors='coerce').dt.date

        return cleaned_df

    def standardise_phone_number(self, phone_number):

        # Regex to get all rows present with only numbers
        numbers_only_pattern = re.compile(r'\d+')
        matches = re.finditer(numbers_only_pattern, phone_number)
        cleaned_numbers = ''.join(match.group() for match in matches)
        
        # Regex to remove US phone numbers with 
        US_cleaning_pattern = re.compile(r'^\"(0+)?1(\d{10})\b')

        # Testing how to substitute the group made of \d{10} to replace the entire phone number
        matches = re.finditer(US_cleaning_pattern, cleaned_numbers)
        US_subbed_numbers = US_cleaning_pattern.sub(r"\2", cleaned_numbers)

        # Commented out is other methods I have tried to use.

        # double_001_matches = re.finditer(US_cleaning_pattern, cleaned_numbers)
        # removing_001 = cleaned_numbers.replace(to_replace = r'^(001)', value = '',regex=True)
        # cleaned_US_numbers = ''.join(match.group() for match in double_001_matches)

        return US_subbed_numbers

if __name__ == '__main__':
    DataCleaning()