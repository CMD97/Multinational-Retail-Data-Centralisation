import pandas as pd
import re

class DataCleaning:
    def clean_user_data(self, users_df):

        # Creating a copy for best practice
        cleaning_users_df = users_df.copy()

        # Dropping duplicates & null values
        cleaning_users_df = cleaning_users_df.dropna()
        cleaning_users_df = cleaning_users_df.drop_duplicates()

        # Dropping the "index" column that's taken through to pgAdmin4
        cleaning_users_df.drop("index", axis=1, inplace=True)
        
        # Setting required types to category
        cleaning_users_df['country'] = cleaning_users_df['country'].astype('category')
        cleaning_users_df['country_code'] = cleaning_users_df['country_code'].astype('category')

        # Ensuring data which has a `country code` of the specified country has the corresponding country in the `country` column.
        cleaning_users_df.loc[cleaning_users_df['country_code'] == 'DE', 'country'] = 'Germany'
        cleaning_users_df.loc[cleaning_users_df['country'] == 'Germany', 'country_code'] = 'DE'

        cleaning_users_df.loc[cleaning_users_df['country_code'] == 'GB', 'country'] = 'United Kingdom'
        cleaning_users_df.loc[cleaning_users_df['country'] == 'United Kingdom', 'country_code'] = 'GB'

        cleaning_users_df.loc[cleaning_users_df['country_code'] == 'US', 'country'] = 'United States'
        cleaning_users_df.loc[cleaning_users_df['country'] == 'United States', 'country_code'] = 'US'

        # Dropping rows that do not have a country code as DE/GB/US - in turn dropping other NULL values
        incorrect_rows_with_wrong_country_codes = ~cleaning_users_df['country_code'].isin(['US','GB','DE'])
        cleaning_users_df = cleaning_users_df[~incorrect_rows_with_wrong_country_codes]

        # Phone Number formatting done in a different function
        cleaning_users_df['phone_number'] = cleaning_users_df['phone_number'].apply(self.standardise_phone_number)
        
        # Setting required columns to timedate
        cleaning_users_df['date_of_birth'] = pd.to_datetime(cleaning_users_df['date_of_birth'], errors='coerce').dt.date
        cleaning_users_df['join_date'] = pd.to_datetime(cleaning_users_df['join_date'], errors='coerce').dt.date

        return cleaning_users_df

    def standardise_phone_number(self, phone_number):                       # standardising phone numbers --- improvements to be made upon the phone_number

        # Regex to get all rows present with only numbers                       
        numbers_only_pattern = re.compile(r'\d+')
        matches = re.finditer(numbers_only_pattern, phone_number)
        cleaned_numbers = ''.join(match.group() for match in matches)

        # Regex to remove the `001` out of a US phone number
        US_cleaning_pattern = re.compile(r"^(0+)?1(\d{10})\b")
        match = re.match(US_cleaning_pattern, cleaned_numbers)
        if match:
            cleaned_numbers = match.group(2)
            return cleaned_numbers
        else:
            return cleaned_numbers
    
    # Cleaning the card details data
    def clean_card_data(self, card_details_df):

        cleaning_card_df = card_details_df.copy()

        # Taking the card details from the init method into the function, dropping NaN rows & duplicates.
        cleaning_card_df = cleaning_card_df.dropna()
        cleaning_card_df = cleaning_card_df.drop_duplicates()
        
        # Using a regex on the expiry date to check for null values and dropping them.
        expiry_date_pattern = re.compile(r"\d{2}\/\d{2}")
        rows_with_incorrect_expiry = ~cleaning_card_df['expiry_date'].str.contains(expiry_date_pattern)
        cleaning_card_df = cleaning_card_df[~rows_with_incorrect_expiry]

        # Using a regex to ensure card_number now has only numbers inside
        cleaning_card_df['card_number'] = cleaning_card_df['card_number'].astype(str)
        cleaning_card_df['card_number'] = cleaning_card_df['card_number'].apply(self.returning_numbers_only)

        # Changing expiry dates to be the last day of the month as they usually will be and changing to date dtype.
        cleaning_card_df['expiry_date'] = cleaning_card_df['expiry_date'].apply(self.convert_expiry_date)

        # Changing the payment date confirmed dtype to a date dtype.
        cleaning_card_df['date_payment_confirmed'] = cleaning_card_df['date_payment_confirmed'].apply(self.standardise_date_format)
        cleaning_card_df['date_payment_confirmed'] = pd.to_datetime(cleaning_card_df['date_payment_confirmed'], format='%Y-%m-%d', errors='coerce').dt.date

        return cleaning_card_df

    def convert_expiry_date(self, expiry_date):
        month, expiry_year = expiry_date.split('/')
        expiry_year = '20' + expiry_year # Expiry dates will only be present for the expiry_year `2000+` hence `20 + expiry_year`
        date_object = pd.Timestamp(f'{expiry_year}-{month}-01') + pd.DateOffset(months=1, days=-1)
        return date_object.date()
    
    def clean_store_data(self, store_details_df):
        cleaning_store_data_df = store_details_df.copy()

        # Dropping the column `lat` as it carries all [null] values.
        cleaning_store_data_df.drop('lat', axis=1, inplace=True)
        # Dropping the index row taken through to pgAdmin4.
        cleaning_store_data_df.drop('index', axis=1, inplace=True)

        # Dropping rows that are incorrect, 14 in total.
        incorrect_rows_with_wrong_country_codes = ~cleaning_store_data_df['country_code'].isin(['US','GB','DE'])
        cleaning_store_data_df = cleaning_store_data_df[~incorrect_rows_with_wrong_country_codes]

        # Using a regex to ensure staff_numbers now has only numbers inside
        cleaning_store_data_df['staff_numbers'] = cleaning_store_data_df['staff_numbers'].apply(self.returning_numbers_only)

        # Ensuring data which has a `country code` of the specified country has the corresponding continent in the `contient` column.
        cleaning_store_data_df.loc[(cleaning_store_data_df['country_code'] == 'DE') | (cleaning_store_data_df['country_code'] == 'GB'), 'continent'] = 'Europe'
        cleaning_store_data_df.loc[cleaning_store_data_df['country_code'] == 'US', 'continent'] = 'America'

        # Getting values labelled `N/A` to be set as NaN
        cleaning_store_data_df['longitude'] = pd.to_numeric(cleaning_store_data_df['longitude'], errors='coerce')
        cleaning_store_data_df['latitude'] = pd.to_numeric(cleaning_store_data_df['latitude'], errors='coerce')

        # Changing datatype of a column to the correct datatype.
        cleaning_store_data_df['longitude'] = cleaning_store_data_df['longitude'].astype(float)
        cleaning_store_data_df['latitude'] = cleaning_store_data_df['latitude'].astype(float)
        cleaning_store_data_df['staff_numbers'] = cleaning_store_data_df['staff_numbers'].astype('int32')
        cleaning_store_data_df['store_type'] = cleaning_store_data_df['store_type'].astype('category')
        cleaning_store_data_df['country_code'] = cleaning_store_data_df['country_code'].astype('category')

        # Setting opening_date column to be the correct datatype.
        cleaning_store_data_df['opening_date'] = cleaning_store_data_df['opening_date'].apply(self.standardise_date_format)
        cleaning_store_data_df['opening_date'] = pd.to_datetime(cleaning_store_data_df['opening_date'], errors='coerce').dt.date

        # Reordering columns
        new_column_order = ['store_code', 'store_type', 'address', 'locality', 'longitude', 'latitude', 'country_code', 'continent', 'staff_numbers', 'opening_date']
        cleaning_store_data_df = cleaning_store_data_df[new_column_order]

        return cleaning_store_data_df
    
    # Function to get all rows present with only numbers  
    def returning_numbers_only(self, uncleaned_number):                       
        discarding_letters_pattern = re.compile(r'\d+')
        matches = re.finditer(discarding_letters_pattern, uncleaned_number)
        cleaned_numbers = ''.join(match.group() for match in matches)
        return cleaned_numbers
    
    # Function to return all the date strings into the right format
    def standardise_date_format(self, date_strings):
        date_formats = ['%Y-%m-%d', '%B %Y %d', '%Y/%m/%d', '%Y %B %d']

        for date_format in date_formats:
            try:
                formatted_date = pd.to_datetime(date_strings, format=date_format, errors='raise')
                return formatted_date.strftime('%Y-%m-%d')
            except ValueError:
                continue

        return date_strings
    
    def clean_products_data(self, products_df):
        cleaning_products_df = products_df.copy()

        # Dropping any rows that are null & dropping the original index
        cleaning_products_df = cleaning_products_df.dropna()
        cleaning_products_df.drop("Unnamed: 0", axis=1, inplace=True)

        # Removing the `£` from the rows in the column.
        cleaning_products_df['product_price'] = cleaning_products_df['product_price'].str.replace('£', '', regex=False)
        
        # After ensuring there is no '£' sign within the column, the regex boolean will mark rows as True if they have anything except numbers and decimal points.
        incorrect_rows_with_invalid_prices = cleaning_products_df['product_price'].str.contains(r'[a-zA-Z]+')
        cleaning_products_df = cleaning_products_df[~incorrect_rows_with_invalid_prices]

        # Cleaning the weight column to convert to kg & convert to dtype `float`
        cleaning_products_df['weight'] = cleaning_products_df['weight'].apply(self.convert_product_weights)

        # Standardising the date & converting them to datetime objects.
        cleaning_products_df['date_added'] = cleaning_products_df['date_added'].apply(self.standardise_date_format)
        cleaning_products_df['date_added'] = pd.to_datetime(cleaning_products_df['date_added'], errors='coerce').dt.date
        
        # Converting columns to correct dtypes.
        cleaning_products_df['category'] = cleaning_products_df['category'].astype('category')
        cleaning_products_df['category'] = cleaning_products_df['category'].astype('category')
        cleaning_products_df['product_price'] = cleaning_products_df['product_price'].astype(float)
        cleaning_products_df['weight'] = cleaning_products_df['weight'].astype(float)

        # Renaming columns to show units at the top & to give better clarity on the columns rows.
        cleaning_products_df.rename(columns={'weight': 'weight (kg)'}, inplace=True)
        cleaning_products_df.rename(columns={'product_price': 'product_price (GBP)'}, inplace=True)
        cleaning_products_df.rename(columns={'removed': 'still_available'}, inplace=True)

        return cleaning_products_df

    def convert_product_weights(self, weight_string):
        # Using a regex to iterate over the rows removing anything that isn't a letter from the end of the string.
        weight_string = re.sub(r'[^a-zA-Z]+$', '', weight_string)

        # Converting all weights within the weight column to standardised kg.
        if " x " in weight_string:
            weight_string = weight_string.split(" x ")
            weight_string = float(weight_string[0]) * float(weight_string[1][:-1]) /1000
            return weight_string
        elif "kg" in weight_string:
            return float(weight_string.replace('kg', ''))
        elif "g" in weight_string: 
            return float(weight_string.replace('g','')) * 0.001
        elif "ml" in weight_string:
            return float(weight_string.replace('ml','')) * 0.001
        elif "oz" in weight_string:
            return float(weight_string.replace('oz', '')) / 35.274
        else:
            return weight_string

    def clean_orders_data(self, orders_df):
        
        # Creating a copy for best practice.
        cleaning_orders = orders_df.copy()

        # Dropping columns that are unnecessary or have the majority of rows with NULL in them.
        cleaning_orders = cleaning_orders.drop(['level_0', 'index', '1', 'first_name', 'last_name'], axis=1)

        # Card number was originally a bigint, this should be a string.
        cleaning_orders['card_number'] = cleaning_orders['card_number'].astype(str)

        # Product Quantity was originally a bigint, upon investigation product quantity only goes to 15, therefore can be stored as dtype int32.
        cleaning_orders['product_quantity'] = cleaning_orders['product_quantity'].astype('int32')
        return cleaning_orders
    
    def clean_date_details(self, date_times_df):

        # Creating a copy for best practice.
        cleaning_date_times_df = date_times_df.copy()

        # Dropping rows where the year is not exactly 4 digits long.
        year_pattern = re.compile(r'^\d{4}$')
        incorrect_rows = ~cleaning_date_times_df['year'].str.contains(year_pattern)
        cleaning_date_times_df = cleaning_date_times_df[~incorrect_rows]

        # Creating a new timestamp column from the 4 columns the timestamp was spread over.
        cleaning_date_times_df['sale_timestamp'] = (cleaning_date_times_df['year'] + '-' + cleaning_date_times_df['month'] + '-' + 
                                                       cleaning_date_times_df['day'] + ' ' + cleaning_date_times_df['timestamp'])
        
        # Dropping the columns that were used to concatenate the new timestamp
        cleaning_date_times_df.drop(['timestamp', 'month', 'year', 'day'], axis=1, inplace=True)

        # Changing the new timestamp to a datetime object.
        cleaning_date_times_df['sale_timestamp'] = pd.to_datetime(cleaning_date_times_df['sale_timestamp'])
        
        # Reordering columns
        new_column_order = ['sale_timestamp', 'time_period', 'date_uuid']
        cleaning_date_times_df = cleaning_date_times_df[new_column_order]

        return cleaning_date_times_df


if __name__ == '__main__':
    DataCleaning()