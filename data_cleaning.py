class DataCleaning(object):
    
    @classmethod
    def clean_user_data(cls):
        '''
        This method uploads the dataframe from read_rds_table
        method from DataExtractor class in data_extraction.py file
        and clears all corrupted data by looking for 'NULL' 
        values in first_name cell and also by checking if dates are 
        correct. Returns "clean" dataframe with all corrupt columns deleted.
        '''

        from database_utils import DatabaseConnector
        from data_extraction import DataExtractor
        import pandas as pd

        users_table = DataExtractor().read_rds_table('legacy_users', DatabaseConnector('db_creds.yaml'))

        # Some useful functions to check overall information on the dataframe
        # users_table.dtypes
        # users_table.info()
        # users_table.head()
        # new_data.sum()

        if users_table['index'].is_unique == True:
            pass
        else:
            print('Some indexes have duplicates. Please check.')
            users_table['index_to_correct'] = users_table['index'].apply(lambda x: True 
                                                                         if x in users_table['index'].is_unique
                                                                         else False)
        users_table['first_name_issues'] = users_table['first_name'].apply(lambda x: pd.NaT 
                                                                           if 'NULL' in x
                                                                           else True)
        users_table = users_table[users_table['first_name_issues'].isin([True])]
        users_table['date_of_birth'] = pd.to_datetime(users_table['date_of_birth'], errors= 'coerce')
        users_table['join_date'] = pd.to_datetime(users_table['join_date'], errors= 'coerce')
        users_table = users_table.dropna()
        users_table_upd = users_table.drop(columns= ['first_name_issues', 'index'])

        return users_table_upd

    @classmethod
    def clean_card_details(cls):
        '''
        This method uploads the dataframe from retrieve_pdf_dataf method 
        from DataExtractor class in data_extraction.py file and clears all 
        corrupted data by looking for 'NULL' values in columns and also by 
        checking if dates are correct.
        '''
        from data_extraction import DataExtractor
        import pandas as pd

        users_table_pdf = DataExtractor.retrieve_pdf_data()
        users_table_pdf['date_payment_confirmed'] = pd.to_datetime(users_table_pdf['date_payment_confirmed'], errors = 'coerce')
        users_table_pdf = users_table_pdf.dropna()
        
        return users_table_pdf
    
    @classmethod
    def called_clean_stored_data(cls):
        '''
        This method uploads the dataframe from retrieve_stores_data method 
        from DataExtractor class in data_extraction.py file
        and clears all corrupted data by looking for 'NULL' values 
        in columns and also by checking if dates are correct.
        '''
        from data_extraction import DataExtractor
        import pandas as pd

        stores_table_api = DataExtractor.retrieve_stores_data()
        stores_table_api['opening_date'] = pd.to_datetime(stores_table_api['opening_date'], errors = 'coerce')
        stores_table_api['latitude'] = stores_table_api['latitude'].fillna("N/A")
        stores_table_api = stores_table_api.drop("lat", axis= 1) # same as .drop(columns= "lat")
        stores_table_api = stores_table_api.dropna()
        
        return stores_table_api

    @classmethod
    def convert_product_weights(cls):
        '''
        This method cleans weight column in the DataFrame products_df.
        Converts all grams to kg and all ml to g with 1:1 ratio.
        Removes all excess characters then represents the weights as float.
        '''
        from data_extraction import DataExtractor

        s3_server = "s3://data-handling-public/products.csv"
        stores_df = DataExtractor.extract_from_s3(s3_server)

    # function to be used to remove 'kg','g','ml','.' strings from dataframe
    # and convert g and ml into kg where g = ml and kg = g/1000 kg = ml/1000
        def check_weight(x):
            if 'kg' in x:
                x = x.replace('kg', '')
            elif 'g' in x:
                x = x.replace('g', '')
                x = x.replace('.', '')
                try:
                    x = float(x)
                    x = x / 1000
                except:
                    x = x.split('x')
                    x_0 = float(x[0])
                    x_1 = float(x[1])
                    x = x_0 * x_1
            elif 'ml' in x:
                x = x.replace('ml','')
                try:
                    x = float(x)
                    x = x / 1000
                except:
                    pass
            return x
    
        # drops all the columns with null data
        stores_df = stores_df.dropna()

        # checks the weight and converts it into kg
        stores_df['weight'] = stores_df['weight'].apply(check_weight)

        return stores_df
    
    @classmethod
    def clean_products_data(cls):
        import pandas as pd

        stores_df = DataCleaning.convert_product_weights()

        # function to convert date from a specific format
        def check_time(x):
            from datetime import datetime
            try:
                x = x.replace(' ','-')
                x = datetime.strptime(x, '%Y-%B-%d').date()
            except:
                try:
                    x = x.replace(' ','')
                    x = datetime.strptime(x, '%B-%Y-%d').date()
                except:
                    pass
            return x
        
        # checks if date is correct and converts it if required
        stores_df['date_added'] = pd.to_datetime(stores_df['date_added'], errors = 'ignore')
        stores_df['date_added'] = stores_df['date_added'].apply(check_time)
        stores_df['date_added'] = pd.to_datetime(stores_df['date_added'], errors = 'coerce')

        # drops all the columns with null data
        stores_df = stores_df.dropna()

        return stores_df

    @classmethod
    def clean_order_details(cls):
        '''
        This method uploads the dataframe from read_rds_table
        method from DataExtractor class in data_extraction.py file
        and clears all data. Removes first_name,last_name,and 1 columns
        This table is acting as the source of truth for sales.
        '''
        
        from database_utils import DatabaseConnector
        from data_extraction import DataExtractor

        orders_table = DataExtractor().read_rds_table('orders_table', DatabaseConnector('db_creds.yaml'))
        orders_table = orders_table.drop(columns = ['first_name', '1', 'last_name', 'level_0'])

        return orders_table

