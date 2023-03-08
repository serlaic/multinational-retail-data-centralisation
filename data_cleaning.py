class DataCleaning(object):
    
    @classmethod
    def clean_user_data(cls):
        '''
        This method will upload the dataframe from read_rds_table method from DataExtractor class in data_extraction.py file
        and will clear all corrupted data by looking for 'NULL' values in first_name cell and
        also by checking if dates are correct. Returns "clean" dataframe with all corrupt columns deleted.
        '''

        from database_utils import DatabaseConnector
        from data_extraction import DataExtractor
        import pandas as pd
        import numpy as np

        users_table = DataExtractor().read_rds_table('legacy_users', DatabaseConnector('db_creds.yaml'))
       
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
        users_table_upd = users_table.dropna()
   #    users_table_upd = users_table_upd.drop(columns= ['first_name_issues', 'index'])

        return users_table_upd.drop(columns= ['first_name_issues', 'index'])

    @classmethod
    def clean_card_details(cls):
        '''
        This method will upload the dataframe from retrieve_pdf_dataf method from DataExtractor class in data_extraction.py file
        and will clear all corrupted data by looking for 'NULL' values in columns and
        also by checking if dates are correct.
        '''
        from data_extraction import DataExtractor
        import pandas as pd

        pdf_table = DataExtractor.retrieve_pdf_data()
        pdf_table_df = pdf_table[0]

        # Some useful functions to check overall information on the dataframe
        # print(pdf_table_df.dtypes)
        # print(pdf_table_df.info())
        # print(pdf_table_df.head())

        pdf_table_df['expiry_date'] = pd.to_datetime(pdf_table_df['expirty_date'], erros= 'coerce')



DataCleaning.clean_card_details()