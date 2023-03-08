class DataCleaning():

    def __init__(self):
        pass
    
    def clean_user_data(cls):
        '''
        This method will upload the data from DataExtractor class in data_extraction.py file
        and will clear all corrupted data by looking for 'NULL' values in first_name cell and
        also by checking if dates are correct.
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
            print('All indexes are unique. No changes required')
        else:
            print('Some indexes have duplicates. Please check.')
            users_table['index_to_correct'] = users_table['index'].apply(lambda x: True 
                                                                         if x in users_table['index'].is_unique
                                                                         else False)
        users_table['first_name_issues'] = users_table['first_name'].apply(lambda x: pd.NaT 
                                                                           if 'NULL' in x
                                                                           else True)
        users_table = users_table[users_table['first_name_issues'].isin([True])]
        users_table['date_of_birth'] = pd.to_datetime(users_table['date_of_birth'], errors = 'coerce')
        users_table['join_date'] = pd.to_datetime(users_table['join_date'], errors = 'coerce')
        users_table_upd = users_table.dropna()
        users_table_upd = users_table_upd.drop(columns= ['first_name_issues', 'index'])

        return users_table_upd


