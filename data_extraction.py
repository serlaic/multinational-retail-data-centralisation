class DataExtractor(object):
    
    @classmethod
    def read_rds_table(cls, table_name, db_conn):
        '''
        This function will take a table name as an argument and 
        return a pandas DataFrame.
        '''
        import pandas as pd
        return pd.read_sql_table(table_name, db_conn.init_db_engine())
    
    @classmethod
    def retrieve_pdf_data(cls):
        '''
        This function will take a pdf file and return pandas dataframe
        '''
        import tabula
        pdf_table_df = tabula.read_pdf('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf', pages='all', multiple_tables = False)
        return pdf_table_df[0]

    @classmethod
    def list_number_of_stores(cls):
        '''
        This method return the number of stores to extract. 
        It takes in the number of stores endpoint and header 
        dictionary as an argument
        '''
        import requests
        import json
        params = {"format": "json"}
        api_key = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        response = requests.get('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',params=params, headers= api_key)
        number_stores = response.json()
        return number_stores['number_stores']

    @classmethod
    def retrieve_stores_data(cls):
        '''
        This method return take and retrieve store endpoint as 
        an egument and extracts all the stores from the API.
        '''
DataExtractor.list_number_of_stores()
        

        





        