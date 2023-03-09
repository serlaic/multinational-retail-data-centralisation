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
        api_key = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        # requests information
        response = requests.get('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', headers= api_key)
        # converts response into dictionary
        store_number = response.json()
        # returns the number of stores
        return store_number['number_stores']

    @classmethod
    def retrieve_stores_data(cls):
        '''
        This method return take and retrieve store endpoint as 
        an egument and extracts all the stores from the API.
        '''
        import requests
        import pandas as pd
        api_key = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        store_data_df = pd.DataFrame()
        # for loop to go through all the pages and add them into the store_data_df dataframe
        for store_number in range(0,DataExtractor().list_number_of_stores()):
            # requests information
            response = requests.get(f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}", headers= api_key)
            # converts response into dictionary
            store_data_json = response.json()
            # converts dictionary into dataframe
            store_data_df_single = pd.DataFrame.from_dict([store_data_json])
            # adds single dataframe into final dataframe after each iterration
            store_data_df = pd.concat([store_data_df, store_data_df_single],ignore_index= True)
        # returns final dataframe with all the data
        return store_data_df

        

        





        