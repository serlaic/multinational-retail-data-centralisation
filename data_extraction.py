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
        return tabula.read_pdf('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf', pages='all', multiple_tables = False)






        

        





        