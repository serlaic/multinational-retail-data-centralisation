class DataExtractor():

    def __init__(self):
        pass

    def read_rds_table(cls, table_name, db_conn):
        '''
        This function will take a table name as an argument and 
        return a pandas DataFrame.
        '''
        import pandas as pd
        return pd.read_sql_table(table_name, db_conn.init_db_engine())
    







        

        





        