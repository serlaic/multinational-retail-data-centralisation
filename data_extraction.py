class DataExtractor():
    

    def __init__(self, table_name, db_conn):
        self.table_name = table_name
        self.db_conn = db_conn

    def read_rds_table(self):
        '''
        This function will take a table name as an argument and 
        return a pandas DataFrame.
        '''
        import pandas as pd
        return pd.read_sql_table(self.table_name, self.db_conn.init_db_engine())
    







        

        





        