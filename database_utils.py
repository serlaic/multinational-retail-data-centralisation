class DatabaseConnector:

    def __init__(self):
        pass  

    def red_db_creds(self):
        '''
        This function reads the credentials from yaml file and returns the dictionary of credentials

        '''
        import yaml

        with open('db_creds.yaml', 'r') as stream:
            yaml_data = yaml.safe_load(stream)    
        return yaml_data

    def init_db_engine(self):
        '''
        This function will read the credentials from the return of read_db_creds and initialise and 
        return and sqlaclhemy databse engine.
        '''
        from sqlalchemy import create_engine

        yaml_data = self.red_db_creds()
        engine = create_engine(f"{yaml_data['DATABASE_TYPE']}://{yaml_data['RDS_USER']}:{yaml_data['RDS_PASSWORD']}@{yaml_data['RDS_HOST']}:{yaml_data['RDS_PORT']}/{yaml_data['RDS_DATABASE']}")
        engine.connect()
        return engine

    def list_db_tables(self):
        '''
        This function will list all the tables in a database so you know 
        which tables you can extract data from
        '''
        from sqlalchemy import inspect

        inspector = inspect(self.init_db_engine())
        tablet_list = inspector.get_table_names()
        return tablet_list
