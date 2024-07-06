'''
class DatabaseConnector

DatabaseConnector will be used to connect with and upload data to the database.
'''

class DatabaseConnector:
    '''
    Description
        The class is responsible for establishing utility methods for connecting to and interacting with remote (AWS RDS) or local PosgtreSQL databases.

    Attributes:
    ----------
    None

    Methods:
    -------
    read_db_creds()
        reads database credentials from a yaml file.
    
    init_db_engine():
        initialises the connection to the database and returns back engine object to use for interacting with the database.
    
    list_db_tables():
        lists available tables to interact with in the database.

    upload_to_db():
        is used to create and write data into the database using a the passed table name.
    '''
    def __init__(self):
        pass

    def read_db_creds(self, mode='remote'):
        '''
        Description
            reads database credentials from a yaml file.

        Parameters:
        ----------
        mode: string
            accepts either 'remote' or 'local' values.
            is used to determine which connection is required to be established.

        Returns:
        ----------
        dataMap: dictionary
            contains the database connection parameters required for establishing the connection.
        '''
        import yaml
        
        if mode == 'local':
            with open('db_creds_local.yaml') as f:
                # use safe_load instead load
                dataMap = yaml.safe_load(f)
        elif mode == 'remote':
            with open('db_creds_remote.yaml') as f:
                # use safe_load instead of load
                dataMap = yaml.safe_load(f)
        
        ## testing-start -> remove
        # for key, value in dataMap.items():
        #     print(key, '\t', value)
        # print(dataMap.get('RDS_HOST'))
        # print(dataMap['RDS_HOST'])
        # testing-end

        return dataMap

    def init_db_engine(self, mode='remote'):
        '''
        Description
            initialises the connection to the database and returns back engine object to use for interacting with the database.

        Parameters:
        ----------
        mode: string
            accepts either 'remote' or 'local' values.
            is used to determine which connection is required to be established.

        Returns:
        ----------
        engine: sqlalchmy engine object
            is used for establishing connectiona and interacting with the database
        '''
        from sqlalchemy import create_engine

        creds = self.read_db_creds(mode)
        
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = creds['RDS_HOST']
        USER = creds['RDS_USER']
        PASSWORD = creds['RDS_PASSWORD'] #None
        DATABASE = creds['RDS_DATABASE']
        PORT = creds['RDS_PORT']

        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

        return engine

    def list_db_tables(self, mode='remote'):
        '''
        Description
            lists available tables to interact with in the database.

        Parameters:
        ----------
        mode: string
            accepts either 'remote' or 'local' values.
            is used to determine which connection is required to be established.

        Returns:
        ----------
        db_names_list: list
            list of table names available to interact with in the established database connection.
        '''
        from sqlalchemy import inspect

        engine = self.init_db_engine(mode)
        conn = engine.connect()

        inspector = inspect(engine)
        db_names_list = inspector.get_table_names()

        print(db_names_list)
        
        conn.close()

        return db_names_list
    
    def upload_to_db(self, dfc, table_name):
        '''
        Description
            is used to create and write data into the database using a the passed table name.

        Parameters:
        ----------
        dfc: Pandas Dataframe
            The dataframe to be written in to a table in the database.
            Ideally the dataframe should free from missing, wrong formatted, and erroneous cells.

        Returns:
        ----------
        None
        '''
        print(f'Writing DB Table :: {table_name} \n' )
        #print(dfc.head(5))
        engine = self.init_db_engine(mode='local')
        dfc.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        
        return
    
if __name__ == '__main__':
    connector = DatabaseConnector()
    
    db_names_list = connector.list_db_tables(mode='remote')

    print(db_names_list)


    