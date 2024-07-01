'''
class DatabaseConnector

DatabaseConnector will be used to connect with and upload data to the database.
'''

class DatabaseConnector:
    '''
    Description

    
    Parameters:
    ----------
    parm: type
        description
    
    Attributes:
    ----------
    attribute: type
        description

    Methods:
    -------
    method()
        description
    '''
    def __init__(self, parm1='', parm2=''):
        #pass

        self.parm1 = parm1
        self.parm2 = parm2
        self.attr1 = ''
        self.attr2 = ''

    def method1(self, parm1='') -> None:
        '''
        Description

        Parameters:
        ----------
        parm: type
            description

        Returns:
        ----------
        parm: type
            description
        '''
        pass

        #return self.attr1, self.attr2

    def read_db_creds(self):
        import yaml
        
        with open('db_creds.yaml') as f:
            # use safe_load instead load
            dataMap = yaml.safe_load(f)
        
        ## testing-start -> remove
        # for key, value in dataMap.items():
        #     print(key, '\t', value)
        # print(dataMap.get('RDS_HOST'))
        # print(dataMap['RDS_HOST'])
        # testing-end

        return dataMap

    def init_db_engine(self):
        from sqlalchemy import create_engine

        creds = self.read_db_creds()
        
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = creds['RDS_HOST']
        USER = creds['RDS_USER']
        PASSWORD = creds['RDS_PASSWORD'] #None
        DATABASE = creds['RDS_DATABASE']
        PORT = creds['RDS_PORT']

        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

        return engine

    def list_db_tables(self):
        from sqlalchemy import inspect

        engine = self.init_db_engine()
        conn = engine.connect()

        inspector = inspect(engine)
        db_names_list = inspector.get_table_names()

        print(db_names_list)
        
        conn.close()

        return db_names_list
    
    def read_db_table(self, db_table):
        import pandas as pd

        engine = self.init_db_engine()
        #conn = engine.connect()
        table_load = pd.read_sql_table(db_table, engine)
        #conn.close()
        #print(table_load.head(5))

        return table_load
    
def read_rds_table(conn, db_table):

    table_load = conn.read_db_table(db_table)

    return table_load
    
if __name__ == '__main__':
    myconnection = DatabaseConnector()
    
    db_names_list = myconnection.list_db_tables()

    for db_table in db_names_list:
        # ['legacy_store_details', 'dim_card_details', 'legacy_users', 'orders_table']
        print(f'\nReading DB Table :: {db_table} \n' )
        table_load = read_rds_table(myconnection, db_table)
        print(table_load.head(5))


    