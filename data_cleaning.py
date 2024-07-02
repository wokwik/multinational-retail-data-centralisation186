'''
Class DataCleaning

This script will contain a class DataCleaning with methods to clean data from each of the data sources.
'''
import numpy as np
import re
from datetime import datetime

class DataCleaning:
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
    def clean_date(self,x):
        
        date = ''
        try:
            if type(x) == float and np.isnan(x):
                date = np.nan
            elif '-' in x: #x.contains('-'):
                date = datetime.strptime(x, '%Y-%m-%d').date()
            elif '/' in x: #x.contains('/'):
                date = datetime.strptime(x, '%Y/%m/%d').date()
            elif bool(re.search('\d{4}', x)):
                year = re.findall(r'\d{4}', x)[0]
                day = re.findall(r'\d{2}', x)[0]
                month = re.findall(r'[a-zA-Z]+', x)[0]
                date_str = year + '-' + month + '-' + day
                date = datetime.strptime(date_str, '%Y-%B-%d').date()
            else:
                #deals with x = '7KGJ3C5TSW', not match any of the above options
                date = np.nan
        except:
            # deals with errors when x='2328-BAER-23' i.e. does not convert to a proper/true date
            date = np.nan
        finally:
            return date
    
    def clean_user_data(self,df):

        #print(table_load.head(5))
        ## save file to csv for data clean in Jupyter Notebook
        #df.to_csv('./data/legacy_users.csv', sep=',', index=False, header=True, encoding='utf-8')

        ## convert date strings to proper date type columns and resolve formatting and erronous values 
        #pd.to_datetime(df['join_date'], format='%Y-%m-%d') #won't work with wrong format or NA string values. so need custom apply()
        df['date_of_birth'] = df['date_of_birth'].apply(lambda x: self.clean_date(x))
        df['join_date'] = df['join_date'].apply(lambda x: self.clean_date(x))

        ## if email address does not contain @, then replace with NaN for removal in a later step
        df['email_address'] = df['email_address'].apply(lambda x: x if type(x) != float and '@' in x else np.nan)

        ## remove raws for already existing and newly introduced NaN cells in the conversion above.
        dfc = df[~df.isnull().any(axis=1)].copy(deep=True)

        return dfc
        #pass

if __name__ == '__main__':
    from database_utils import DatabaseConnector

    myConnection = DatabaseConnector()
    dataCleaner = DataCleaning()

    db_names_list = myConnection.list_db_tables()

    for db_table in db_names_list:
        # ['legacy_store_details', 'dim_card_details', 'legacy_users', 'orders_table']
        if db_table == 'legacy_users':
            print(f'\nReading DB Table :: {db_table} \n' )
            table_load = myConnection.read_db_table(db_table)
            # print(table_load.head(5))
            dfc = dataCleaner.clean_user_data(table_load)
            print(dfc.head(5))

    #pass