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

        ## remove raws for both already existing and newly introduced NaN cells in the conversion above.
        dfc = df[~df.isnull().any(axis=1)].copy(deep=True) #df.dropna(how='any',axis=0, inplace=True) or use .copy() 

        return dfc
        #pass
    

    def clean_card_data(self, df):
        df['date_payment_confirmed'] = df['date_payment_confirmed'].apply(lambda x: self.clean_date(x))

        ## remove raws for both already existing and newly introduced NaN cells in the conversion above.
        dfc = df[~df.isnull().any(axis=1)].copy(deep=True)

        #dfc.to_csv('./data/card_details_clean.csv', sep=',', index=False, header=True, encoding='utf-8')

        return dfc
        #pass

    def called_clean_store_data(self, df):
        df['opening_date'] = df['opening_date'].apply(lambda x: self.clean_date(x))
        df['staff_numbers'] = df['staff_numbers'].apply(lambda x: re.sub('\D', '', x) if ~x.isnumeric() else x) #clean 3n9 for staff_numbers, which should be numeric
        
        # remove lat, which is empty and duplicate
        df.drop('lat', axis=1, inplace=True)

        #rearrange latitude next to longitude
        col = df.pop('latitude')
        df.insert(3, col.name, col)

        ## remove raws for both already existing and newly introduced NaN cells in the conversion above.
        dfc = df.dropna(axis=0, subset=['opening_date']).copy(deep=True) # or use inplace=True
        #dfc = df[~df.isnull().any(axis=1)].copy(deep=True) #removes all data because lat column is null i.e. empty for all rows! 

        dfc.to_csv('./data/stores_clean.csv', sep=',', index=False, header=True, encoding='utf-8')

        return dfc
    
if __name__ == '__main__':
    import pandas as pd
    from database_utils import DatabaseConnector
    from data_extraction import DataExtractor

    connector = DatabaseConnector()
    extractor = DataExtractor()    
    cleaner = DataCleaning()

    db_names_list = connector.list_db_tables()

    for db_table in db_names_list:
        # ['legacy_store_details', 'dim_card_details', 'legacy_users', 'orders_table']
        if db_table == 'legacy_users' and False:
            print(f'\nReading DB Table :: {db_table} \n' )
            table_load = connector.read_db_table(db_table)
            # print(table_load.head(5))
            dfc_users = cleaner.clean_user_data(table_load)
            #print(dfc.head(5))
            dfc_users.to_csv('./data/legacy_users_clean.csv', sep=',', index=False, header=True, encoding='utf-8')
            connector.upload_to_db(dfc_users,'dim_users')
    
    pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    print(f'\nReading PDF :: {pdf_path} \n' )
    pdf_load = extractor.retrieve_pdf_data(pdf_path)
    
    dfc_pdf = cleaner.clean_card_data(pdf_load)
    connector.upload_to_db(dfc_pdf,'dim_card_details')

    num_stores = extractor.list_number_of_stores()
    print('Number of Stores ::', num_stores)

    stores_list = []
    for x in range(0,num_stores):
        store_data = extractor.retrieve_stores_data(x)
        stores_list.append(store_data)

    df_stores = pd.DataFrame(stores_list)
    print(df_stores.head(5))

    dfc_stores = cleaner.called_clean_store_data(df_stores)

    print('\nClean Stores ::')
    print(dfc_stores.head(5))
    #dfc_stores.to_csv('./data/stores_clean.csv', sep=',', index=False, header=True, encoding='utf-8')
    #pass