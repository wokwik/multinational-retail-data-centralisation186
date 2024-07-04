'''
Class DataCleaning

This script will contain a class DataCleaning with methods to clean data from each of the data sources.
'''

import pandas as pd
import numpy as np
from datetime import datetime
import re

from database_utils import DatabaseConnector
from data_extraction import DataExtractor

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

        df.loc[df['store_code'].str.contains('WEB') & df['latitude'].isna(), 'latitude'] = "N/A"
        print(df[df['store_code'].str.contains('WEB') & df['longitude'].isna(), 'longitude'].values[0])
        print(df[df['store_code'].str.contains('WEB') & df['latitude'].isna(), 'latitude'].values[0])
        ## remove raws for both already existing and newly introduced NaN cells in the conversion above.
        dfc = df.dropna(axis=0, subset=['opening_date']).copy(deep=True) # or use inplace=True
        #dfc = df[~df.isnull().any(axis=1)].copy(deep=True) #removes all data rows because lat column is all null i.e. empty for all rows! 

        #dfc.to_csv('./data/stores_clean.csv', sep=',', index=False, header=True, encoding='utf-8')

        return dfc
    
    def clean_weight(self, string):
        def make_numeric(equation):
            if '+' in equation:
                y = equation.split('+')
                x = int(y[0].strip())+int(y[1].strip()) if "." not in equation else float(y[0].strip())+float(y[1].strip())
            elif '-' in equation:
                y = equation.split('-')
                x = int(y[0].strip())-int(y[1].strip()) if "." not in equation else float(y[0].strip())-float(y[1].strip())
            elif 'x' in equation or '*' in equation:
                y = equation.split('x') if 'x' in equation else equation.split('*')
                x = int(y[0].strip())*int(y[1].strip()) if "." not in equation else float(y[0].strip())*float(y[1].strip())
            elif '/' in equation:
                y = equation.split('/')
                x = int(y[0].strip())/int(y[1].strip()) if "." not in equation else float(y[0].strip())/float(y[1].strip())
                
            return x

        matches = ["+", "-", "x", "*", "/"]
        #string = '5.3g'
        if 'kg' in string.lower() and not any(x in string for x in matches):
            string.lower().replace("kg", "").replace(',', '.')
            string = re.sub('[^0-9.]', '', string)
            val = float(string)
        #string = '100g'
        elif 'g' in string.lower() and not any(x in string for x in matches):
            string = string.lower().replace("g", "").replace(',', '.')
            string = re.sub('[^0-9.]', '', string)
            val = float(string)/1000
        #string = '100ml'
        elif 'ml' in string and not any(x in string for x in matches):
            string = string.lower().replace("ml", "").replace(',', '.')
            string = re.sub('[^0-9.]', '', string)
            val = float(string)/1000
        #string = '10oz' 
        elif 'oz' in string.lower() and not any(x in string for x in matches):
            #1oz = 28.4ml
            string = string.lower().replace("oz", "").replace(',', '.')
            string = re.sub('[^0-9.]', '', string)
            val = float(string)*28.4/1000
        #string = '2 x 50g'
        elif any(x in string for x in matches):
            kg = 'kg' in string.lower()
            g = 'kg' not in string.lower() and 'g' in string.lower()
            oz = 'oz' in string.lower()
            string = string.lower().replace("kg", "").replace("g", "").replace("ml", "").replace("oz", "").replace(",",".")
            val = make_numeric(string)
            val = val / 1000 if g == True else val
            val = val * 28.4 / 1000 if oz == True else val
        else:
            val = np.nan

        return val
    
    def convert_product_weights(self, df):
        df['weight'] = df['weight'].apply(lambda x: self.clean_weight(str(x)))
        return df

    def clean_products_data(self, df):
        # name unamed column
        df.rename( columns={'Unnamed: 0':'index'}, inplace=True )

        #clean ¬£39.99
        df['product_price'] = df['product_price'].apply(lambda x: re.sub('[^0-9.]', '', x) if isinstance(x,str) else x)

        # clean date from none date or wrong and erronous formatted date
        df['date_added'] = df['date_added'].apply(lambda x: self.clean_date(x))

        ## remove raws for both already existing and newly introduced NaN cells in the conversion above.
        dfc = df[~df.isnull().any(axis=1)].copy(deep=True)

        return dfc
###
# run code
###
def clean_warehouse_users():
    connector = DatabaseConnector()
    extractor = DataExtractor()
    cleaner = DataCleaning()

    db_names_list = connector.list_db_tables()

    for db_table in db_names_list:
        # ['legacy_store_details', 'dim_card_details', 'legacy_users', 'orders_table']
        if db_table == 'legacy_users':
            print(f'\nReading DB Table :: {db_table} \n' )
            table_load = connector.read_db_table(db_table)
            # print(table_load.head(5))
            dfc_users = cleaner.clean_user_data(table_load)
            #print(dfc.head(5))
            dfc_users.to_csv('./data/legacy_users_clean.csv', sep=',', index=False, header=True, encoding='utf-8')
            connector.upload_to_db(dfc_users,'dim_users')
    
    return


def clean_pdf_cards_details():
    connector = DatabaseConnector()
    extractor = DataExtractor()
    cleaner = DataCleaning()

    pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    print(f'\nReading PDF :: {pdf_path} \n' )
    pdf_load = extractor.retrieve_pdf_data(pdf_path)
    
    dfc_pdf = cleaner.clean_card_data(pdf_load)
    connector.upload_to_db(dfc_pdf,'dim_card_details')
    
    return

def clean_api_stores():
    connector = DatabaseConnector()
    extractor = DataExtractor()
    cleaner = DataCleaning()

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
    dfc_stores.to_csv('./data/stores_clean.csv', sep=',', index=False, header=True, encoding='utf-8')
    connector.upload_to_db(dfc_stores,'dim_store_details')
    
    return

def clean_s3_products():
    connector = DatabaseConnector()
    extractor = DataExtractor()    
    cleaner = DataCleaning()

    df_s3 = extractor.extract_from_s3()
    dfkg_s3 = cleaner.convert_product_weights(df_s3)
    print(dfkg_s3.head())
    print(dfkg_s3.tail())
    
    dfc_products = cleaner.clean_products_data(dfkg_s3)

    #dfc_products.to_csv('./data/products_clean.csv', sep=',', index=False, header=True, encoding='utf-8')
    
    connector.upload_to_db(dfc_products,'dim_products')

    return

if __name__ == '__main__':
    # clean_warehouse_users()
    # clean_pdf_cards_details()
    # clean_api_stores()
    clean_s3_products()


    #pass