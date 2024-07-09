'''
Class DataCleaning

This script will contain a class DataCleaning with methods to clean data from each of the data sources.
'''

import pandas as pd
import numpy as np
from datetime import datetime
import re

class DataCleaning:
    '''
    Description
        The class is responsible for providing a number of methos for cleaning dataframes produced from quering raw data from different sources.

    Methods:
    -------
    clean_date()
        A common method for cleaning date strings, it deals with empty values, NaNs, wrongly formatted dates, and erroneous dates. 
    clean_user_data()
        A method for cleaning users data by correcting dates column values, and email address format.
    clean_card_data()
        A method for cleaning card data, by correcting dates and removing raws with missing values.
    called_clean_store_data()
        A method for cleaning store data by correcting for dates, removing missing values, removing erroneous raws, removing empty columns, reordering columns, and correcting staff id values.
    clean_weight()
        A common method for converting weight value from none-KG values, such as ml, g, and oz, into KG units.
        Also, it evaluates strings "2 x 50g" into its total KG value 0.1KG.
        Used in the .apply lambda options
    convert_product_weights()
        A method for normalising the data column to be of values in KG units.
    clean_products_data()
        A method for cleaning products data such as correcting for dates, removing unnecessary characters in price, and giving names to columns without any. 
    clean_orders_data()
        A method for cleaning orders data by removing PII data such as first and last names, and by remvoing empty columns.
    clean_events_data()
        A method for cleaning events data such as correcting year, month, and day columns by removing raws with non numeric cells, and raws with NaN values.
    '''
    def __init__(self):
        pass


    def clean_date(self,x):
        '''
        Description
            Reads AWS RDS table into a Dataframe.

        Parameters:
        ----------
        x: string
            The date string to be checked for validity, wrong format, or empty values.

        Returns:
        ----------
        date: Date type
            The date string cleaned and converted to Date type or Null if it can't be converted to a true date.
        '''
        date = ''
        try:
            if type(x) == float and np.isnan(x):
                date = np.nan
            elif '-' in x:
                date = datetime.strptime(x, '%Y-%m-%d').date()
            elif '/' in x:
                date = datetime.strptime(x, '%Y/%m/%d').date()
            elif bool(re.search('\d{4}', x)):
                year = re.findall(r'\d{4}', x)[0]
                day = re.findall(r'\d{2}', x)[0]
                month = re.findall(r'[a-zA-Z]+', x)[0]
                date_str = year + '-' + month + '-' + day
                date = datetime.strptime(date_str, '%Y-%B-%d').date()
            else:
                # deals with x = '7KGJ3C5TSW', or not matching any of the above options
                date = np.nan
        except:
            # deals with errors when x='2328-BAER-23' i.e. does not convert to a proper/true date
            date = np.nan
        finally:
            return date
    
    def clean_user_data(self,df):
        '''
        Description
            A method for cleaning users data by correcting dates column values, and email address format.

        Parameters:
        ----------
        df: Pandas Dataframe
            The dataframe holding users data.

        Returns:
        ----------
        dfc: Pandas Dataframe
            The dataframe holding cleaned users data
        '''
        # drop index column generated by salalchamy
        df.drop(['index'], axis=1, inplace=True)
        
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
        '''
        Description
            A method for cleaning card data, by correcting dates and removing raws with missing values.

        Parameters:
        ----------
        df: Pandas Dataframe
            The dataframe holding cards data.

        Returns:
        ----------
        dfc: Pandas Dataframe
            The dataframe holding cleaned cards data
        '''
        # clean date. erroneous and wrong dates are changed to null
        df['date_payment_confirmed'] = df['date_payment_confirmed'].apply(lambda x: self.clean_date(x))

        ## remove raws for both already existing and newly introduced NaN cells in the conversion above.
        dfc = df[~df.isnull().any(axis=1)].copy(deep=True)

        return dfc
        #pass

    def called_clean_store_data(self, df):
        '''
        Description
            A method for cleaning store data by correcting for dates, removing missing values, removing erroneous raws, removing empty columns, reordering columns, and correcting staff id values.

        Parameters:
        ----------
        df: Pandas Dataframe
            The dataframe holding store details data.

        Returns:
        ----------
        dfc: Pandas Dataframe
            The dataframe holding cleaned store details data
        '''
        # clean date. erroneous and wrong dates are changed to null
        df['opening_date'] = df['opening_date'].apply(lambda x: self.clean_date(x))

        # clean 3n9 for staff_numbers, which should be numeric. remove non numeric characters so it becomes 39
        df['staff_numbers'] = df['staff_numbers'].apply(lambda x: re.sub('\D', '', x) if isinstance(x, str) and not x.isnumeric() else x)
        
        # remove lat, which is empty and duplicate
        df.drop('lat', axis=1, inplace=True)

        # rearrange latitude next to longitude
        col = df.pop('latitude')
        df.insert(3, col.name, col)

        # ensure null latitude value for the web store is N/A similar to other not aplicable columns e.g. logitude
        df.loc[df['store_code'].str.contains('WEB') & df['latitude'].isna(), 'latitude'] = "N/A"
        # print(df[df['store_code'].str.contains('WEB') & df['longitude'].isna(), 'longitude'].values[0])
        # print(df[df['store_code'].str.contains('WEB') & df['latitude'].isna(), 'latitude'].values[0])

        ## remove raws for both already existing and newly introduced NaN cells in the conversion above.
        dfc = df.dropna(axis=0, subset=['opening_date']).copy(deep=True) # or use inplace=True
        #dfc = df[~df.isnull().any(axis=1)].copy(deep=True) #removes all data rows because lat column is all null i.e. empty for all rows!

        return dfc
    
    def clean_weight(self, string):
        '''
        Description
            A common method for converting weight value from none-KG values, such as ml, g, and oz, into KG units.
            Also, it evaluates strings "2 x 50g" into its total KG value 0.1KG.
            Used in the .apply lambda options

        Parameters:
        ----------
        string: String
            The string holding weight information.

        Returns:
        ----------
        x: float
            The numeric value of the string in KG or Null.
        '''
        def make_numeric(equation):
            # evaluate an equation in string format to its result. similar to using eval(), which is not safe
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
        '''
        Description
            A method for normalising the data column to be of values in KG units.

        Parameters:
        ----------
        df: Pandas Dataframe
            The dataframe holding products data.

        Returns:
        ----------
        dfc: Pandas Dataframe
            The dataframe holding converted products weights into KG.
        '''
        # change weight values and change to normalised kg equivalents for all rows
        df['weight'] = df['weight'].apply(lambda x: self.clean_weight(str(x))) #
        return df

    def clean_products_data(self, df):
        '''
        Description
            A method for cleaning products data such as correcting for dates, removing unnecessary characters in price, and giving names to columns without any.

        Parameters:
        ----------
        df: Pandas Dataframe
            The dataframe holding products data.

        Returns:
        ----------
        dfc: Pandas Dataframe
            The dataframe holding cleaned products data
        '''
        
        # name unamed column
        df.rename( columns={'Unnamed: 0':'index'}, inplace=True )

        # clean ¬£39.99
        df['product_price'] = df['product_price'].apply(lambda x: re.sub('[^0-9.]', '', x) if isinstance(x,str) else x)

        # clean date from none date or wrong and erronous formatted date
        df['date_added'] = df['date_added'].apply(lambda x: self.clean_date(x))

        ## remove raws for both already existing and newly introduced NaN cells in the conversion above.
        dfc = df[~df.isnull().any(axis=1)].copy(deep=True)

        return dfc
    

    def clean_orders_data(self, df):
        '''
        Description
            A method for cleaning orders data by removing PII data such as first and last names, and by remvoing empty columns.

        Parameters:
        ----------
        df: Pandas Dataframe
            The dataframe holding orders data.

        Returns:
        ----------
        dfc: Pandas Dataframe
            The dataframe holding cleaned orders data
        '''

        # drop PII data first and last names
        df.drop(['first_name'], axis=1, inplace=True)
        df.drop(['last_name'], axis=1, inplace=True)
        #drop empty column 1
        df.drop(columns=['1'], axis=1, inplace=True)
        #drop useless column level_0
        df.drop(columns='level_0',inplace=True)

        # remove non numeric characters from string like: 
        # "?4971858637664481"
        # "???3554954842403828"
        # "??4654492346226715"
        # "?3544855866042397"
        df['card_number'] = df['card_number'].apply(lambda x: re.sub('[^0-9]', '', x) if isinstance(x,str) else x)

        return df

    def clean_events_data(self, df):

        '''
        Description
            A method for cleaning events data such as correcting year, month, and day columns by removing raws with non numeric cells, and raws with NaN values.

        Parameters:
        ----------
        df: Pandas Dataframe
            The dataframe holding events data.

        Returns:
        ----------
        dfc: Pandas Dataframe
            The dataframe holding cleaned events data
        '''
        # work on a copy
        dfc = df.copy(deep=True)

        #convert all columns that need to be numeric individually
        dfc['year'] = dfc['year'].apply(pd.to_numeric, errors='coerce')
        dfc['month'] = dfc['month'].apply(pd.to_numeric, errors='coerce')
        dfc['day'] = dfc['day'].apply(pd.to_numeric, errors='coerce')
        dfc['timestamp'] = dfc['timestamp'].apply(pd.to_datetime, format='%H:%M:%S', errors='coerce')
        #df['hour'] = pd.to_datetime(df['time'], format='%H:%M:%S').dt.hour

        # remove all NaN and Nulls
        dfc.dropna(how='any',axis=0, inplace=True) 

        # could be done using apply:
        # df['year'] = df['year'].apply(lambda x:np.nan if pd.isna(x) or not str(x).isnumeric() else x)
        # df['month'] = df['month'].apply(lambda x: np.nan if pd.isna(x) or not str(x).isnumeric() else x)
        # df['day'] = df['day'].apply(lambda x: np.nan if pd.isna(x) or not str(x).isnumeric() else x)

        # could be done in one go (convert and remove generate and existing NaNs), but not ideal, as gives less control and is not composable!
        #dfc = df[pd.to_numeric(df['year'], errors='coerce').notnull()].copy(deep=True) #removes all NaN and Null at once for all rows. but need to break it down.
        return dfc
        #pass


if __name__ == '__main__':
    #run_warehouse_users()
    #run_pdf_cards_details()
    #run_api_stores()
    #run_s3_products()
    #run_warehouse_orders()
    #run_json_events()

    pass