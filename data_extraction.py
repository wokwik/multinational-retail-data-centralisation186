'''
class DataExtractor

This class will work as a utility class, in it you will be creating methods that help extract data from different data sources.
The methods contained will be fit to extract data from a particular data source, these sources will include CSV files, an API and an S3 bucket.
'''

import pandas as pd

class DataExtractor:
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
        self.parm1 = parm1
        self.parm2 = parm2
        self.attr1 = ''
        self.attr2 = ''
        #pass

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
        #return self.attr1, self.attr2
        pass

    def read_rds_table(self, connector, db_table, mode):
        import pandas as pd

        engine = connector.init_db_engine(mode)
        
        df_table = pd.read_sql_table(db_table, engine, index_col=None)

        return df_table
    
    def retrieve_pdf_data(self, pdf_path):
        import tabula

        df_list = tabula.read_pdf(pdf_path, pages = 'all', stream=False)
        #print('dfs length :: ',len(dfs))
        #print(dfs[0].head(5))
        df_pdf = pd.concat(df_list) # concat list of dataframes that represent individual pdf pages

        #print('df shape ::', df.shape)

        return df_pdf

    def list_number_of_stores(self):
        import requests
        import json

        url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
        headers = {
            "Content-Type": "application/json",
            "x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
        }

        response = requests.get(url, headers=headers)

        print(json.dumps(response.json()))
        return response.json()['number_stores']

    def retrieve_stores_data(self, store_id):
        import requests
        import json

        url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/" + str(store_id)
        headers = {
            "Content-Type": "application/json",
            "x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
        }

        response = requests.get(url, headers=headers)

        #print(json.dumps(response.json()))
        return response.json()
    
    def extract_from_s3(self):
        import boto3
        from botocore import UNSIGNED
        from botocore.config import Config

        BUCKET_NAME = 'data-handling-public' 
        KEY = 'products.csv' 
        s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))

        #create a list of 'Contect' objects from the s3 bucket
        #list_files = client.list_objects(Bucket=bucket)['Contents']
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=KEY)

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            df_s3 = pd.read_csv(response.get("Body"))
            return(df_s3)
        else:
            print(f"Unsuccessful S3 get_object response. Status - {status}")
            return None


if __name__ == '__main__':

    import pandas as pd
    from database_utils import DatabaseConnector

    extractor = DataExtractor()
    connector = DatabaseConnector()

    db_names_list = connector.list_db_tables(mode='remote')

    for db_table in db_names_list:
        # ['legacy_store_details', 'dim_card_details', 'legacy_users', 'orders_table']
        print(f'\nReading DB Table :: {db_table} \n' )
        df_table = extractor.read_rds_table(connector, db_table)
        print(df_table.head(5))
        #df_table.to_csv(f'./data/db__{db_table}.csv', sep=',', index=False, header=True, encoding='utf-8')


    pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    df_card = extractor.retrieve_pdf_data(pdf_path)
    print(df_card.head(5))
    #df_card.to_csv('./data/pdf__card_details.csv', sep=',', index=False, header=True, encoding='utf-8')


    num_stores = extractor.list_number_of_stores()
    print('Number of Stores ::', num_stores)

    stores_list = []
    for x in range(0,num_stores):
        store_data = extractor.retrieve_stores_data(x)
        stores_list.append(store_data)

    df_stores = pd.DataFrame(stores_list)
    print(df_stores.head(5))
    #df_stores.to_csv('./data/api__stores.csv', sep=',', index=False, header=True, encoding='utf-8')


    df_products = extractor.extract_from_s3()
    print(df_products.head(5))
    #df_products.to_csv('./data/s3__products.csv', sep=',', index=False, header=True, encoding='utf-8')
