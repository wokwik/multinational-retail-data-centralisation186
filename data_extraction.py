'''
class DataExtractor

This class will work as a utility class, in it you will be creating methods that help extract data from different data sources.
The methods contained will be fit to extract data from a particular data source, these sources will include CSV files, an API and an S3 bucket.
'''

import pandas as pd

class DataExtractor:
    '''
    This class is responsible for extracting data fromn different data sources and different data formats.
    
    Parameters:
    ----------
    parm: type
        description
    
    Attributes:
    ----------
    None

    Methods:
    -------
    read_rds_table()
        Reads AWS RDS table into a Dataframe.
    retrieve_pdf_data()
        Reads PDF table into a Dataframe.
    list_number_of_stores()
        Return back the number of stores from API endpoint.
    retrieve_stores_data()
        Reads stores data from API end point into a Dataframe.
    extract_from_s3()
        Reads CSV file from an S3 bucket into a Dataframe.
    extract_from_json()
        Reads a json file stored in an S3 bucket into a Dataframe.
    '''
    
    def __init__(self):
        pass

    def read_rds_table(self, connector, db_table, mode):
        '''
        Description
            Reads AWS RDS table into a Dataframe.

        Parameters:
        ----------
        connector: DatabaseConnector class instance
            The class is responsible for establishing utility methods for connecting to and interacting with remote (AWS RDS) or local PosgtreSQL databases.
        db_table: string
            The name of the database table to read into a dataframe.
        mode: string
            accepts either 'remote' or 'local' values.
            is used to determine which connection is required to be established.

        Returns:
        ----------
        df_table: Pandas Dataframe
            A dataframe containing the data from the database table.
        '''
        import pandas as pd

        engine = connector.init_db_engine(mode)
        
        df_table = pd.read_sql_table(db_table, engine, index_col=None)

        return df_table
    
    def retrieve_pdf_data(self, pdf_path):
        '''
        Description
            Reads PDF table into a Dataframe.

        Parameters:
        ----------
        pdf_path: string
            The url for the location of the PDF file on the internet.

        Returns:
        ----------
        df_pdf: Pandas Dataframe
            A dataframe containing the data from the PDF file.
        '''
        import tabula # type: ignore

        df_list = tabula.read_pdf(pdf_path, pages = 'all', stream=False)
        #print('dfs length :: ',len(dfs))
        #print(dfs[0].head(5))
        df_pdf = pd.concat(df_list) # concat list of dataframes that represent individual pdf pages

        #print('df shape ::', df.shape)

        return df_pdf

    def list_number_of_stores(self,url, headers):
        '''
        Description
            Return back the number of stores from API endpoint.

        Parameters:
        ----------
        url: string
            The API endpoint url or location on the internet.
        headers: dictionary
            A dictionary containing the API key for gaining access and quering the endpoint for the required data.

        Returns:
        ----------
        number_stores: int
            The number of stores available for this retail company.
        '''
        import requests

        response = requests.get(url, headers=headers)
        
        # import json
        # print(json.dumps(response.json()))
        return response.json()['number_stores']

    def retrieve_stores_data(self, url, headers, store_id):
        '''
        Description
            Reads stores data from API end point into a Dataframe.

        Parameters:
        ----------
        url: string
            The API endpoint url or location on the internet.
        headers: dictionary
            A dictionary containing the API key for gaining access and quering the endpoint for the required data.
        store_id:
            The store id, for the endpoint to return back information about.

        Returns:
        ----------
        store_data: dictionary
            Contains the store information for the store_id.
        '''
        import requests

        response = requests.get(url+str(store_id), headers=headers)

        #import json
        #print(json.dumps(response.json()))
        return response.json()
    
    def extract_from_s3(self, bucket_name, file_key):
        '''
        Description
            Reads CSV file from an S3 bucket into a Dataframe.

        Parameters:
        ----------
        bucket_name: string
            The AWS Bucket name where the file is stored.
        file_key: string
            The file key for the CSV stored in the AWS Bucket.
        Returns:
        ----------
        df_s3: Pandas Dataframe
            A dataframe containing the data from the CSV file inside the S3 Bucket.
        '''
        import boto3
        from botocore import UNSIGNED
        from botocore.config import Config

        s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))

        #create a list of 'Contect' objects from the s3 bucket
        #list_files = client.list_objects(Bucket=bucket)['Contents']
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            df_s3 = pd.read_csv(response.get("Body"))
            return(df_s3)
        else:
            print(f"Unsuccessful S3 get_object response. Status - {status}")
            return None

    def extract_from_json(self, json_url):
        '''
        Description
            Reads a json file stored in an S3 bucket into a Dataframe.

        Parameters:
        ----------
        json_url: string
            A url for the location of the JSON file on the internet

        Returns:
        ----------
        df_s3: Pandas Dataframe
            A dataframe containing the data from the JSON file on the internet including inside the S3 Bucket.
        '''

        df_s3 = pd.read_json(json_url)

        return df_s3
        
        #pass 

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
