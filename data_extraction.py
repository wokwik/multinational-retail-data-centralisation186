'''
class DataExtractor

This class will work as a utility class, in it you will be creating methods that help extract data from different data sources.
The methods contained will be fit to extract data from a particular data source, these sources will include CSV files, an API and an S3 bucket.
'''
import random

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

    def retrieve_pdf_data(self, pdf_path):
        import tabula
        import pandas as pd

        dfs = tabula.read_pdf(pdf_path, pages = 'all', stream=False)
        #print('dfs length :: ',len(dfs))
        #print(dfs[0].head(5))
        df = pd.concat(dfs) # concat list of dataframes that represent individual pdf pages

        #print('df shape ::', df.shape)
        #df.to_csv('./data/card_details.csv', sep=',', index=False, header=True, encoding='utf-8')
        return df

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
    
if __name__ == '__main__':

    import pandas as pd

    extractor = DataExtractor()

    pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    #df_pdf = extractor.retrieve_pdf_data(pdf_path)

    num_stores = extractor.list_number_of_stores()
    print('Number of Stores ::', num_stores)

    stores_list = []
    for x in range(0,num_stores):
        store_data = extractor.retrieve_stores_data(x)
        stores_list.append(store_data)

    df_stores = pd.DataFrame(stores_list)
    print(df_stores.head(5))

    #df_stores.to_csv('./data/stores.csv', sep=',', index=False, header=True, encoding='utf-8')
