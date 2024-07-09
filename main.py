"""
This is the main file of the program. 

You can run it to choose which part of the pipeline you woud want to trigger.
"""

import pandas as pd

from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

#######
# run code
#######

def run_warehouse_users():
    # Code to connect, extract, and clean warehouse users data.

    connector = DatabaseConnector()
    extractor = DataExtractor()
    cleaner = DataCleaning()

    db_names_list = connector.list_db_tables()

    for db_table in db_names_list:
        # ['legacy_store_details', 'dim_card_details', 'legacy_users', 'orders_table']
        if db_table == 'legacy_users':
            print(f'\nRetrieving DB Table :: {db_table}' )
            df_db = extractor.read_rds_table(connector, db_table, mode='remote')
            # print(table_load.head(5))
            
            print(f'\nTable to CSV :: {db_table} - raw' )
            df_db.to_csv('./data/db__legacy_users_raw.csv', sep=',', index=False, header=True, encoding='utf-8')

            print(f'\nCleaning DB Table :: {db_table}' )
            dfc_users = cleaner.clean_user_data(df_db)
            #print(dfc.head(5))

            print(f'\nTable to CSV :: {db_table} - clean' )
            dfc_users.to_csv('./data/db__legacy_users_clean.csv', sep=',', index=False, header=True, encoding='utf-8')
            
            print(f'\nTable to Local DB :: {db_table}' )
            connector.upload_to_db(dfc_users,'dim_users')
    
    return

def run_warehouse_orders():
    # Code to connect, extract, and clean warehouse orders data.

    connector = DatabaseConnector()
    extractor = DataExtractor()
    cleaner = DataCleaning()

    db_names_list = connector.list_db_tables()

    for db_table in db_names_list:
        # ['legacy_store_details', 'dim_card_details', 'legacy_users', 'orders_table']
        if db_table == 'orders_table':
            print(f'\nRetrieving DB Table :: {db_table}' )
            df_db = extractor.read_rds_table(connector, db_table, mode='remote')
            #print(df_db.head(5))
            
            print(f'\nTable to CSV :: {db_table} - raw' )
            df_db.to_csv('./data/db__orders_raw.csv', sep=',', index=False, header=True, encoding='utf-8')

            print(f'\nCleaning DB Table :: {db_table}' )
            dfc_orders = cleaner.clean_orders_data(df_db)
            #print(dfc_orders.head(5))

            print(f'\nTable to CSV :: {db_table} - clean' )
            dfc_orders.to_csv('./data/db__orders_clean.csv', sep=',', index=False, header=True, encoding='utf-8')
            
            print(f'\nTable to Local DB :: {db_table}' )
            connector.upload_to_db(dfc_orders,'orders_table')
    
    return

def run_pdf_cards_details():
    # Code to connect, extract, and clean warehouse cards data.

    connector = DatabaseConnector()
    extractor = DataExtractor()
    cleaner = DataCleaning()

    pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    
    print(f'\nRetrieving PDF :: {pdf_path}' )
    df_pdf = extractor.retrieve_pdf_data(pdf_path)
    
    print(f'\nPDF to CSV :: {pdf_path} - raw' )
    df_pdf.to_csv('./data/pdf__card_details_raw.csv', sep=',', index=False, header=True, encoding='utf-8')

    print(f'\nCleaning PDF :: {pdf_path}' )
    dfc_cards = cleaner.clean_card_data(df_pdf)
    
    print(f'\nPDF to CSV :: {pdf_path} - clean' )
    dfc_cards.to_csv('./data/pdf__card_details_clean.csv', sep=',', index=False, header=True, encoding='utf-8')

    print(f'\nPDF to Local DB :: {pdf_path}' )
    connector.upload_to_db(dfc_cards,'dim_card_details')
    
    return

def run_api_stores():
    # Code to connect, extract, and clean warehouse stores data.

    connector = DatabaseConnector()
    extractor = DataExtractor()
    cleaner = DataCleaning()

    url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    headers = {
        "Content-Type": "application/json",
        "x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
    }
    
    print(f'\nReading API  :: Number of Stores' )
    num_stores = extractor.list_number_of_stores(url,headers)
    print('Number of Stores ::', num_stores)
    
    url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
    headers = {
        "Content-Type": "application/json",
        "x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
    }

    print(f'\nRetrieving API :: Individual Store Data' )
    stores_list = []
    for store_id in range(0,num_stores):
        store_data = extractor.retrieve_stores_data(url, headers, store_id)
        stores_list.append(store_data)

    df_api = pd.DataFrame(stores_list)
    #print(df_api.head(5))

    print(f'\nAPI to CSV :: Stores - raw' )
    df_api.to_csv('./data/api__stores_raw.csv', sep=',', index=False, header=True, encoding='utf-8')

    print(f'\nCleaning API :: Stores' )
    dfc_stores = cleaner.called_clean_store_data(df_api)

    # print('\nClean Stores ::')
    # print(dfc_stores.head(5))

    print(f'\nAPI to CSV :: Stores - clean' )
    dfc_stores.to_csv('./data/api__stores_clean.csv', sep=',', index=False, header=True, encoding='utf-8')
    
    print(f'\nAPI to Local DB :: Stores' )
    connector.upload_to_db(dfc_stores,'dim_store_details')
    
    return

def run_s3_products():
    # Code to connect, extract, and clean warehouse products data.

    connector = DatabaseConnector()
    extractor = DataExtractor()    
    cleaner = DataCleaning()

    BUCKET_NAME = 'data-handling-public' 
    KEY = 'products.csv'

    print(f'\Retrieving S3 CSV :: Products \n' )
    df_s3 = extractor.extract_from_s3(BUCKET_NAME,KEY)

    print(f'\S3 to CSV:: Stores - raw \n' )
    df_s3.to_csv('./data/s3__products_raw.csv', sep=',', index=False, header=True, encoding='utf-8')

    print(f'\Cleaning S3 :: Products \n' )
    dfkg_s3 = cleaner.convert_product_weights(df_s3)
    # print(dfkg_s3.head())
    # print(dfkg_s3.tail())
    dfc_products = cleaner.clean_products_data(dfkg_s3)

    print(f'\S3 to CSV:: Stores - clean \n' )
    dfc_products.to_csv('./data/s3__products_clean.csv', sep=',', index=False, header=True, encoding='utf-8')
    
    print(f'\S3 to Local DB :: Stores \n' )
    connector.upload_to_db(dfc_products,'dim_products')

    return

def run_json_events():
    # Code to connect, extract, and clean warehouse events data.

    connector = DatabaseConnector()
    extractor = DataExtractor()    
    cleaner = DataCleaning()

    json_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'

    print(f'\Retrieving S3 Json :: Events \n' )
    df_json = extractor.extract_from_json(json_url)

    print(df_json.info())
    print(type(df_json['month'][0]))
    print(type(df_json['year'][0]))
    print(f'\S3 to CSV:: Events - raw \n' )
    df_json.to_csv('./data/s3__events_raw.csv', sep=',', index=False, header=True, encoding='utf-8')

    print(f'\Cleaning S3 Json :: Events \n' )
    dfc_events = cleaner.clean_events_data(df_json)

    print(f'\S3 to CSV:: Events - clean \n' )
    dfc_events.to_csv('./data/s3__events_clean.csv', sep=',', index=False, header=True, encoding='utf-8')
    
    print(f'\S3 to Local DB :: Events \n' )
    connector.upload_to_db(dfc_events,'dim_date_times')

    pass

if __name__ == '__main__':
    
    choice = ""

    while choice != "q":
        
        choice = input("""Press a number to run (press q to quit):
                            1- run_warehouse_users()
                            2- run_pdf_cards_details()
                            3- run_api_stores()
                            4- run_s3_products()
                            5- run_warehouse_orders()
                            6- run_json_events()
                            7- run entire pipeline! It takes some time to complete!\n""")

        if choice == '1':
            print("You chose 1: run_warehouse_users()")
            run_warehouse_users()
        elif choice == '2':
            print("You chose 2: run_pdf_cards_details()")
            run_pdf_cards_details()
        elif choice == '3':
            print("You chose 3: run_api_stores()")
            run_api_stores()
        elif choice == '4':
            print("You chose 4: run_s3_products()")
            run_s3_products()
        elif choice == '5':
            print("You chose 5: run_warehouse_orders()")
            run_warehouse_orders()
        elif choice == '6':
            print("You chose 6: run_json_events()")
            run_json_events()
        elif choice == '7':
            print("You chose 7: Run the entire pipeline at once!")
            run_warehouse_users()
            run_pdf_cards_details()
            run_api_stores()
            run_s3_products()
            run_warehouse_orders()
            run_json_events()
        elif choice == 'q':
            print("You chose to quit program. Goodbye!")
        else:
            print("Error: That is not a valid input!")
        #run_warehouse_users()
        #run_pdf_cards_details()
        #run_api_stores()
        #run_s3_products()
        #run_warehouse_orders()
        #run_json_events()

    #pass