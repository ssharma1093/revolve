import argparse
from datetime import datetime
import pandas as pd
import json
import os
import sys
import traceback
import logging

def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')

    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
   
    return vars(parser.parse_args())


def main():
    try:
        
# using logging in append mode

        log_file = './cust_txn_log.log'
        logging.basicConfig(filename=log_file,format='%(asctime)s %(message)s') 
        logger=logging.getLogger() 
        logger.setLevel(logging.DEBUG) 
        
# getting the file/data location        
        params = get_params()
        customers_location = params['customers_location']
        products_location = params['products_location']
        transactions_location = params['transactions_location']
        output_location = params['output_location']
        
# checking locations and data if exists
        if os.path.exists(customers_location):
            logger.info("customers csv exists..") 
            cust_flag = True
        else:
            cust_flag = False
            logger.info("customers csv does not exists") 
        if os.path.exists(products_location):
            logger.info("products csv exists..") 
            prod_flag = True
        else:
            prod_flag = False
            logger.info("products csv does not exists") 
        if os.path.exists(transactions_location):
            logger.info("transactions folder exists..") 
            trans_flag = True
        else:
            trans_flag = False
            logger.info("transactions folder does not exists") 
        if os.path.exists(output_location):
            logger.info("output location exists") 
            out_flag = True
        else:
            out_flag = False
            logger.info("output location not exists.") 

# if any flag is false then exit
        if (cust_flag and prod_flag and trans_flag and out_flag) != True:
            logger.info("Incomplete data/location. Process terminating!!") 
            logger.info("-----------------------------------------------") 
            sys.exit()
        else:
# else load the customer and product data
            customers = pd.read_csv(customers_location)
            products = pd.read_csv(products_location)

"""
Idea is to create an empty dataframe. 
Iterate within the transaction location folderwise and collect all the transaction JSON files and bring it to a tabular form in dataframe.
"""
            temp_data = pd.DataFrame(columns=['customer_id','product_id','price','date_of_purchase'])
            for i in os.listdir(transactions_location):
                for j in os.listdir(transactions_location+'/'+i):

# the below line iterates through all the folders in transactions directory and appends all the transaction.json file as elements into the list 'decouple'
                    decouple = [json.loads(line) for line in open(transactions_location+'/'+i+'/'+j,'r')]

# for every element in the list 'decouple', we do the following and append to temp_data dataframe
                    for idx,val in enumerate(decouple):
 
# use JSON_normalize to flatten basket to individual product id and price into decoup_data dataframe 
                        decoup_data = pd.json_normalize(val['basket'])

# add customerid and date of purchase to decoup_data dataframe and append it to temp_data dataframe 
                        decoup_data['customer_id'] = val['customer_id']
                        decoup_data['date_of_purchase'] = val['date_of_purchase']
                        temp_data = temp_data.append(decoup_data)

# after appending all elements, merge customers and products with temp_data 
            temp_data = temp_data.merge(customers,on='customer_id',how='left')
            temp_data = temp_data.merge(products[['product_id','product_category']],on='product_id',how='left')

"""
the below lines of code, convert the date of purchase and extract week_no, year
"""
            temp_data['qty'] = temp_data['product_id']
            temp_data['date_of_purchase'] = pd.to_datetime(temp_data.date_of_purchase)
            temp_data['txn_date'] = pd.to_datetime(temp_data.date_of_purchase.dt.date)
            temp_data['year'] = temp_data.txn_date.dt.strftime('%Y')
            temp_data['week_no'] = temp_data.txn_date.dt.week
            temp_data['week_no'] = temp_data.week_no.astype(str)
            temp_data['week_no'] = temp_data['week_no'] + '_' + temp_data['year']

"""
the below code finally aggregates the data basis the week wise and writes to the output location in JSON format
"""

            for wkno in temp_data.week_no.unique():
                final_output = temp_data[
                    temp_data.week_no == wkno
                ].groupby(['customer_id','loyalty_score','product_id','product_category']).agg(
                    {'qty':'count'}
                ).reset_index().rename(columns = {'qty':'purchase_count'}).sort_values(
                    by=['customer_id','purchase_count']).reset_index().drop(columns='index')

                output_str = output_location+'week_'+wkno+'.json'

                final_output.to_json(output_str,orient='records')
            logger.info("Data processed!!") 
            logger.info("-----------------------------------------------") 

    except Exception:
        msg = traceback.format_exc()
        now = datetime.now()
        dt_string = now.strftime("%d%m%Y_%H%M%S")
        log_file = './cust_txn_log_error_'+dt_string+'.txt'
        with open(log_file, 'w') as f:
            f.write(msg)


if __name__ == "__main__":
    main()