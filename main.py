import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, date
import argparse


from dotenv import load_dotenv
from supabase import create_client
import os

import logging
from imp import reload

# Initiate log file
LOG_FILENAME = 'fetch_all_time_price.log'

def initiate_logging(LOG_FILENAME):
    reload(logging)

    formatLOG = '%(asctime)s - %(levelname)s: %(message)s'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO, format=formatLOG)
    logging.info('Program started')

def get_data(ticker):
    # Fetch data from yahoo finance
    stock = yf.Ticker(ticker).history(interval="1d",period="max",auto_adjust=False) 

    stock = stock.reset_index()
    stock = stock.sort_values("Date")
    stock = stock[["Date","High","Low"]]
    stock["Date"] = pd.to_datetime(stock["Date"].dt.date)

    return stock

def get_all_time_price(stock):
    ## Find the max value in 'high' column and min value in 'low' column
    max_high = stock['High'].max()
    min_low = stock['Low'].min()

    # Select rows with max in 'high' or min in 'low'
    stock_high = stock[(stock['High'] == max_high)][["Date","High"]].drop_duplicates(subset=['High'])
    stock_high.columns = ["date","price"]
    stock_high["type"] = "all_time_high"

    stock_low = stock[(stock['Low'] == min_low)][["Date","Low"]].drop_duplicates(subset=['Low'])
    stock_low.columns = ["date","price"]
    stock_low["type"] = "all_time_low"

    return stock_high,stock_low

def get_52w_price(stock):
    # Get 52_w_high and 52_w_low
    start_date = (datetime.now() - timedelta(weeks=52))

    ## Filter the DataFrame for rows within the last 52 weeks
    df_52w = stock[stock['Date'] >= start_date]

    high_52w = df_52w['High'].max()
    low_52w = df_52w['Low'].min()

    # Select rows with max in 'high' or min in 'low'
    price_52w_high = df_52w[(df_52w['High'] == high_52w)][["Date","High"]].drop_duplicates(subset=['High'])
    price_52w_high.columns = ["date","price"]
    price_52w_high["type"] = "52_w_high"

    price_52w_low = df_52w[(df_52w['Low'] == low_52w)][["Date","Low"]].drop_duplicates(subset=['Low'])
    price_52w_low.columns = ["date","price"]
    price_52w_low["type"] = "52_w_low"

    return price_52w_high,price_52w_low

def get_90d_price(stock):
    # Get 90_d_high and 90_d_low
    start_date_week = (datetime.now() - timedelta(days=90))

    ## Filter the DataFrame for rows within the last 52 weeks
    df_90d = stock[stock['Date'] >= start_date_week]

    high_90d = df_90d['High'].max()
    low_90d = df_90d['Low'].min()

    # Select rows with max in 'high' or min in 'low'
    price_90d_high = df_90d[(df_90d['High'] == high_90d)][["Date","High"]].drop_duplicates(subset=['High'])
    price_90d_high.columns = ["date","price"]
    price_90d_high["type"] = "90_d_high"

    price_90d_low = df_90d[(df_90d['Low'] == low_90d)][["Date","Low"]].drop_duplicates(subset=['Low'])
    price_90d_low.columns = ["date","price"]
    price_90d_low["type"] = "90_d_low"

    return price_90d_high,price_90d_low

def get_ytd_price(stock):
    # Get ytd_high and ytd_low
    start_date_ytd = f"{datetime.now().year}-01-01"

    ## Filter the DataFrame for rows within the last 52 weeks
    df_ytd = stock[stock['Date'] >= start_date_ytd]

    high_ytd = df_ytd['High'].max()
    low_ytd = df_ytd['Low'].min()

    # Select rows with max in 'high' or min in 'low'
    price_ytd_high = df_ytd[(df_ytd['High'] == high_ytd)][["Date","High"]].drop_duplicates(subset=['High'])
    price_ytd_high.columns = ["date","price"]
    price_ytd_high["type"] = "ytd_high"

    price_ytd_low = df_ytd[(df_ytd['Low'] == low_ytd)][["Date","Low"]].drop_duplicates(subset=['Low'])
    price_ytd_low.columns = ["date","price"]
    price_ytd_low["type"] = "ytd_low"

    return price_ytd_high,price_ytd_low

def main():
    # Initiate Supabase DB
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    supabase = create_client(url, key)
    
    initiate_logging(LOG_FILENAME)

    parser = argparse.ArgumentParser(description="Batch Data")
    parser.add_argument('batch', type=int, help='Specify the batch you want to run (1-3')

    args = parser.parse_args()

    # Get active company list
    response = supabase.table('idx_active_company_profile').select('symbol').execute()
    act_symbol = pd.DataFrame(response.data)
    
    all_df = pd.DataFrame()

    if args.batch = 1
        act_symbol = act_symbol.iloc[0:400,:]
    elif args.batch = 2:
        act_symbol = act_symbol.iloc[400:800,:]
    elif args.batch = 3:
        act_symbol = act_symbol.iloc[800:,:]
    
# Fetch historical all time price data for every stock
    for i in act_symbol["symbol"]:
                
        stock = get_data(i)
        stock_high, stock_low = get_all_time_price(stock)
        price_52w_high, price_52w_low = get_52w_price(stock)
        price_90d_high, price_90d_low = get_90d_price(stock)
        price_ytd_high, price_ytd_low = get_ytd_price(stock)
        
        # Combine all price status data
        result = pd.concat([stock_high,stock_low,price_52w_high,price_52w_low,price_90d_high,price_90d_low,price_ytd_high,price_ytd_low])
        result["symbol"] = i
    
        result["price"] = result["price"].astype('int')
    
        result["date"] = result['date'].astype('str')
    
        result.reset_index(inplace=True,drop=True)
    
        all_df = pd.concat([all_df,result])
    
        print(f"Finish for stock {i}")

    # Collect existing all time price data
    response = supabase.table('idx_all_time_price').select('*').execute()
    at_price_hist = pd.DataFrame(response.data)
    
    # Remove unchanged all time price data
    update_df = pd.merge(
        all_df, 
        at_price_hist, 
        how='left', 
        indicator=True
    ).query('_merge == "left_only"').drop('_merge', axis=1)
    
    # Upload the data into supabase
    for record in update_df.to_dict(orient="records"):
        try:
            supabase.table('idx_all_time_price').upsert(record).execute()
        except:
            print("Financial report for the symbol is already available in the database")
    
    logging.info(f"{update_df.shape[0]} data are updated on {date.today()}, the stocks are {update_df.to_json(orient='records')}")

if __name__ == "__main__":
    main()
