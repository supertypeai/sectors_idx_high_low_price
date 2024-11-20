import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

from dotenv import load_dotenv
from supabase import create_client
import os

# Initiate Supabase DB
load_dotenv()
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase = create_client(url, key)

# Get active company list
response = supabase.table('idx_active_company_profile').select('symbol').execute()
act_symbol = pd.DataFrame(response.data)

all_df = pd.DataFrame()

# Fetch historical all time price data for every stock
for i in act_symbol["symbol"]:
    try:
        # Fetch data from yahoo finance
        stock = yf.Ticker(i).history(interval="1d",period="max") 

        stock = stock.reset_index()
        stock = stock.sort_values("Date")
        stock = stock[["Date","High","Low"]]

        # Get ATH and ATL price data

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

        # Get 52_w_high and 52_w_low
        timezone = stock['Date'].dt.tz
        start_date = (datetime.now(tz=timezone) - timedelta(weeks=52))

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

        # Combine all price status data
        result = pd.concat([stock_high,stock_low,price_52w_high,price_52w_low])
        result["symbol"] = i

        result["price"] = result["price"].astype('int')

        result["date"] = result['date'].dt.date.astype('str')

        result.reset_index(inplace=True,drop=True)

        all_df = pd.concat([all_df,result])

        print(f"Finish for stock {i}")
    except:
        print(f"Error for stock {i}")

# Collect existing all time price data
response = supabase.table('idx_all_time_price').select('*').execute()
at_price_hist = pd.DataFrame(response.data)

# Remove unchanged all time price data
update_df = pd.concat([all_df,at_price_hist])
update_df = update_df[~update_df.duplicated(keep=False)]

# Upload the data into supabase
for record in update_df.to_dict(orient="records"):
    try:
        supabase.table('idx_all_time_price').upsert(record).execute()
    except:
        print("Financial report for the symbol is already available in the database")
