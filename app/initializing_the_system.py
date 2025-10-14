
import pytz.exceptions
import requests.exceptions
from pandas import DataFrame

import curl_cffi

from app.db_orm import Stock
from .sqlspeaker import update_country, update_sector, update_currency, insert_stockspots, \
    financial_insert_function, update_industry, update_stock_object, update_stock_exchange

import yfinance as yf
from yfinance.exceptions import YFRateLimitError
import time
from datetime import date, timedelta
from typing import Optional
import datetime


# get market capacity







# get the history of the stock
def history(share: yf.ticker.Ticker,start_date: datetime.date,stock_id:int) -> Optional[DataFrame]:

    try:


        hist = share.history(start=start_date, end=datetime.datetime.now().date(), interval='1d')
        if hist.empty:
            return None

        hist.columns = [j.lower().replace(" ", "_") for j in hist.columns]
        hist.index = hist.index.date
        hist = hist[["open", "close", "dividends", "stock_splits", "volume",'high','low']]\
            .reset_index().rename(columns={'index': "spot_date", "open": "open_value", "close": "close_value", 'high': 'high_value', 'low': 'low_value'})
        hist["stock_id"] = int(stock_id)
        return hist[["stock_id", "spot_date", "open_value", "close_value", "dividends", "stock_splits","volume", "high_value", "low_value"]]

    except requests.exceptions.HTTPError | requests.exceptions.ConnectionError | pytz.exceptions.UnknownTimeZoneError:

        return None



# get the yearly balance_sheet of a stock
def get_annual_balancesheet(share: yf.ticker.Ticker) -> Optional[DataFrame]:
    try:
        b_s = share.balancesheet.transpose()
        if b_s.empty:
            return None
        b_s.columns = [j.lower().replace(" ", "_") for j in b_s.columns]
        return b_s


    except requests.exceptions.HTTPError:

        return None

    except requests.exceptions.ConnectionError:

        return None

    except pytz.exceptions.UnknownTimeZoneError:
        return None

# get the quarterly balance_sheet of a stock
def get_quarterly_balancesheet(share: yf.ticker.Ticker) -> Optional[DataFrame]:
    try:
        b_s = share.quarterly_balancesheet.transpose()
        if b_s.empty:
            return None
        b_s.columns = [j.lower().replace(" ", "_") for j in b_s.columns]
        return b_s

    except requests.exceptions.HTTPError | requests.exceptions.ConnectionError | pytz.exceptions.UnknownTimeZoneError:

        return None

# get the yearly cashflow of a stock
def get_annual_cashflow(share: yf.ticker.Ticker) -> Optional[DataFrame]:
    try:
        c_f = share.cashflow.transpose()
        if c_f.empty:
            return None
        c_f.columns = [j.lower().replace(" ", "_") for j in c_f.columns]
        return c_f


    except requests.exceptions.HTTPError | requests.exceptions.ConnectionError | pytz.exceptions.UnknownTimeZoneError:

        return None

# get the quarterly cashflow of a stock
def get_quarterly_cashflow(share: yf.ticker.Ticker) -> Optional[DataFrame]:
    try:
        c_f = share.quarterly_cashflow.transpose()
        if c_f.empty:
            return None
        c_f.columns = [j.lower().replace(" ", "_") for j in c_f.columns]
        return c_f


    except:
        return None

# For income_statement
# get the yearly income_statement of a stock
def get_annual_income_statement(share: yf.ticker.Ticker) -> Optional[DataFrame]:
    try:
        i_s = share.incomestmt.transpose()
        if i_s.empty:
            return None
        i_s.columns = [j.lower().replace(" ", "_") for j in i_s.columns]
        return i_s


    except requests.exceptions.HTTPError:

        return None



    except requests.exceptions.ConnectionError:

        return None



    except pytz.exceptions.UnknownTimeZoneError:

        return None

# get the quarterly income_statement of a stock
def get_quarterly_income_statement(share: yf.ticker.Ticker) -> Optional[DataFrame]:
    try:
        i_s = share.quarterly_incomestmt.transpose()
        if i_s.empty:
            return None
        i_s.columns = [j.lower().replace(" ", "_") for j in i_s.columns]
        return i_s

    except:

        return None

# the main sync function
def info_generate(symbol_list: list[Stock]):

    # that function gets a list of symbols and updates the market capacity
    # and updates the history of
    trial = 0
    index = 0

    while index< len(symbol_list):
        try:
            stock = symbol_list[index]
            stock_data = yf.Ticker(stock.symbol)
            stock_id = stock.stock_id
            stock_symbol = stock.symbol
           
            inf = stock_data.info
            


            hist = history(share=stock_data, start_date=date(2020, 1, 1), stock_id=stock_id)

            if type(hist) == DataFrame:

                insert_stockspots(hist_all=hist, stock_id=stock_id)
           
            values = {
                'shares_issued': inf.get('impliedSharesOutstanding'),
                'country_id': update_country( inf.get('country')),
                'industry_id': update_industry( inf.get('industry')),
                'sector_id': update_sector( inf.get('sector')),
                'cur_id': update_currency( inf.get("financialCurrency")),
                'se_id': update_stock_exchange( inf.get("fullExchangeName")),
                'company_name': inf.get('longName'),
                'information': inf.get('longBusinessSummary')
            }
            update_stock_object(stock_id=stock_id, values=values)

            #annual Balancesheet
            ann_bs = get_annual_balancesheet(share=stock_data)

            if isinstance(ann_bs, DataFrame):

                financial_insert_function(df=ann_bs, table_name="annual_balance_sheet",stock_id=stock_id)
            else:
                pass

            # Quarterly Balancesheet
            quarter_bs = get_quarterly_balancesheet(share=stock_data)
            if isinstance(quarter_bs, DataFrame):
                financial_insert_function(df=quarter_bs, table_name="quarterly_balance_sheet",stock_id=stock_id)
            else:
                pass

            # Annual Income Statement
            ann_is = get_annual_income_statement(share=stock_data)

            if isinstance(ann_is, DataFrame):
                financial_insert_function(df=ann_is, table_name="annual_income_statement",stock_id=stock_id)
            else:
                pass

            # Quarterly Income Statement
            quarter_is = get_quarterly_income_statement(share=stock_data)

            if isinstance(quarter_is, DataFrame):
                financial_insert_function(df=quarter_is, table_name="quarterly_income_statement",stock_id=stock_id)
            else:
                pass

            # Annual Cashflow
            ann_cf = get_annual_cashflow(share=stock_data)
            if isinstance(ann_cf, DataFrame):
                financial_insert_function(df=ann_cf, table_name="annual_cash_flow",stock_id=stock_id)
            else:
                pass

            # Quarterly Cashflow
            quarter_cf = get_quarterly_cashflow(share=stock_data)
            if isinstance(quarter_cf, DataFrame):
                financial_insert_function(df=quarter_cf, table_name="quarterly_cash_flow",stock_id=stock_id)
            else:
                pass

            print(f'all done for ticker {stock_symbol}',flush=True)
            index += 1
            trial = 0
            return "success"


        except (YFRateLimitError, requests.exceptions.Timeout, curl_cffi.requests.exceptions.Timeout) as e:
            print(f"Rate limit error with ticker {stock.symbol}: {e}",flush=True)
            print("Pausing for 90 seconds before retrying...", flush=True)
            time.sleep(90)
            if trial < 3:
                trial += 1
            else:
                trial = 0
                index += 1
                return "nack"



        # except Exception as e:
        #     print(f"Unhandled error for {stock.symbol}: {e}")
        #     index += 1
        #     trial = 0


