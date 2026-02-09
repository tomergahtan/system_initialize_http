import os
import io
from sqlalchemy import select, create_engine,update,  text
from sqlalchemy.dialects.postgresql import insert
from .db_orm import (Stock, Currency, Sector, Country,
                        AnnualBalanceSheet, AnnualIncomeStatement, AnnualCashFlow,
                        QuarterlyBalanceSheet, QuarterlyIncomeStatement,
                         QuarterlyCashFlow, Irrelevant,
                        Industry, StockExchange)
from datetime import datetime
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import DBAPIError
import logging
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

psql =os.getenv('PSQL')

engine_dest = create_engine(psql)
Session = sessionmaker(bind=engine_dest)

TABLE_MODLE_MAP = {
    "annual_balance_sheet": AnnualBalanceSheet,
    "quarterly_balance_sheet": QuarterlyBalanceSheet,
    "annual_income_statement": AnnualIncomeStatement,
    "quarterly_income_statement": QuarterlyIncomeStatement,
    "annual_cash_flow": AnnualCashFlow,
    "quarterly_cash_flow": QuarterlyCashFlow,
}


# connect to the database investments
with Session() as session:
    try:
        se = session.query(StockExchange).all()
        country_set = { country.country_name: country.country_id for country in session.query(Country).all()}
        sector_set = { sector.sector_name:sector.sector_id for sector in session.query(Sector).all()}
        currency_set = { currency.cur_name:currency.cur_id for currency in session.query(Currency).all()}
        industry_set = { industry.industry_name:industry.industry_id for industry in session.query(Industry).all()}
        stock_exchange_set = { stock_exchange.se_name:stock_exchange.se_id for stock_exchange in se}
        se_currency_set = { stock_exchange.se_name:stock_exchange.currency_id for stock_exchange in se}
      
    except DBAPIError as e:
        print(f"Error: {e}")
    finally:
        session.close()



def update_stock_exchange(stock_exchange_name: str,currency:str):
    if not stock_exchange_name:
        return None

    currency_id = update_currency(currency)
    if not stock_exchange_name in stock_exchange_set and stock_exchange_name:
        with Session() as session:
            try:
                stock_exchange = StockExchange(se_name=stock_exchange_name,currency_id=currency_id)
                session.add(stock_exchange)
                session.commit()
                stock_exchange_set[stock_exchange_name] = stock_exchange.se_id
                se_currency_set[stock_exchange_name] = currency_id
            except Exception as e:
                print(f"Error during stock exchange insertion {stock_exchange_name}: {e}",flush=True)
                session.rollback()
            finally:
                session.close()
    elif currency_id and currency_id != se_currency_set.get(stock_exchange_name):
        with Session() as session:
            try:
                session.execute(update(StockExchange).where(StockExchange.se_name == stock_exchange_name).values(currency_id=currency_id))
                session.commit()
                se_currency_set[stock_exchange_name] = currency_id
            except Exception as e:
                print(f"Error during stock exchange currency update {stock_exchange_name}: {e}",flush=True)
                session.rollback()
            finally:
                session.close()
    return stock_exchange_set.get(stock_exchange_name)

def update_country(country_name: str):
    if not country_name:
        return None
        # Attempt to insert the country (trigger prevents duplicates)
    if not country_name in country_set and country_name:
        with Session() as session:
            try:
                country = Country(country_name=country_name)
                session.add(country)
                session.commit()
                country_set[country_name] = country.country_id

            except Exception as e:
                session.rollback()
                print(f"Error during country insertion {country_name}: {e}",flush=True)
            finally:
                session.close()
    return country_set.get(country_name)

# insert stocks
def update_currency(cur: str):
    if not cur:
        return None
    """
    Update the currency of a stock in the database.
    Note: this Currency  is the currency of the finencial data of the stock
    and not the currency of the stock itself.
    """
        # Attempt to insert the currency (trigger prevents duplicates)
    if not cur in currency_set and cur:
        with Session() as session:
            try:
                currency = Currency(cur_name=cur)
                session.add(currency)
                session.commit()
                currency_set[cur] = currency.cur_id
                # print(f"Added currency {cur}")
            except Exception as e:
                print(f"Error during currency insertion with stock {cur}: {e}",flush=True)
                session.rollback()
            finally:
                session.close()
    return currency_set.get(cur)

def update_sector(sector_name:str):
    """
    Update the sector of a stock in the database.
    """
    if not sector_name:
        return None
        # Attempt to insert the sector (trigger prevents duplicates)
    if not sector_name in sector_set and sector_name:
        with Session() as session:
            try:
                sector = Sector(sector_name=sector_name)
                session.add(sector)
                session.commit()
                sector_set[sector_name] = sector.sector_id
                # print(f"Added sector {sector_name}")
            except Exception as e:
                print(f"Error during sector insertion {sector_name}: {e}",flush=True)
                session.rollback()
            finally:
                session.close()
    return sector_set.get(sector_name)

def update_industry(industry_name:str):
    """
    Update the industry of a stock in the database.
    """
    if not industry_name:
        return None

        # Attempt to insert the industry (trigger prevents duplicates)
    if not industry_name in industry_set and industry_name:
        with Session() as session:
            try:
                industry = Industry(industry_name=industry_name)
                session.add(industry)
                session.commit()
                industry_set[industry_name] = industry.industry_id
            except Exception as e:
                print(f"Error during industry insertion {industry_name}: {e} ",flush=True)
                session.rollback()
            finally:
                session.close()
    return industry_set.get(industry_name)

def update_stock_object(stock_id: int, values: dict):
    
    values['last_update'] = datetime.now().date()
    values = {k: v if v else None for k, v in values.items()}
    with Session() as session:
        try:
            session.execute(update(Stock).where(Stock.stock_id == stock_id).values(**values))   
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error during stock object update for stock {stock_id}: {e}",flush=True)
        finally:
            session.close()
            
def insert_into_irrelevant(stock_id:int):
    with Session() as session:
        try:
            irrelevant = Irrelevant(stock_id=stock_id)
            session.add(irrelevant)
            session.commit()
        except Exception as e:
            session.rollback()

        finally:
            session.close()
# update the market capacity of a stock

def _copy_df(raw_conn, df: pd.DataFrame, table: str, cols: list[str]) -> None:
    """COPY FROM STDIN ל־table (טבלת temp)."""
    if df.empty:
        return
    buf = io.StringIO()
    df.loc[:, cols].to_csv(buf, index=False, header=False, date_format="%Y-%m-%d", na_rep="\\N")
    buf.seek(0)
    cur = raw_conn.cursor()
    cur.copy_expert(
        f"COPY {table} ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')",
        buf,
    )

def insert_stockspots( hist_all: pd.DataFrame, stock_id: int) -> None:
    """
    מחליף את כל הנתונים של מניה אחת (stock_spots, dividends, splitting) לפי hist_all.
    - מוחק כל מה שקיים למניה הזו בשלוש הטבלאות.
    - מכניס מחדש רק את מה שיש ב-hist עבור המניה.
    - אם אין דיבידנדים/פיצולים ב-hist → גם יימחקו כולם (אין הכנסה).

    hist_all חייב להכיל לפחות את העמודות:
      stock_id, spot_date, open_value, close_value, high_value, low_value, volume,
      dividends (מספרי; אם >0), stock_splits (מספרי; אם >0)
    """

    # 1) סינון למניה המבוקשת ובניית DF-ים בלי iterrows (הכי מהיר)
    df = hist_all.loc[hist_all["stock_id"] == stock_id].copy()
    if df.empty:
        return  # אין מה לעדכן

    spots = (
        df[["stock_id","spot_date","open_value","close_value","high_value","low_value","volume"]]
        .dropna(subset=["stock_id","spot_date"], how="any")  # שומר על תקינות PK
        .drop_duplicates(subset=["stock_id","spot_date"])
    )
    # תיקון טעות כתיב אם קרה: (למקרה שעמודה שגויה)
    if "spot_id" in spots.columns:
        spots = spots.rename(columns={"spot_id": "stock_id"})

    divs = (
        df.loc[df["dividends"] > 0, ["stock_id", "spot_date", "dividends"]]
          .rename(columns={"spot_date": "div_date", "dividends": "amount"})
          .dropna(subset=["div_date"])
          .drop_duplicates(subset=["stock_id","div_date"])
    )

    splt = (
        df.loc[df["stock_splits"] > 0, ["stock_id", "spot_date", "stock_splits"]]
          .rename(columns={"spot_date": "split_date", "stock_splits": "ratio"})
          .dropna(subset=["split_date"])
          .drop_duplicates(subset=["stock_id","split_date"])
    )

    # 2) טרנזקציה אחת: יצירת temp, COPY, מחיקה, והכנסה מחדש
    with engine_dest.begin() as conn:
        try:
            conn.exec_driver_sql("SET LOCAL synchronous_commit = OFF;")

            # temp tables (נופלות אוטומטית בסוף הטרנזקציה)
            conn.exec_driver_sql("""
            CREATE TEMP TABLE tmp_spots (
            stock_id    BIGINT NOT NULL,
            spot_date   DATE   NOT NULL,
            open_value  DOUBLE PRECISION,
            close_value DOUBLE PRECISION,
            high_value  DOUBLE PRECISION,
            low_value   DOUBLE PRECISION,
            volume      BIGINT
            ) ON COMMIT DROP;
            """)
            conn.exec_driver_sql("""
            CREATE TEMP TABLE tmp_divs (
            stock_id BIGINT NOT NULL,
            div_date DATE   NOT NULL,
            amount   NUMERIC(20,8)
            ) ON COMMIT DROP;
            """)
            conn.exec_driver_sql("""
            CREATE TEMP TABLE tmp_splt (
            stock_id  BIGINT NOT NULL,
            split_date DATE  NOT NULL,
            ratio     NUMERIC(20,8)
            ) ON COMMIT DROP;
            """)

            # COPY ל-temp—באותה טרנזקציה
            raw = conn.connection  # DBAPI connection בתוך אותה טרנזקציה
            if not spots.empty:
                _copy_df(raw, spots, "tmp_spots",
                        ["stock_id","spot_date","open_value","close_value","high_value","low_value","volume"])
            if not divs.empty:
                _copy_df(raw, divs, "tmp_divs", ["stock_id","div_date","amount"])
            if not splt.empty:
                _copy_df(raw, splt, "tmp_splt", ["stock_id","split_date","ratio"])

            # מחיקה ממוקדת בטבלאות היעד
            conn.execute(text("DELETE FROM stock_spots WHERE stock_id = :sid"), {"sid": stock_id})
            conn.execute(text("DELETE FROM dividends  WHERE stock_id = :sid"), {"sid": stock_id})
            conn.execute(text("DELETE FROM splitting  WHERE stock_id = :sid"), {"sid": stock_id})

            # הכנסת הנתונים מחדש (אם יש)
            if not spots.empty:
                conn.exec_driver_sql("""
                INSERT INTO stock_spots (stock_id, spot_date, open_value, close_value, high_value, low_value, volume)
                SELECT stock_id, spot_date, open_value, close_value, high_value, low_value, volume
                FROM tmp_spots;
                """)
            if not divs.empty:
                conn.exec_driver_sql("""
                INSERT INTO dividends (stock_id, div_date, amount)
                SELECT stock_id, div_date, amount
                FROM tmp_divs;
                """)
            if not splt.empty:
                conn.exec_driver_sql("""
                INSERT INTO splitting (stock_id, split_date, ratio)
                SELECT stock_id, split_date, ratio
                FROM tmp_splt;
                """)
            
        except Exception as e:
           
            print(f"Error during refresh_single_stock: {e}",flush=True)
    
# a function that gets

def financial_insert_function(df: pd.DataFrame, stock_id: int, table_name: str):
    success = True
    with Session() as session:
        try:
            Model = TABLE_MODLE_MAP[table_name]
            
            # 1. Clean and Prepare DataFrame
            if df.index.name != 'publish_date':
                df = df.reset_index().rename(columns={df.index.name if df.index.name else 'index': 'publish_date'})
            else:
                df = df.reset_index()

            df['publish_date'] = pd.to_datetime(df['publish_date']).dt.date

            # 2. Build the records list
            records = []
            for _, row in df.iterrows():
                # Drop publish_date from the JSON data blob to avoid redundancy
                data_dict = row.drop(['publish_date']).dropna().to_dict()
                if len(data_dict) > 4:
                    records.append({
                        "stock_id": stock_id,
                        "publish_date": row['publish_date'],
                        "data": data_dict
                    })


            if not records:
                return True

            # 3. Perform PostgreSQL Upsert
            stmt = insert(Model).values(records)
            
            # Define the update action on conflict
            upsert_stmt = stmt.on_conflict_do_update(
            # Instead of 'constraint="name"', use the column objects or names
            index_elements=[Model.stock_id, Model.publish_date], 
            set_={"data": stmt.excluded.data}
            )

            session.execute(upsert_stmt)
            session.commit()
            
            

        except Exception as e:
            session.rollback()
            print(f"Error during insertion for stock {stock_id}: {e}")
            success = False
        finally:
            session.close()
    return success
    