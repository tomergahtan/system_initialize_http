import os
import io
from sqlalchemy import select,Connection, create_engine,update, delete, text,exists
from .db_orm import (Stock, Currency, Sector, Country,
                        AnnualBalanceSheet, AnnualIncomeStatement, AnnualCashFlow,
                        QuarterlyBalanceSheet, QuarterlyIncomeStatement, QuarterlyCashFlow, Irrelevant, StockView, Integer, Date, Float, Dividend, Splitting,
                        StockSpot, Industry)

from datetime import date, datetime
import pandas as pd

from sqlalchemy.orm import sessionmaker

from sqlalchemy.exc import DBAPIError





import logging
logging.getLogger('yfinance').setLevel(logging.CRITICAL)
logger = logging.getLogger('my_app')
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler('../../output.log', mode='a')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Add the handler to your logger
logger.addHandler(file_handler)

# Example usage
logger.info("This log will appear in output.log")
logger.debug("Debugging information here.")
psql = os.getenv('PSQL')

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
data_types_stock_spots = {
        "stock_id": Integer,
        "spot_date": Date,
        "open_value": Float,
        "close_value": Float,
        "volume":Float
    }

# connect to the database investments
with Session() as session:
    try:
        subq = select(1).where(Irrelevant.stock_id == Stock.stock_id)
        stmt = select(Stock).where(~subq.exists())  
        stock_list = session.scalars(stmt).all()
        country_set = { country.country_name: country.country_id for country in session.query(Country).all()}
        sector_set = { sector.sector_name:sector.sector_id for sector in session.query(Sector).all()}
        currency_set = { currency.cur_name:currency.cur_id for currency in session.query(Currency).all()}
        industry_set = { industry.industry_name:industry.industry_id for industry in session.query(Industry).all()}

    except DBAPIError as e:
        print(f"Error: {e}")
    finally:
        session.close()





def update_country(country_name: str):
        # Attempt to insert the country (trigger prevents duplicates)
    if not country_name in country_set and country_name is not None :
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
    """
    Update the currency of a stock in the database.
    Note: this Currency  is the currency of the finencial data of the stock
    and not the currency of the stock itself.
    """
        # Attempt to insert the currency (trigger prevents duplicates)
    if not cur in currency_set and cur is not None:
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
        # Attempt to insert the sector (trigger prevents duplicates)
    if not sector_name in sector_set and sector_name is not None:
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


        # Attempt to insert the industry (trigger prevents duplicates)
    if not industry_name in industry_set and industry_name is not None:
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
    values = {k: v for k, v in values.items() if v is not None}
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
            logger.error(f"Error during irrelevant insertion stock id {stock_id}: {e}",flush=True)
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
            volume      DOUBLE PRECISION
            ) ON COMMIT DROP;
            """)
            conn.exec_driver_sql("""
            CREATE TEMP TABLE tmp_divs (
            stock_id BIGINT NOT NULL,
            div_date DATE   NOT NULL,
            amount   DOUBLE PRECISION
            ) ON COMMIT DROP;
            """)
            conn.exec_driver_sql("""
            CREATE TEMP TABLE tmp_splt (
            stock_id  BIGINT NOT NULL,
            split_date DATE  NOT NULL,
            ratio     DOUBLE PRECISION
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

def financial_insert_function(df: pd.DataFrame, stock_id: int,table_name:str):
    with Session() as session:
        try:

            Model = TABLE_MODLE_MAP[table_name]
            # Ensure the DataFrame's index is named 'publish_date' and reset it to a column
            if df.index.name != 'publish_date':
                df = df.reset_index().rename(columns={'index': 'publish_date'})
            else:
                df = df.reset_index()

            # Convert 'publish_date' to datetime.date
            df['publish_date'] = pd.to_datetime(df['publish_date']).dt.date

            # Query existing records for the given stock_id
            existing_dates = session.scalars(
                select(Model.publish_date).
                where(Model.stock_id == stock_id)
            ).all()

            # Filter out rows that already exist in the database
            new_records = df[~df['publish_date'].isin(existing_dates)]

            # Iterate over the new records and add them to the session
            for _, row in new_records.iterrows():
                # Create the data dictionary encapsulated under the 'data' key
                data_dict = row.drop(['publish_date']).dropna().to_dict()
                record_data = {"data": data_dict}

                # Create an instance of the ORM model
                record = Model(
                    stock_id=stock_id,
                    publish_date=row['publish_date'],
                    data=data_dict
                )

                # Add the record to the session
                session.add(record)

            # Commit the session to insert all new records
            session.commit()


        except Exception as e:
            session.rollback()
            print(f"Error during insertion:{stock_id} {e}","\n")
        finally:
            session.close()
