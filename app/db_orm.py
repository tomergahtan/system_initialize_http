from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import  String, Float, ForeignKey, Date, BigInteger, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict
from datetime import date
class Base(DeclarativeBase):
    pass
# StockExchange table
class StockExchange(Base):
    __tablename__ = 'stock_exchange'

    se_id: Mapped[int] = mapped_column(primary_key=True)
    se_name: Mapped[str] = mapped_column(String(20), nullable=False, unique=True)

    # stocks: Mapped[list["Stock"]] = relationship("Stock", back_populates="stock_exchange")

    def __repr__(self):
        return f"<StockExchange(id={self.id}, name='{self.name}')>"

# Currency table
class Currency(Base):
    __tablename__ = 'currency'

    cur_id: Mapped[int] = mapped_column(primary_key=True)
    cur_name: Mapped[str] = mapped_column(String(20), nullable=False, unique=True)

    # stocks: Mapped[list["Stock"]] = relationship("Stock", back_populates="currency")

    def __repr__(self):
        return f"<Currency(cur_id={self.cur_id}, cur_name='{self.cur_name}')>"

# Industry table
class Industry(Base):
    __tablename__ = 'industry'

    industry_id: Mapped[int] = mapped_column(primary_key=True)
    industry_name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)

# Sectors table
class Sector(Base):
    __tablename__ = 'sectors'

    sector_id: Mapped[int] = mapped_column(primary_key=True)
    sector_name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)

    # stocks: Mapped[list["Stock"]] = relationship("Stock", back_populates="sector")

    def __repr__(self):
        return f"<Sector(sector_id={self.sector_id}, sector_name='{self.sector_name}')>"

# Country table
class Country(Base):
    __tablename__ = 'country'

    country_id: Mapped[int] = mapped_column(primary_key=True)
    country_name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)

    # stocks: Mapped[list["Stock"]] = relationship("Stock", back_populates="country")

    def __repr__(self):
        return f"<Country(country_id={self.country_id}, country_name='{self.country_name}')>"

# Stocks table
class Stock(Base):
    __tablename__ = 'stocks'

    stock_id: Mapped[int] = mapped_column(primary_key=True)
    symbol: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    se_id: Mapped[int] = mapped_column(ForeignKey('stock_exchange.id'), nullable=True)
    implied_shares_outstanding: Mapped[int] = mapped_column(BigInteger, nullable=True)
    cur_id: Mapped[int] = mapped_column(ForeignKey('currency.cur_id'),nullable=True)
    sector_id: Mapped[int] = mapped_column(ForeignKey('sectors.sector_id'),nullable=True)
    country_id: Mapped[int] = mapped_column(ForeignKey('country.country_id'),nullable=True)
    industry_id: Mapped[int] = mapped_column(ForeignKey('industry.industry_id'),nullable=True)
    last_update: Mapped[Date] = mapped_column(Date, nullable=True)
    company_name: Mapped[str] = mapped_column(Text, nullable=True)
    information: Mapped[str] = mapped_column(Text, nullable=True)
    last_reset: Mapped[Date] = mapped_column(Date, nullable=True)


    def __repr__(self):
        return f"<Stock(stock_id={self.stock_id}, symbol='{self.symbol}')>"

# Irrelevant table
class Irrelevant(Base):
    __tablename__ = 'irrelevant'

    stock_id: Mapped[int] = mapped_column(ForeignKey('stocks.stock_id'), primary_key=True)


    def __repr__(self):
        return f"<Irrelevant(stock_id={self.stock_id})>"

# StockSpot table
class StockSpot(Base):
    __tablename__ = "stock_spots"

    stock_id: Mapped[int] = mapped_column(ForeignKey("stocks.stock_id"), primary_key=True)
    spot_date: Mapped[date] = mapped_column(Date, primary_key=True)

    # Float(53) maps to DOUBLE PRECISION on Postgres
    open_value: Mapped[float]  = mapped_column(Float(53))
    close_value: Mapped[float] = mapped_column(Float(53))
    high_value: Mapped[float]  = mapped_column(Float(53))
    low_value: Mapped[float]   = mapped_column(Float(53))

    volume: Mapped[int] = mapped_column(BigInteger)

    def __repr__(self):
        return f"<StockSpot(stock_id={self.stock_id}, spot_date={self.spot_date})>"

# Splitting table
class Splitting(Base):
    __tablename__ = 'splitting'

    stock_id: Mapped[int] = mapped_column(ForeignKey('stocks.stock_id'), primary_key=True)
    split_date: Mapped[date] = mapped_column(primary_key=True)
    ratio: Mapped[float] = mapped_column(Float)


    def __repr__(self):
        return f"<Splitting(stock_id={self.stock_id}, split_date={self.split_date}, ratio={self.ratio})>"

# Dividends table
class Dividend(Base):
    __tablename__ = 'dividends'

    stock_id: Mapped[int] = mapped_column(ForeignKey('stocks.stock_id'), primary_key=True)
    div_date: Mapped[date] = mapped_column(primary_key=True)
    amount: Mapped[float] = mapped_column(Float)


    def __repr__(self):
        return f"<Dividend(stock_id={self.stock_id}, div_date={self.div_date}, amount={self.amount})>"

# Balance Sheets, Income Statements, and Cash Flows
class AnnualBalanceSheet(Base):
    __tablename__ = 'annual_balance_sheet'

    stock_id: Mapped[int] = mapped_column(ForeignKey('stocks.stock_id', ondelete='CASCADE'), primary_key=True)
    publish_date: Mapped[date] = mapped_column(primary_key=True)
    data: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), nullable=False)


    def __repr__(self):
        return f"<AnnualBalanceSheet(stock_id={self.stock_id}, publish_date={self.publish_date})>"

class QuarterlyBalanceSheet(Base):
    __tablename__ = 'quarterly_balance_sheet'

    stock_id: Mapped[int] = mapped_column(ForeignKey('stocks.stock_id', ondelete='CASCADE'), primary_key=True)
    publish_date: Mapped[date] = mapped_column(primary_key=True)
    data: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), nullable=False)


    def __repr__(self):
        return f"<QuarterlyBalanceSheet(stock_id={self.stock_id}, publish_date={self.publish_date})>"

class AnnualIncomeStatement(Base):
    __tablename__ = 'annual_income_statement'

    stock_id: Mapped[int] = mapped_column(ForeignKey('stocks.stock_id', ondelete='CASCADE'), primary_key=True)
    publish_date: Mapped[date] = mapped_column(primary_key=True)
    data: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), nullable=False)


    def __repr__(self):
        return f"<AnnualIncomeStatement(stock_id={self.stock_id}, publish_date={self.publish_date})>"

class QuarterlyIncomeStatement(Base):
    __tablename__ = 'quarterly_income_statement'

    stock_id: Mapped[int] = mapped_column(ForeignKey('stocks.stock_id', ondelete='CASCADE'), primary_key=True)
    publish_date: Mapped[date] = mapped_column(primary_key=True)
    data: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), nullable=False)


    def __repr__(self):
        return f"<QuarterlyIncomeStatement(stock_id={self.stock_id}, publish_date={self.publish_date})>"

class AnnualCashFlow(Base):
    __tablename__ = 'annual_cash_flow'

    stock_id: Mapped[int] = mapped_column(ForeignKey('stocks.stock_id', ondelete='CASCADE'), primary_key=True)
    publish_date: Mapped[date] = mapped_column(primary_key=True)
    data: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), nullable=False)


    def __repr__(self):
        return f"<AnnualCashFlow(stock_id={self.stock_id}, publish_date={self.publish_date})>"

class QuarterlyCashFlow(Base):
    __tablename__ = 'quarterly_cash_flow'

    stock_id: Mapped[int] = mapped_column(ForeignKey('stocks.stock_id', ondelete='CASCADE'), primary_key=True)
    publish_date: Mapped[date] = mapped_column(primary_key=True)
    data: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), nullable=False)

    # stock: Mapped[Stock] = relationship("Stock", back_populates="cash_quarterly")

    def __repr__(self):
        return f"<QuarterlyCashFlow(stock_id={self.stock_id}, publish_date={self.publish_date})>"

