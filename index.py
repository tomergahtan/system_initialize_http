from fastapi import FastAPI, HTTPException
from typing import Optional
import uvicorn 
from app.initializing_the_system import info_generate
from app.sqlspeaker import Session, StockView
app = FastAPI()

def get_stock_by_id(stock_id: int) -> Optional[StockView]:
    with Session() as session:
        stock = session.query(StockView).filter(StockView.stock_id == stock_id).first()
        return stock
    



@app.get("/")
def healthcheck():
    return {"status": "ok"}


@app.get("/initialize/{stock_id}")
def initialize_stock(stock_id: int):
    stock = get_stock_by_id(stock_id)
    if stock is None:
        raise HTTPException(status_code=404, detail="stock_id not found")
    try:
        info_generate([stock])
        return {"status": "success", "stock_id": stock.stock_id, "symbol": stock.symbol}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"initialization failed: {e}")


if __name__ == "__main__":
    print("app initialized")
    uvicorn.run("index:app", host="0.0.0.0", port=8000,reload=True)



