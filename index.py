
from app.consumer import conn,QUEUE,ch
from app.initializing_the_system import info_generate
from app.sqlspeaker import Session, Stock
from typing import Optional
import json

def get_stock_by_id(stock_id: int) -> Optional[Stock]:
    with Session() as session:
        try:    
            stock = session.query(Stock).filter(Stock.stock_id == stock_id).first()
            return stock
        except Exception as e:
            print(f"Error getting stock by id: {e}",flush=True)
            return None
        finally:
            session.close()
    
def callback(ch, method, properties, body):
    try:
        data = json.loads(body.decode())
        
        print(data,flush=True)
        stock = get_stock_by_id(data["stock_id"])
        print (f"stock: {stock.symbol}",flush=True)
        if stock is not None:
            info_generate([stock])
            print(" [x] Stock initialized:", stock.stock_id)
        else:
            print(" [x] Stock not found:", data["stock_id"],flush=True)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        
        print(" [!] Bad message:", body, e,flush=True)
    # Acknowledge




# Ensure QoS so we don’t flood this consumer
ch.basic_qos(prefetch_count=1)

# Attach to the existing queue
ch.basic_consume(queue=QUEUE, on_message_callback=callback)


print("version 1.0.1")
print(f" [*] Waiting for messages on queue '{QUEUE}'… CTRL+C to exit.", flush=True)
try:
    ch.start_consuming()
except KeyboardInterrupt:
    print("Stopping consumer…")
    try: ch.stop_consuming()
    except: pass
    try: conn.close()
    except: pass
