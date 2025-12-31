from app.consumer import connect,QUEUE,HOST
from app.initializing_the_system import info_generate
from app.sqlspeaker import Session, Stock
from typing import Optional
import json
import pika
import time
import traceback


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
        if isinstance(stock, Stock):
            print (f"stock: {stock.symbol}",flush=True)
            result = info_generate([stock])
            if result == "success":
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            print(" [x] Stock initialized:", stock.stock_id)
        else:
            print(" [x] Stock not found:", data["stock_id"],flush=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        # put the same message back to the head of the queue

        print(" [!] Bad message:", body, e,flush=True)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    # Acknowledge



def start_consumer():
    
    while True:
        try:
            conn = connect()
            ch = conn.channel()
            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(queue=QUEUE, on_message_callback=callback)

            print("version 1.0.2")
            print(f" [*] Connected to RabbitMQ at {HOST}, waiting for messages on '{QUEUE}'…", flush=True)
            ch.start_consuming()

        except pika.exceptions.AMQPError as e:
            print("Connection lost or broker unavailable. Retrying in 5s…", e, flush=True)
            time.sleep(5)
            continue
        except Exception as e:
            print("Unexpected error in consumer:", e, flush=True)
            traceback.print_exc()
            time.sleep(5)
            continue

if __name__ == "__main__":
    start_consumer()