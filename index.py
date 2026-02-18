from app.consumer import connect,QUEUE,HOST
from app.initializing_the_system import info_generate
from app.sqlspeaker import get_stock_by_id, Stock
from typing import Optional
import json
import pika
import time
import traceback
from app import logger
from datetime import datetime
from app.producer import send_rabbitmq_message
def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")



def callback(ch, method, properties, body):
    try:
        data = json.loads(body.decode())
        stock_id = data["stock_id"]
        if "from" in data:
            logger.info(f"data arrived from: {data['from']}")
        else:
            logger.info("data arrived from unknown source")


     
        stock = get_stock_by_id(stock_id)
        
        if isinstance(stock, Stock):
            logger.info(f"working on stock: {stock.symbol}")

            result = info_generate([stock])
            if result == "success":
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            logger.info(f"Stock initialized: {stock.symbol}")
            logger.info(f"transfer to screener service: {stock.symbol}")
            send_rabbitmq_message(message={"stock_id": stock.stock_id, "from": "stock_reset","action": data["action"]}, max_retries=5)
        else:
            logger.error(f"Stock not found: {data['stock_id']}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        # put the same message back to the head of the queue

        logger.error(f"Bad message: {body} {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    # Acknowledge



def start_consumer():
    
    while True:
        try:
            logger.info(f"Starting consumer at {now()}")
            conn = connect()
            logger.info(f"Connected to RabbitMQ at {HOST}")
            ch = conn.channel()
            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(queue=QUEUE, on_message_callback=callback)
            logger.info(f"Consuming messages from {QUEUE}")

            logger.info(f"version 1.0.2")
            logger.info(f"Connected to RabbitMQ at {HOST}, waiting for messages on '{QUEUE}'…")
            ch.start_consuming()

        except pika.exceptions.AMQPError as e:
            logger.error(f"Connection lost or broker unavailable. Retrying in 5s… {e}")
            time.sleep(5)
            continue
        except Exception as e:
            logger.error(f"Unexpected error in consumer: {e}")
            traceback.print_exc()
            time.sleep(5)
            continue

if __name__ == "__main__":
    start_consumer()