import os, ssl, json, pika, time
from pika import exceptions as px
from app import logger
RABBIT_HOST = os.getenv("RABBITMQ_HOST")
RABBIT_USER = os.getenv("RABBITMQ_USER")
RABBIT_PASS = os.getenv("RABBITMQ_PASS")

# ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
# ctx.load_verify_locations("ca.crt")
# ctx.check_hostname = False   # לתעודה self-signed

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE



params = pika.ConnectionParameters(
    host=RABBIT_HOST,
    port=int(os.getenv("RABBITMQ_PORT",5671)),
    virtual_host="/",
    credentials=pika.PlainCredentials(RABBIT_USER, RABBIT_PASS),
    heartbeat=30,
    ssl_options=pika.SSLOptions(ctx, server_hostname=RABBIT_HOST),
)

conn = None
ch = None
exchange = "screener_refresh"
routing_key = "screener.refresh"

def _open_connection():
    global conn, ch
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.exchange_declare(exchange=exchange, exchange_type="topic", durable=True)
    logger.info("RabbitMQ connected")

def send_rabbitmq_message(message: dict, max_retries: int = 5):
    global conn, ch
    payload = json.dumps(message).encode()
    props = pika.BasicProperties(content_type="application/json", delivery_mode=2)

    for attempt in range(1, max_retries + 1):
        try:
            if conn is None or conn.is_closed or ch is None or ch.is_closed:
                _open_connection()

            ch.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=payload,
                properties=props,
            )
            logger.info(f"Message sent (attempt {attempt}): {message}")
            return True

        except (px.StreamLostError, px.ConnectionClosed, px.ChannelClosed) as e:
            logger.error(f"Publish failed (attempt {attempt}): {e}")
            try:
                if conn and not conn.is_closed:
                    conn.close()
            except Exception:
                pass
            conn, ch = None, None
            time.sleep(1)  # המתנה קצרה לפני ניסיון נוסף

    logger.error(f"Message NOT sent after max retries: {message}")
    return False
