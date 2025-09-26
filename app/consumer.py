import ssl
import pika
import os

HOST = os.getenv('RABBITMQ_HOST')
PORT = 5671
USER = os.getenv('RABBITMQ_USER')
PASS = os.getenv('RABBITMQ_PASS')
QUEUE = "stock_reset_q"
WORKERS = int(os.getenv("WORKERS", "2"))

# TLS context for self-signed certs
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.load_verify_locations("ca.crt")
ctx.check_hostname = False   # skip hostname check for self-signed

params = pika.ConnectionParameters(
    host=HOST,
    port=PORT,
    virtual_host="/",
    credentials=pika.PlainCredentials(USER, PASS),
    ssl_options=pika.SSLOptions(ctx),
    heartbeat=0,                   # הכי פשוט: לבטל heartbeat כדי לא ליפול בעיבוד ארוך
    blocked_connection_timeout=300,
    connection_attempts=10,
    retry_delay=3.0,
    client_properties={"connection_name": "stock_reset_consumer"},
)

conn = pika.BlockingConnection(params)
ch = conn.channel()
