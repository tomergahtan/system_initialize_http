import ssl
import pika
import os
from app import logger
HOST = os.getenv('RABBITMQ_HOST')
PORT = int(os.getenv('RABBITMQ_PORT',5671))
USER = os.getenv('RABBITMQ_USER')
PASS = os.getenv('RABBITMQ_PASS')
QUEUE = "stock_reset_q"
WORKERS = int(os.getenv("WORKERS", "2"))

# TLS context for self-signed certs
# ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
# ctx.load_verify_locations("ca.crt")
# ctx.check_hostname = False   # skip hostname check for self-signed

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


def connect():
    logger.info(f"Connecting to RabbitMQ at {HOST}:{PORT}")
    params = pika.ConnectionParameters(
        host=HOST,
        port=PORT,
        virtual_host="/",
        credentials=pika.PlainCredentials(USER, PASS),
        ssl_options=pika.SSLOptions(ctx),
        heartbeat=60,                      # enable heartbeat
        blocked_connection_timeout=300,
        connection_attempts=10,
        retry_delay=5,
        client_properties={"connection_name": "stock_reset_consumer"},
    )
    conn = pika.BlockingConnection(params)
    logger.info(f"Connected to RabbitMQ at {HOST}:{PORT}")
    return conn


