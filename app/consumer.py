import ssl
import pika
import os

HOST = os.getenv('RABBITMQ_HOST')
PORT = os.getenv('RABBITMQ_PORT')
USER = os.getenv('RABBITMQ_USER')
PASS = os.getenv('RABBITMQ_PASS')
QUEUE = os.getenv('RABBITMQ_QUEUE')

# TLS context for self-signed certs
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.load_verify_locations("app/ca.crt")
ctx.check_hostname = False   # skip hostname check for self-signed

params = pika.ConnectionParameters(
    host=HOST,
    port=PORT,
    virtual_host="/",
    credentials=pika.PlainCredentials(USER, PASS),
    ssl_options=pika.SSLOptions(ctx),
)

conn = pika.BlockingConnection(params)
ch = conn.channel()
