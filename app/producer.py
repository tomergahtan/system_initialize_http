# import os, ssl, json, pika, time
# from pika import exceptions as px
# from app import logger
# RABBIT_HOST = os.getenv("RABBITMQ_HOST")
# RABBIT_USER = os.getenv("RABBITMQ_USER")
# RABBIT_PASS = os.getenv("RABBITMQ_PASS")

# # ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
# # ctx.load_verify_locations("ca.crt")
# # ctx.check_hostname = False   # לתעודה self-signed

# ctx = ssl.create_default_context()
# ctx.check_hostname = False
# ctx.verify_mode = ssl.CERT_NONE



# params = pika.ConnectionParameters(
#     host=RABBIT_HOST,
#     port=int(os.getenv("RABBITMQ_PORT",5671)),
#     virtual_host="/",
#     credentials=pika.PlainCredentials(RABBIT_USER, RABBIT_PASS),
#     heartbeat=30,
#     ssl_options=pika.SSLOptions(ctx, server_hostname=RABBIT_HOST),
# )

# conn = None
# ch = None
# exchange = "screener_refresh"
# routing_key = "screener.refresh"

# def _open_connection():
#     global conn, ch
#     conn = pika.BlockingConnection(params)
#     ch = conn.channel()
#     ch.exchange_declare(exchange=exchange, exchange_type="topic", durable=True)
#     logger.info("RabbitMQ connected")

# def send_rabbitmq_message(message: dict, max_retries: int = 5):
#     global conn, ch
#     payload = json.dumps(message).encode()
#     props = pika.BasicProperties(content_type="application/json", delivery_mode=2)

#     for attempt in range(1, max_retries + 1):
#         try:
#             if conn is None or conn.is_closed or ch is None or ch.is_closed:
#                 _open_connection()

#             ch.basic_publish(
#                 exchange=exchange,
#                 routing_key=routing_key,
#                 body=payload,
#                 properties=props,
#             )
#             logger.info(f"Message sent (attempt {attempt}): {message}")
#             return True

#         except (px.StreamLostError, px.ConnectionClosed, px.ChannelClosed) as e:
#             logger.error(f"Publish failed (attempt {attempt}): {e}")
#             try:
#                 if conn and not conn.is_closed:
#                     conn.close()
#             except Exception:
#                 pass
#             conn, ch = None, None
#             time.sleep(1)  # המתנה קצרה לפני ניסיון נוסף

#     logger.error(f"Message NOT sent after max retries: {message}")
#     return False




import os, ssl, json, pika, time
from pika import exceptions as px

RABBIT_HOST = os.getenv("RABBITMQ_HOST")
RABBIT_USER = os.getenv("RABBITMQ_USER")
RABBIT_PASS = os.getenv("RABBITMQ_PASS")

from app import logger


class RabbitMQConnection:

    def __init__(self, 
        exchange: str, 
        routing_key: str , 
        exchange_type: str = "topic", 
        username: str = RABBIT_USER, 
        password: str = RABBIT_PASS, 
        host: str = RABBIT_HOST,
        port: int = int(os.getenv("RABBITMQ_PORT",5671)), 
        heartbeat: int = 30,
        virtual_host: str = "/"
        ):
        if not username or not password or not host or not port:
            raise ValueError("Username, password, host and port are required")

        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
        self.ch = None
        self.exchange = exchange
        self.routing_key = routing_key
        self.ctx = ssl.create_default_context()
        self.ctx.check_hostname = False
        self.ctx.verify_mode = ssl.CERT_NONE
        self.exchange_type = exchange_type
    
    def _open_connection(self):

        params = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host="/",
            credentials=pika.PlainCredentials(self.username, self.password),
            heartbeat=30,
            ssl_options=pika.SSLOptions(self.ctx, server_hostname=self.host),
        )
        self.conn = pika.BlockingConnection(params)
        self.ch = self.conn.channel()
        self.ch.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type, durable=True)
        
    def _close_connection(self):
        if self.conn:
            self.conn.close()
        if self.ch:
            self.ch.close()
        self.conn = None
        self.ch = None
    
    def send_message(self, message: dict, max_retries: int = 5):
        payload = json.dumps(message).encode()
        props = pika.BasicProperties(content_type="application/json", delivery_mode=2)

        for attempt in range(1, max_retries + 1):
            try:
                if self.conn is None or self.conn.is_closed or self.ch is None or self.ch.is_closed:
                    self._open_connection()

                self.ch.basic_publish(
                    exchange=self.exchange,
                    routing_key=self.routing_key,
                    body=payload,
                    properties=props,
                )
                logger.info("Message sent (attempt %d): %s", attempt, message)
                return True

            except (px.StreamLostError, px.ConnectionClosed, px.ChannelClosed) as e:
                logger.warning("Publish failed (attempt %d): %s", attempt, e)
                try:
                    if self.conn and not self.conn.is_closed:
                        self.conn.close()
                except Exception:
                    pass
                self.conn, self.ch = None, None
                time.sleep(1)  

        logger.error("Message NOT sent after max retries: %s", message)
        return False
    
    def close(self):
        self._close_connection()