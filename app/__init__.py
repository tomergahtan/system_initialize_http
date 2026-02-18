import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
for noisy in [
    "pika",
    "pika.adapters",
    "pika.adapters.utils",
    "pika.adapters.utils.connection_workflow",
    "pika.adapters.utils.io_services_utils",
]:
    logging.getLogger(noisy).setLevel(logging.CRITICAL)

logger = logging.getLogger(__name__)

logger.info("Application started")
logger.warning("Something looks suspicious")
logger.error("Something failed")
