import logging
import json
from config import app_name

stream_handler = logging.StreamHandler()
stream_formatter = logging.Formatter(json.dumps({'@t': '%(asctime)s', '@m': '%(message)s', '@l': '%(levelname)s'}))
stream_handler.setFormatter(stream_formatter)

logger = logging.getLogger(app_name)
logger.setLevel(logging.INFO)
logger.addHandler(stream_handler)