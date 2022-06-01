import logging
import json

stream_handler = logging.StreamHandler()
stream_formatter = logging.Formatter(json.dumps({'@t': '%(asctime)s', '@m': '%(message)s', '@l': '%(levelname)s'}))
stream_handler.setFormatter(stream_formatter)

logger = logging.getLogger("tweet-stream-processor")
logger.setLevel(logging.INFO)
logger.addHandler(stream_handler)
