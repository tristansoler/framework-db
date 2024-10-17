import logging
import json
import threading
import sys

LOG_FORMAT = '%(asctime)s [%(levelname)s][%(filename)s][%(funcName)s][%(lineno)d] %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

class Logger:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                cls._instance = super(Logger, cls).__new__(cls)
                try:
                    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)

                    cls._instance.logger = logger= logging.getLogger()
                except (FileNotFoundError, json.JSONDecodeError) as e:
                    raise RuntimeError(f"Failed to initialize logger due to error: {e}")
        return cls._instance
    

logger = Logger()._instance.logger