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
                    app_name = "DF"
                    log_level = logging.INFO
                    logger = logging.getLogger(app_name)
                    logger.setLevel(log_level)

                    formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)
                    console_handler = logging.StreamHandler(sys.stdout)
                    console_handler.setLevel(log_level)
                    console_handler.setFormatter(formatter)
                    logger.addHandler(console_handler)

                    cls._instance.logger = logging.getLogger(app_name)
                except (FileNotFoundError, json.JSONDecodeError) as e:
                    raise RuntimeError(f"Failed to initialize logger due to error: {e}")
        return cls._instance


logger = Logger()._instance.logger
