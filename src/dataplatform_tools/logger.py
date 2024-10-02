"""
Module for logs registration in Data Platform.

Example usage:
    from src.dataplatform_tools.logger import configure_logger
    logger = configure_logger('MyAppName', 'INFO')
    logger.info('some log message')

"""

import logging
import sys

LOG_FORMAT = '%(asctime)s [%(levelname)s][%(filename)s][%(funcName)s][%(lineno)d] %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def configure_logger(app_name: str, log_level: str = 'INFO') -> logging.Logger:
    log_level = validate_log_level(log_level)
    logger = logging.getLogger(app_name)
    logger.setLevel(log_level)
    formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger


def validate_log_level(log_level: str) -> str:
    valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    if log_level in valid_levels:
        return log_level
    else:
        raise ValueError(f'Invalid log level: {log_level}. Choose one of: {valid_levels}')
