# Program name: logger_config.py
# Description: Central logging utility used across all programs

# Import libraries
import logging
from logging import Logger, FileHandler, StreamHandler, Formatter
from typing import Optional

def get_logger(name:Optional[str] = None, log_file: str = 'main.log') -> Logger:
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        logger.setLevel(logging.INFO)
        
        file_handler = FileHandler(log_file)
        file_handler.setFormatter(Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

        console_handler = FileHandler(log_file)
        console_handler.setFormatter(Formatter('%(name)s - %(levelname)s - %(message)s'))

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger