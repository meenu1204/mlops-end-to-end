# Name: main_pipeline.py
# Description: Run all stages in ML lifecycle and adopt MLOps practices

# Import necessary libraries
import logging

from data_pipeline.ingestion import stock_data_collector
from logger_config import get_logger

logger = get_logger('MLOps_main')

def run_ml_pipeline():
    logger.info('Starting pipeline')

    logger.info('Step 1: Data Ingestion')
    stock_data_collector.collect_data()

if __name__ == '__main__':
    run_ml_pipeline()