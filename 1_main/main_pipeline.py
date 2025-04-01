# Name: main_pipeline.py
# Description: Run all stages in ML lifecycle and adopt MLOps practices

# Import necessary libraries
import logging
from logger_config import get_logger

logger = get_logger('MLOps_main')

def run_ml_pipeline():
    logger.info('Starting pipeline')

    logger.info('Step 1: Data Ingestion')
    

if __name__ == '__main__':
    run_ml_pipeline()