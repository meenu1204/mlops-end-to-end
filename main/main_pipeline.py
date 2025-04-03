# Name: main_pipeline.py
# Description: Run all stages in ML lifecycle and adopt MLOps practices

# Import necessary libraries
import logging
import subprocess
import threading
import time

from data_pipeline.ingestion import stock_data_collector
from data_pipeline.ingestion import real_time_kafka_producer
from main.logger_config import get_logger

logger = get_logger('MLOps_main')
logger.info('---Logger initialized and ready---')

def run_ml_pipeline():
    logger.info('Starting pipeline')

    logger.info('Step 1: Data ingestion')
    ingestion_thread = threading.Thread(target=stock_data_collector.collect_data, daemon=True)
    ingestion_thread.start()
    time.sleep(10)

    logger.info('Step 2.1: kafka shell script')
    subprocess.run(
        ["bash", "data_pipeline/ingestion/kafka_producer.sh"], 
        check=True
    )
    
    logger.info('Step 2.2: Kafka producer sending data')
    real_time_kafka_producer.stream_data_from_csv()

if __name__ == "__main__":
    run_ml_pipeline()
