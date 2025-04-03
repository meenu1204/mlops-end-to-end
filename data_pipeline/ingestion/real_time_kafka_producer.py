# Name: real_time_kafka_producer.py
# Description: Simulate real-time streaming of data every 15 minutes using Apache Kafka

# Import necessary libraries 
import csv
import yaml
import json # Convert each row into JSON before sending to Kafka
import time
import logging

from confluent_kafka import Producer

from main.logger_config import get_logger

logger = get_logger()

# Load configuration
def load_config(path: str = 'main/config.yaml') -> dict:
    with open(path, 'r') as f:
        return yaml.safe_load(f)
    
cfg = load_config()

KAFKA_TOPIC = cfg['kafka_topic']
KAFKA_BOOTSTRAP_SERVERS = cfg['kafka_bootstrap_servers']
STREAMING_INTERVAL = int(cfg['streaming_interval'])
CSV_FILE = cfg['output_file']

# Initialise a Kafka producer
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

# Callback to determine the delivery status of each message
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Read data from csv and produce data to Kafka
def stream_data_from_csv():
    with open(CSV_FILE, mode='r') as file:
        # Read each row in csv as a dictionary
        csv_reader = csv.DictReader(file)
        for row in csv_reader:          
            producer.produce(KAFKA_TOPIC, value=json.dumps(row), callback=delivery_report)
            logging.info(f'Sent: {row}')
            time.sleep(STREAMING_INTERVAL)          
    producer.flush()
    logging.info('Finished sending messages')
