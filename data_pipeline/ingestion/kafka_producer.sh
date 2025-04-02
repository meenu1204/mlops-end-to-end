#!/bin/bash

# Name: kafka_producer.sh
# Description: Creates zookeeper, kafka broker, topic and also run kafka producer

# Set Kafka installation directory
KAFKA_DIR="/usr/local/opt/kafka/libexec" 

PYTHON_PRODUCER_SCRIPT="data_pipeline/ingestion/real_time_kafka_producer.py"  
TOPIC_NAME="AAPL_data_stream"

# Start Zookeeper
echo "Starting Zookeeper..."  
$KAFKA_DIR/bin/zookeeper-server-start.sh $KAFKA_DIR/config/zookeeper.properties > logs/zookeeper.log 2>&1 &
sleep 5

# Start Kafka Broker
echo "Starting Kafka Broker..."
$KAFKA_DIR/bin/kafka-server-start.sh $KAFKA_DIR/config/server.properties > logs/kafka.log 2>&1 &
sleep 15

# Create topic if it doesn't exist
echo "Creating Kafka topic: $TOPIC_NAME"
$KAFKA_DIR/bin/kafka-topics.sh --create \
  --topic $TOPIC_NAME \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --if-not-exists

# Run kafka Producer
echo "Running Python Kafka producer: $PYTHON_PRODUCER_SCRIPT"
python3 $PYTHON_PRODUCER_SCRIPT
