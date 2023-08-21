import os
import json
import logging
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Read environment variables
kafka_topic = os.environ.get('KAFKA_TOPIC')

BOOTSTRAP_SERVERS = 'kafka:9092'

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Create a Kafka admin client
kafka_admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)

# Check if the topic exists
topic_names = [topic for topic in kafka_admin.list_topics()]
topic_exists = kafka_topic in topic_names
if not topic_exists:
    new_topic = NewTopic(name=kafka_topic, num_partitions=1, replication_factor=1)
    kafka_admin.create_topics([new_topic])

# Create a Kafka producer
kafka_producer = KafkaProducer(
    value_serializer=lambda message: json.dumps(message).encode('ascii'),
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    retries=5
)

def handle_send_success(record_metadata):
    """
    Callback function to handle successful message sending to Kafka.

    Args:
        record_metadata: Metadata about the sent record.
    """
    logging.info("Message sent successfully: Topic=%s, Partition=%s, Offset=%s",
                 record_metadata.topic, record_metadata.partition, record_metadata.offset)

def handle_send_error(exception):
    """
    Callback function to handle error during message sending to Kafka.

    Args:
        exception: Exception raised during message sending.
    """
    logging.error("Error while sending message: %s", exception)

def send_message_to_kafka(message):
    """
    Send a message to Kafka.

    Args:
        message: The message to send.
    """
    kafka_producer.send(kafka_topic, message)\
        .add_callback(handle_send_success)\
        .add_errback(handle_send_error)

