import os
from typing import Dict, Any, Iterator

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from dotenv import load_dotenv

# Configuration
TOPIC_NAME = 'netflix_click_events'
CSV_PATH = 'vodclickstream_uk_movies_03.csv'
CHUNK_SIZE = 10000


def get_config() -> Dict[str, str]:
    """Load and validate configuration from environment variables."""
    load_dotenv()

    required_vars = [
        'BOOTSTRAP_SERVER',
        'SCHEMA_REGISTRY_URL',
        'KAFKA_KEY',
        'KAFKA_SECRET',
        'SR_KEY',
        'SR_SECRET'
    ]

    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    return {var: os.getenv(var) for var in required_vars}


def process_records(filename: str, chunk_size: int) -> Iterator[Dict[str, Any]]:
    """Generate records from CSV file."""
    for chunk in pd.read_csv(filename, chunksize=chunk_size):
        for record in chunk.to_dict('records'):
            yield {
                'row_id': int(record['Unnamed: 0']),
                'datetime': str(record['datetime']),
                'duration': float(record['duration']),
                'title': str(record['title']),
                'genres': str(record['genres']),
                'release_date': str(record['release_date']),
                'movie_id': str(record['movie_id']),
                'user_id': str(record['user_id'])
            }


def delivery_report(err: Any, msg: Any) -> None:
    """Handle delivery reports from Kafka producer."""
    if err is not None:
        print(f"Delivery failed: {err}")
        return
    print(f'Record delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}')


def main() -> None:
    # Get configuration
    config = get_config()

    # Set up Schema Registry
    schema_registry = SchemaRegistryClient({
        'url': config['SCHEMA_REGISTRY_URL'],
        'basic.auth.user.info': f"{config['SR_KEY']}:{config['SR_SECRET']}"
    })

    # Avro schema for serialization
    schema_str = """
    {
        "type": "record",
        "name": "MovieClick",
        "namespace": "com.streaming.movies",
        "fields": [
            {"name": "row_id", "type": "long"},
            {"name": "datetime", "type": "string"},
            {"name": "duration", "type": "double"},
            {"name": "title", "type": "string"},
            {"name": "genres", "type": "string"},
            {"name": "release_date", "type": "string"},
            {"name": "movie_id", "type": "string"},
            {"name": "user_id", "type": "string"}
        ]
    }
    """

    # Set up serializer and producer
    avro_serializer = AvroSerializer(schema_registry, schema_str)
    producer = Producer({
        'bootstrap.servers': config['BOOTSTRAP_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': config['KAFKA_KEY'],
        'sasl.password': config['KAFKA_SECRET']
    })

    # Process and produce records
    processed = 0
    for record in process_records(CSV_PATH, CHUNK_SIZE):
        producer.produce(
            topic=TOPIC_NAME,
            key=str(record['row_id']),
            value=avro_serializer(record, SerializationContext(TOPIC_NAME, MessageField.VALUE)),
            on_delivery=delivery_report
        )

        processed += 1
        if processed % CHUNK_SIZE == 0:
            print(f"Processed {processed:,} records")
            producer.flush()

    print(f"\nProcessed {processed:,} total records")
    producer.flush()


if __name__ == '__main__':
    main()
