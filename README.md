# Netflix Clickstream Analytics
with Confluent Cloud and Flink SQL

A Python script that streams movie viewing event data from the [Netflix audience behaviour - UK movies dataset](https://www.kaggle.com/datasets/vodclickstream/netflix-audience-behaviour-uk-movies) from Kaggle, to Confluent Cloud using Avro serialization.

## Prerequisites

- Python 3.7+
- Confluent Cloud account
- A copy of the Netflix clickstream dataset on [Kaggle](https://www.kaggle.com/datasets/vodclickstream/netflix-audience-behaviour-uk-movies)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/athena15/kafka_movie_stream_producer.git
cd kafka_movie_stream_producer
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file in the project root with your Confluent Cloud credentials:
```
BOOTSTRAP_SERVER=your_bootstrap_server
SCHEMA_REGISTRY_URL=your_schema_registry_url
KAFKA_KEY=your_kafka_key
KAFKA_SECRET=your_kafka_secret
SR_KEY=your_schema_registry_key
SR_SECRET=your_schema_registry_secret
```

## Usage

1. Place your CSV file in the project directory
2. Update the `CSV_PATH` variable in the script if needed
3. Run the script:
```bash
python producer.py
```

## Data Format

The script expects a CSV file with the following columns:
- datetime
- duration
- title
- genres
- release_date
- movie_id
- user_id

## Configuration

Adjust these parameters at the top of the script as needed:
- `TOPIC_NAME`: Kafka topic name
- `CSV_PATH`: Path to your CSV file
- `CHUNK_SIZE`: Number of records to process in each batch
