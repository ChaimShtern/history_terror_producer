import json
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
from app.uttils.static_varyebls import relevant_fields as relevant

from app.service.read_fille import read_csv_file_to_json, global_terrorism_1000_rows_path, split_to_relevant_fields
from toolz import partition_all

load_dotenv(verbose=True)

statistics_topic = os.environ['STATISTICS_TOPIC']


def produce(topic: str, value):
    producer = KafkaProducer(
        bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'],
        value_serializer=lambda v: json.dumps(v).encode()
    )
    producer.send(
        topic=topic,
        value=value
    )


def produce_statistics_messages():
    data = read_csv_file_to_json(global_terrorism_1000_rows_path)
    relevant_fields = split_to_relevant_fields(data, relevant)
    for batch in partition_all(200, relevant_fields):
        print(json.dumps(batch))
        produce(
            topic=statistics_topic,
            value=json.dumps(batch))
