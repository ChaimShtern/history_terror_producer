import json
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

from app.service.convert_fiiles import convert_dict_for_sql
from app.uttils.static_varyebls import relevant_fields as relevant
from app.uttils.static_varyebls import relevant_neo4j_fields as relevant_neo4j

from app.service.read_fille import read_csv_file_to_json, global_terrorism_1000_rows_path, split_to_relevant_fields, \
    split_to_relevant_neo4j_fields, Worldwide_Terrorism_5000_rows
from toolz import partition_all

load_dotenv(verbose=True)

statistics_topic = os.environ['STATISTICS_TOPIC']
neo4j_topic = os.environ['NEO4J_TOPIC']


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
    data = data + [convert_dict_for_sql(dicty) for dicty in read_csv_file_to_json(Worldwide_Terrorism_5000_rows)]
    relevant_fields = split_to_relevant_fields(data, relevant)
    for batch in partition_all(200, relevant_fields):
        produce(
            topic=statistics_topic,
            value=batch)


def produce_neo4j_statistics_messages():
    data = read_csv_file_to_json(global_terrorism_1000_rows_path)
    relevant_neo4j_fields = split_to_relevant_neo4j_fields(data, relevant_neo4j)
    for batch in partition_all(200, relevant_neo4j_fields):
        produce(
            topic=neo4j_topic,
            value=batch)
