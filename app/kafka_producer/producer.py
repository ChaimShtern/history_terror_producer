import json
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
import pycountry

from app.service.convert_fiiles import convert_dict_for_sql, convert_dict_for_neo4j, convert_dicts_for_elastic, \
    convert_for_elastic_csv1_to_csv2
from app.uttils.static_varyebls import relevant_fields as relevant, relevant_elastic
from app.uttils.static_varyebls import relevant_neo4j_fields as relevant_neo4j
from app.service.read_fille import (
    read_csv_file_to_json,
    global_terrorism_1000_rows_path,
    split_to_relevant_fields,
    split_to_relevant_neo4j_fields,
    Worldwide_Terrorism_5000_rows
)
from toolz import partition_all

load_dotenv(verbose=True)

statistics_topic = os.environ['STATISTICS_TOPIC']
neo4j_topic = os.environ['NEO4J_TOPIC']
elastic_topik = os.environ['ELASTIC_TOPIC']


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        if hasattr(obj, '__dict__'):  # For pycountry objects and similar
            return str(obj)
        try:
            return json.JSONEncoder.default(self, obj)
        except TypeError:
            return str(obj)


producer = KafkaProducer(
    bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'],
    value_serializer=lambda v: json.dumps(v, cls=CustomJSONEncoder).encode()
)


def produce(topic: str, value):
    producer.send(
        topic=topic,
        value=value
    )


def sanitize_data(obj):
    if isinstance(obj, dict):
        return {k: sanitize_data(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_data(item) for item in obj]
    elif isinstance(obj, set):
        return list(obj)
    elif hasattr(obj, '__dict__'):  # For pycountry objects
        return str(obj)
    return obj


def produce_statistics_messages():
    data = read_csv_file_to_json(global_terrorism_1000_rows_path)
    relevant_fields = split_to_relevant_fields(data, relevant)
    converted_data = [convert_dict_for_sql(dicty) for dicty in
                      read_csv_file_to_json(Worldwide_Terrorism_5000_rows)]
    relevant_fields += converted_data

    # Sanitize data before sending
    sanitized_fields = sanitize_data(relevant_fields)

    for batch in partition_all(200, sanitized_fields):
        produce(
            topic=statistics_topic,
            value=batch)


def produce_neo4j_statistics_messages():
    data = read_csv_file_to_json(global_terrorism_1000_rows_path)
    relevant_neo4j_fields = split_to_relevant_neo4j_fields(data, relevant_neo4j)
    converted_data = [convert_dict_for_neo4j(dicty) for dicty in
                      read_csv_file_to_json(Worldwide_Terrorism_5000_rows)]
    relevant_neo4j_fields += converted_data

    # Sanitize data before sending
    sanitized_fields = sanitize_data(relevant_neo4j_fields)

    for batch in partition_all(200, sanitized_fields):
        produce(
            topic=neo4j_topic,
            value=batch
        )


def produce_elastic_statistics_messages():
    csv1 = read_csv_file_to_json(global_terrorism_1000_rows_path)
    csv2 = read_csv_file_to_json(Worldwide_Terrorism_5000_rows)

    relevant_elastic_fields = split_to_relevant_fields(csv1, relevant_elastic)

    converted_data = convert_for_elastic_csv1_to_csv2(csv2)
    total_list = relevant_elastic_fields + converted_data

    res = convert_dicts_for_elastic(total_list)


    # Sanitize data before sending
    sanitized_fields = sanitize_data(res)

    for batch in partition_all(200, sanitized_fields):
        print(batch[0])
        produce(
            topic=elastic_topik,
            value=batch
        )
