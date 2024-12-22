from app.kafka_producer.producer import produce_statistics_messages, produce_neo4j_statistics_messages

if __name__ == '__main__':
    produce_statistics_messages()
    produce_neo4j_statistics_messages()