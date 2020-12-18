import json
import argparse

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import numpy as np
import pandas as pd
from datetime import datetime

import time
import random

_KAFKA_HOST = '127.0.0.1:9092'
_KAFKA_INPUT_TOPIC = 'delta_products'


def send_message(producer, topic, input_file):

    df_rows = pd.read_csv(input_file)
    df_rows = df_rows.fillna(0)
    for _, row in df_rows.iterrows():

        time.sleep(random.randint(0, 3))
        json_data = json.dumps(
            {
                'ProductID': row['ProductID'],
                'Date': row['Date'],
                'Price': row['Price'],
                'Quantity': row['Quantity'],
            }
        ).encode('utf-8')
        print(json_data)
        producer.send(topic, json_data)

    producer.flush()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', required=True)
    args = parser.parse_args()

    print('Create Kafka topics: {}'.format(_KAFKA_INPUT_TOPIC))
    admin_client = KafkaAdminClient(bootstrap_servers=_KAFKA_HOST, client_id='bank_client')
    topic_list = []
    topic_list.append(NewTopic(name=_KAFKA_INPUT_TOPIC, num_partitions=1, replication_factor=1))

    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except TopicAlreadyExistsError as err:
        print('Topics already exisit... skip')

    print('Pushing data to Kafka topic: {}'.format(_KAFKA_INPUT_TOPIC))
    producer = KafkaProducer(bootstrap_servers=_KAFKA_HOST)

    send_message(producer=producer, topic=_KAFKA_INPUT_TOPIC, input_file=args.input)
