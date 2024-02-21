"""
This script produce the event/payload on kafka topic.
"""

import json
from kafka import KafkaProducer
from ratelimit import limits, RateLimitException, sleep_and_retry

ONE_MINUTE = 60
MAX_CALLS_PER_MINUTE = 500
TOPIC = '<Topic_NAME>'
EVENTS_FILE_PATH = '<file_path>'


producer = KafkaProducer(bootstrap_servers=['localhost:9092'])


@sleep_and_retry
@limits(calls=MAX_CALLS_PER_MINUTE, period=ONE_MINUTE)
def _produce(json_message):
    key = json_message['context']['key']
    value = json.dumps(json_message)
    print(f'sending {key} : {value}')
    producer.send(TOPIC, key=key, value=value)


def produce():
    with open(EVENTS_FILE_PATH, 'r+') as payload_file_obj:
        payload = json.load(payload_file_obj)
        for json_message in payload:
            _produce(json_message)

    producer.flush()


if __name__ == "__main__":
    produce()
