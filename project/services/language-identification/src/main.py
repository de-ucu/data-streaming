import os
import json

from confluent_kafka import Consumer, Producer

from helpers import get_language


BATCH_SIZE = 10000
TOPIC = 'languages'
LANGUAGES = set(os.environ['SELECTED_LANGUAGES'].split(','))

consumer = Consumer({
    'bootstrap.servers': 'broker_1,broker_2,broker_3',
    'group.id': 'language-identification',
    'auto.offset.reset': 'earliest',
    'client.id': 'language-identifier',
})
consumer.subscribe(['tweets'])

producer = Producer({
    'bootstrap.servers': 'broker_1,broker_2,broker_3',
})

messages_batch = []
keys_batch = []

while True:
    msg = consumer.poll(0.0)
    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    keys_batch.append(msg.key().decode('utf-8'))
    message = msg.value().decode('utf-8')
    messages_batch.append(message)

    if len(messages_batch) == BATCH_SIZE:
        languages = get_language(messages_batch)

        for i, m in enumerate(messages_batch):
            language = languages[i]
            partition = 1 if language in LANGUAGES else 0
            value = json.dumps({
                'lang': language,
                'text': m,
            })
            producer.produce(TOPIC, value.encode('utf-8'), keys_batch[i], partition)

        producer.flush()
        messages_batch = []
        keys_batch = []

consumer.close()
f.close()