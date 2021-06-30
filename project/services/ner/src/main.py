import json
import spacy
from confluent_kafka import Consumer, TopicPartition, Producer


nlp = spacy.load("xx_ent_wiki_sm")

BATCH_SIZE_CONSUME = 1000
BATCH_SIZE_PRODUCE = 2000
TOPIC = 'entities'

def preprocess(text):
    tokens = text.split(' ')
    tokens = [token for token in tokens if not (token.startswith("@") or token.startswith("#") or token.startswith("http"))]
    return ' '.join(tokens)


consumer = Consumer({
    'bootstrap.servers': 'broker_1,broker_2,broker_3',
    'group.id': 'ner',
    'auto.offset.reset': 'earliest',
    'client.id': 'ner',
})
consumer.assign([TopicPartition('languages', 1)])

producer = Producer({
    'bootstrap.servers': 'broker_1,broker_2,broker_3',
})


results_batch = []
messages_batch = {}

while True:
    msg = consumer.poll(0.0)
    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    messages_batch[msg.key().decode('utf-8')] = json.loads(msg.value().decode('utf-8'))

    if len(messages_batch) >= BATCH_SIZE_CONSUME:
        keys = messages_batch.keys()
        texts = [preprocess(value['text']) for value in messages_batch.values()]

        for doc in nlp.pipe(texts):
            results_batch.extend([ent.text for ent in doc.ents if ent.label_ == 'PER'])
        messages_batch = {}

    if len(results_batch) >= BATCH_SIZE_PRODUCE:
        for entity in results_batch:
            producer.produce(TOPIC, entity.encode('utf-8'))
        producer.flush()
        results_batch = []

for entity in results_batch:
    producer.produce(TOPIC, entity.encode('utf-8'))
producer.flush()

consumer.close()