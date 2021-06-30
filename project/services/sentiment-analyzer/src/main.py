import json
import numpy as np
from scipy.special import softmax
from confluent_kafka import Consumer, TopicPartition, Producer
import tritonclient.http as httpclient
from transformers import AutoTokenizer


BATCH_SIZE_CONSUME = 10
BATCH_SIZE_PRODUCE = 500
TOPIC = 'sentiments'
LABELS = ['negative', 'neutral', 'positive']


def preprocess(text):
    new_text = []

    for t in text.split(' '):
        t = '@user' if t.startswith('@') and len(t) > 1 else t
        t = 'http' if t.startswith('http') else t
        new_text.append(t)

    return ' '.join(new_text)


tokenizer = AutoTokenizer.from_pretrained('cardiffnlp/twitter-xlm-roberta-base-sentiment')
triton_client = httpclient.InferenceServerClient(url='triton:8000')

consumer = Consumer({
    'bootstrap.servers': 'broker_1,broker_2,broker_3',
    'group.id': 'sentiment-analysis',
    'auto.offset.reset': 'earliest',
    'client.id': 'sentiment-analyzer',
})
consumer.assign([TopicPartition('languages', 1)])

producer = Producer({
    'bootstrap.servers': 'broker_1,broker_2,broker_3',
})


input_name = ['input_ids', 'attention_mask']
output_name = 'output_0'

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

        encoded_input = tokenizer(texts, max_length=256, truncation=True, padding='max_length', return_tensors='np')
        input_ids = encoded_input['input_ids']
        attention_mask = encoded_input['attention_mask']

        input0 = httpclient.InferInput(input_name[0], (len(input_ids),  256), 'INT64')
        input0.set_data_from_numpy(input_ids, binary_data=False)
        input1 = httpclient.InferInput(input_name[1], (len(attention_mask), 256), 'INT64')
        input1.set_data_from_numpy(attention_mask, binary_data=False)

        output = httpclient.InferRequestedOutput(output_name,  binary_data=False)
        response = triton_client.infer('sentiment', model_version='1', inputs=[input0, input1], outputs=[output])
        logits = response.as_numpy(output_name)
        scores = softmax(logits, 1)
        predictions = np.argmax(scores, 1)

        results_batch.extend(zip(keys, predictions))
        messages_batch = {}

    if len(results_batch) >= BATCH_SIZE_PRODUCE:
        for key, sentiment in results_batch:
            producer.produce(TOPIC, str(sentiment).encode('utf-8'), key)
        producer.flush()
        results_batch = []

for key, sentiment in results_batch:
    producer.produce(TOPIC, str(sentiment).encode('utf-8'), key)
    producer.flush()

consumer.close()
