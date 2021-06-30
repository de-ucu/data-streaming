const zlib = require('zlib');
const readline = require('readline');
const { createReadStream } = require('fs');

const { Kafka } = require('kafkajs');


const TOPIC = 'tweets';
const BATCH_SIZE = 1000;

const kafka = new Kafka({
  clientId: 'data-reader',
  brokers: ['broker_1:9092', 'broker_2:9092', 'broker_3:9092'],
});

const producer = kafka.producer();


const runProducer = async () => {
  await producer.connect();
  const rl = readline.createInterface({
    input: createReadStream('/data/input.gz').pipe(zlib.createGunzip()),
  });
  let batch = [];

  for await (const line of rl) {
    const json = JSON.parse(line);
    const { id_str, text } = json;
    batch.push({
      key: id_str,
      value: text,
    });

    if (batch.length % BATCH_SIZE === 0) {
      await producer.send({
        topic: TOPIC,
        messages: batch,
      });
      batch = [];
    }
  }

  await producer.send({
    topic: TOPIC,
    messages: batch,
  });

  await producer.disconnect();
}

runProducer();
