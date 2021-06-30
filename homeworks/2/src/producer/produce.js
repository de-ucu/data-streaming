const zlib = require('zlib');
const readline = require('readline');
const { createReadStream } = require('fs');

const { Kafka } = require('kafkajs');


const BATCH_SIZE = 150;

const kafka = new Kafka({
  clientId: 'homework-2',
  brokers: ['broker_1:9092', 'broker_2:9092', 'broker_3:9092'],
});

const topic = 'homework-2';

const producer = kafka.producer();

const runProducer = async () => {
  await producer.connect();
  const rl = readline.createInterface({
      input: createReadStream('/data/input.gz').pipe(zlib.createGunzip()),
  });

  let batch = [];

  for await (const line of rl) {
    batch.push({
      value: line,
    });

    if (batch.length % BATCH_SIZE === 0) {
      await producer.send({
        topic,
        messages: batch,
      });
      batch = [];
    }
  }

  await producer.send({
    topic,
    messages: batch,
  });

  await producer.disconnect();
}

runProducer();
