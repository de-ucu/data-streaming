const { createWriteStream } = require('fs');

const { Kafka } = require('kafkajs')

const { CONSUMERS, PARTITIONS } = process.env;
const MESSAGES_COUNT = parseInt(process.env.MESSAGES_COUNT);

const topic = "homework-2";
let messagesProcessed = 0;

const kafka = new Kafka({
  clientId: 'homework-2',
  brokers: ['broker_1:9092', 'broker_2:9092', 'broker_3:9092'],
});
const stream = createWriteStream(`/output/log.txt`, { flags: 'a' });

const admin = kafka.admin();
const consumers = new Array(parseInt(CONSUMERS)).fill(0).map(() => kafka.consumer({
  groupId: 'test-group',
  allowAutoTopicCreation: false,
}));

const sleep = async (time) => {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve();
    }, time);
  })
}

const runConsumer = async () => {
  await admin.connect();
  await admin.createTopics({
    topics: [{
      topic,
      numPartitions: parseInt(PARTITIONS),
    }],
    waitForLeaders: true,
  });
  await admin.disconnect();

  await Promise.all(consumers.map((consumer) => consumer.connect()));
  await Promise.all(consumers.map((consumer) => consumer.subscribe({ topic, fromBeginning: true })));
  await Promise.all(consumers.map((consumer) => {
    consumer.run({
      partitionsConsumedConcurrently: parseInt(PARTITIONS),
      eachMessage: async ({ message }) => {
        await sleep(5);
        stream.write(`${message.timestamp},${+ new Date()}\n`, (err) => {
          if (err) {
            console.log(err);
          }
          messagesProcessed++;
        });
      },
    });
  }));


  setInterval(() => {
    if (messagesProcessed == MESSAGES_COUNT) {
      console.log("Consuming ended");
      process.exit(0);
    }
  }, 5000);

}


runConsumer();
