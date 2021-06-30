const { createWriteStream } = require('fs');

const { Kafka } = require('kafkajs');


const kafka = new Kafka({
    clientId: 'statistics-collector',
    brokers: ['broker_1:9092', 'broker_2:9092', 'broker_3:9092'],
})

const collect = async(topic) => {
    const consumer = kafka.consumer({
        groupId: `statistics-${topic}`,
        allowAutoTopicCreation: false,
    });
    const stream = createWriteStream(`/statistics/${topic}.txt`, { flags: 'a' });

    await consumer.connect();
    await consumer.subscribe({ topic });
    await consumer.run({
        eachMessage: async ({ message }) => {
            stream.write(`${message.value.toString()}\n`, (err) => {
                if (err) {
                  console.log(err);
                }
            });
        },
    });
}

collect('languages');
collect('sentiments');
collect('entities');
