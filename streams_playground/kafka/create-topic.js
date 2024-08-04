const kafka = require("kafka-node");
const { KafkaClient, Admin } = kafka;

const client = new KafkaClient({ kafkaHost: 'localhost:9092' });
const admin = new Admin(client);

const topicToCreate = [
    {
        topic: 'test-topic',
        partitions: 1,
        replicationFactor: 1
    }
];

admin.createTopics(topicToCreate, (error, result) => {
    if (error) {
        console.error('Failed to create topic:', error);
    } else {
        console.log('Topic created successfully:', result);
    }
    client.close();
});