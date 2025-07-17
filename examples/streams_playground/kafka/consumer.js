const kafka = require('kafka-node');

// kafka:
//     brokers:
//         - broker1:9092
// security_protocol: PLAINTEXT  # Change to SASL_SSL if using SASL
// sasl_mechanisms: PLAIN        # Remove if not using SASL
// sasl_username: <CLUSTER API KEY>  # Omit if not using SASL
//     sasl_password: <CLUSTER API SECRET>  # Omit if not using SASL
//         acks: all
//         dr_msg_cb: true
//         topics:
//         - topic: my-topic
//         key: my-key
//         networks:
//         - ethereum
//         events:
//         - Transfer

const client = new kafka.KafkaClient({ kafkaHost: 'localhost:9092' });
const consumer = new kafka.Consumer(
    client,
    [{ topic: 'test-topic', partition: 0 }],
    {
        autoCommit: true,
        fromOffset: 'earliest'
    }
);

consumer.on('message', function (message) {
    console.log('Message consumed:', message);
});

consumer.on('error', function (err) {
    console.error('Consumer error:', err);
});

console.log('Consumer started');

