const amqp = require('amqplib');

async function setup() {
    const connection = await amqp.connect('amqp://guest:guest@localhost:5672');
    const channel = await connection.createChannel();

    const exchange = 'logs';
    const queue = 'joshes_logs';
    const routingKey = 'info';

    await channel.assertExchange(exchange, 'direct', { durable: false });
    await channel.assertQueue(queue, { durable: false });
    await channel.bindQueue(queue, exchange, routingKey);

    console.log(`Queue ${queue} is bound to exchange ${exchange} with routing key ${routingKey}`);

    channel.consume(queue, (msg) => {
        if (msg !== null) {
            console.log(`[x] Received ${msg.content.toString()}`);
            channel.ack(msg);
        }
    });

    console.log(`[*] Waiting for messages in ${queue}. To exit press CTRL+C`);
}

setup().catch(console.error);
