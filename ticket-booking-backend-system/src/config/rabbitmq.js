const amqp = require('amqplib');
require('dotenv').config();

let channel = null;
let connection = null;

const EXCHANGE_NAME = 'ticket_topic_exchange';

async function connectRabbitMQ() {
    try {
        const rabbitUrl = process.env.RABBITMQ_URL || 'amqp://localhost:5672';
        connection = await amqp.connect(rabbitUrl);
        channel = await connection.createChannel();
        
        await channel.assertExchange(EXCHANGE_NAME, 'topic', { durable: true });

        // Assert 3 isolated queues for each event type
        await channel.assertQueue('concert_order_queue', { durable: true });
        await channel.assertQueue('movie_order_queue', { durable: true });
        await channel.assertQueue('comedy_order_queue', { durable: true });

        // Bind queues to topic routing patterns
        await channel.bindQueue('concert_order_queue', EXCHANGE_NAME, 'order.concert.*');
        await channel.bindQueue('movie_order_queue', EXCHANGE_NAME, 'order.movie.*');
        await channel.bindQueue('comedy_order_queue', EXCHANGE_NAME, 'order.comedy.*');

        console.log('Connected to RabbitMQ (Topic Exchange & 3 Event Queues) successfully.');
        return channel;
    } catch (error) {
        console.error('RabbitMQ Connection Error:', error);
        setTimeout(connectRabbitMQ, 5000);
    }
}

function getChannel() {
    return channel;
}

module.exports = { connectRabbitMQ, getChannel, EXCHANGE_NAME };
