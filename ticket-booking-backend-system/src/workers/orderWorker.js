const pool = require('../config/db');
const { connectRabbitMQ, getChannel } = require('../config/rabbitmq');
const { releaseSeatLock } = require('../services/luaScripts');

const QUEUE_NAME = process.env.QUEUE_NAME || 'concert_order_queue';

async function startWorker() {
    await connectRabbitMQ();
    const channel = getChannel();

    // Limit unacknowledged messages per worker to protect MySQL connection pool
    await channel.prefetch(10);

    console.log(`Order worker listening on ${QUEUE_NAME}...`);

    channel.consume(QUEUE_NAME, async (msg) => {
        if (!msg) return;

        const content = JSON.parse(msg.content.toString());
        const { seatId, userId, paymentIntentId, expectedVersion, eventType } = content;

        const connection = await pool.getConnection();
        try {
            await connection.beginTransaction();

            // 1. Perform OCC update on MySQL seats table
            const [updateResult] = await connection.execute(
                `UPDATE seats 
                 SET status = 'SOLD', user_id = ?, version = version + 1 
                 WHERE seat_id = ? AND version = ? AND status = 'LOCKED'`,
                [userId, seatId, expectedVersion]
            );

            if (updateResult.affectedRows === 0) {
                await connection.rollback();
                throw new Error(`OCC_CONFLICT: Seat ${seatId} version mismatch or already updated.`);
            }

            // 2. Insert into immutable orders ledger
            await connection.execute(
                `INSERT INTO orders (payment_intent_id, seat_id, user_id, event_type, status, created_at) 
                 VALUES (?, ?, ?, ?, 'CONFIRMED', NOW())`,
                [paymentIntentId, seatId, userId, eventType]
            );

            await connection.commit();
            console.log(`[SUCCESS][${QUEUE_NAME}] Order committed for Seat ${seatId}, User ${userId}`);
            
            channel.ack(msg);

        } catch (error) {
            await connection.rollback();
            console.error(`[ERROR][${QUEUE_NAME}] Seat ${seatId}:`, error.message);

            await releaseSeatLock(seatId);
            channel.ack(msg);
        } finally {
            connection.release();
        }
    }, { noAck: false });
}

startWorker();
