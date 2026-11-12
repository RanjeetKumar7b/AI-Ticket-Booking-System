const pool = require("../config/db");
const { connectRabbitMQ, getChannel } = require("../config/rabbitmq");
const { releaseSeatLock } = require("../services/luaScripts");

const QUEUE_NAME = process.env.QUEUE_NAME || "concert_order_queue";

async function startWorker() {
  await connectRabbitMQ();

  const channel = getChannel();

  await channel.prefetch(10);

  console.log(`Order worker listening on ${QUEUE_NAME}...`);

  channel.consume(
    QUEUE_NAME,
    async (msg) => {
      if (!msg) return;

      let connection;

      try {
        const content = JSON.parse(msg.content.toString());

        const { seatId, userId, paymentIntentId, expectedVersion, eventType } =
          content;

        if (
          !seatId ||
          !userId ||
          !paymentIntentId ||
          !Number.isInteger(expectedVersion) ||
          !eventType
        ) {
          console.error("Invalid booking message:", content);

          // Permanently reject malformed messages.
          channel.nack(msg, false, false);
          return;
        }

        connection = await pool.getConnection();

        await connection.beginTransaction();

        console.log(
          `[PROCESSING] Seat ${seatId}, User ${userId}, Version ${expectedVersion}`,
        );

        // 1. OCC update
        const [updateResult] = await connection.execute(
          `UPDATE seats
           SET status = 'SOLD',
               user_id = ?,
               version = version + 1
           WHERE seat_id = ?
             AND version = ?
             AND status = 'AVAILABLE'`,
          [userId, seatId, expectedVersion],
        );

        if (updateResult.affectedRows === 0) {
          await connection.rollback();

          console.log(`[OCC_CONFLICT] Seat ${seatId}`);

          // Business conflict: retrying won't fix this.
          await releaseSeatLock(seatId, userId);

          channel.ack(msg);
          return;
        }

        // 2. Insert order
        await connection.execute(
          `INSERT INTO orders
           (payment_intent_id, seat_id, user_id, event_type, status, created_at)
           VALUES (?, ?, ?, ?, 'CONFIRMED', NOW())`,
          [paymentIntentId, seatId, userId, eventType],
        );

        // 3. Commit both operations together
        await connection.commit();

        console.log(
          `[SUCCESS] Order committed for Seat ${seatId}, User ${userId}`,
        );

        // 4. Acknowledge only after successful commit
        channel.ack(msg);
      } catch (error) {
        if (connection) {
          try {
            await connection.rollback();
          } catch (rollbackError) {
            console.error("Rollback failed:", rollbackError.message);
          }
        }

        console.error(`[WORKER_ERROR] ${error.message}`);

        // Temporary infrastructure errors:
        // requeue so RabbitMQ can deliver the message again.
        channel.nack(msg, false, true);
      } finally {
        if (connection) {
          connection.release();
        }
      }
    },
    { noAck: false },
  );
}

startWorker().catch((error) => {
  console.error("Worker startup failed:", error);
  process.exit(1);
});
