const express = require("express");
const router = express.Router();
const { acquireSeatLock, confirmSeatLock } = require("../services/luaScripts");
const { getChannel, EXCHANGE_NAME } = require("../config/rabbitmq");

const VALID_EVENT_TYPES = ["concert", "movie", "comedy"];
const receivedSeats = new Set();
const requestCounts = new Map();

// POST /api/book/hold -> Step 1: Atomic hold via Redis Lua
router.post("/hold", async (req, res) => {
  try {
    // console.log("Incoming Payload:", req.body);
    const { seatId, userId } = req.body;

    if (!seatId || !userId) {
      return res.status(400).json({ error: "seatId and userId are required" });
    }

    receivedSeats.add(seatId);
    requestCounts.set(seatId, (requestCounts.get(seatId) || 0) + 1);

    const locked = await acquireSeatLock(seatId, userId, 300);
    console.log("Lua Lock Result:", locked);
    if (!locked) {
      return res
        .status(409)
        .json({ success: false, message: "Seat is already locked or booked." });
    }

    return res.status(200).json({
      success: true,
      message: "Seat successfully locked for 5 minutes. Proceed to payment.",
      seatId,
      userId,
      expiresIn: 300,
    });
  } catch (error) {
    console.error("Hold error:", error);
    return res.status(500).json({ error: "Internal Server Error" });
  }
});

// POST /api/book/confirm -> Step 2: Payment verified, route to event-specific queue

router.post("/confirm", async (req, res) => {
  try {
    const { seatId, userId, paymentIntentId, expectedVersion } = req.body;

    // 1. Validate request
    if (
      !seatId ||
      !userId ||
      !paymentIntentId ||
      !Number.isInteger(expectedVersion)
    ) {
      return res.status(400).json({
        success: false,
        message: "Missing or invalid confirmation fields",
      });
    }

    // 2. Derive event type
    const eventType = seatId.split("_")[0];

    if (!VALID_EVENT_TYPES.includes(eventType)) {
      return res.status(400).json({
        success: false,
        message: "Invalid seat ID",
      });
    }

    // 3. Get RabbitMQ channel
    const channel = getChannel();

    if (!channel) {
      return res.status(503).json({
        success: false,
        message: "Message queue unavailable",
      });
    }

    // 4. Atomically validate ownership and reserve lock
    // const lockResult = await confirmSeatLock(seatId, userId);

    // if (lockResult === -1) {
    //   return res.status(409).json({
    //     success: false,
    //     message: "Seat lock expired. Please hold the seat again.",
    //   });
    // }

    // if (lockResult === -2) {
    //   return res.status(409).json({
    //     success: false,
    //     message: "You do not own this seat lock.",
    //   });
    // }

    // 5. Prepare booking event
    const orderPayload = {
      seatId,
      userId,
      paymentIntentId,
      expectedVersion,
      eventType,
      timestamp: Date.now(),
    };

    const routingKey = `order.${eventType}.created`;

    // 6. Publish event
    channel.publish(
      EXCHANGE_NAME,
      routingKey,
      Buffer.from(JSON.stringify(orderPayload)),
      {
        persistent: true,
        contentType: "application/json",
        messageId: paymentIntentId,
      },
    );

    // 7. Return accepted response
    return res.status(202).json({
      success: true,
      message: "Booking confirmation accepted",
      seatId,
      paymentIntentId,
      status: "PROCESSING",
    });
  } catch (error) {
    console.error("Confirmation error:", error);

    return res.status(500).json({
      success: false,
      message: "Unable to process booking confirmation",
    });
  }
});

router.get("/hold/stats", (req, res) => {
  res.json({
    uniqueSeatsReceived: receivedSeats.size,
    requestCounts: Object.fromEntries(requestCounts),
  });
});

module.exports = router;
