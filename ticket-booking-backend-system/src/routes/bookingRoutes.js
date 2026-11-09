const express = require('express');
const router = express.Router();
const { acquireSeatLock } = require('../services/luaScripts');
const { getChannel, EXCHANGE_NAME } = require('../config/rabbitmq');

const VALID_EVENT_TYPES = ['concert', 'movie', 'comedy'];

// POST /api/book/hold -> Step 1: Atomic hold via Redis Lua
router.post('/hold', async (req, res) => {
    try {
        const { seatId, userId } = req.body;
        if (!seatId || !userId) {
            return res.status(400).json({ error: 'seatId and userId are required' });
        }

        const locked = await acquireSeatLock(seatId, userId, 300);
        if (!locked) {
            return res.status(409).json({ success: false, message: 'Seat is already locked or booked.' });
        }

        return res.status(200).json({ 
            success: true, 
            message: 'Seat successfully locked for 5 minutes. Proceed to payment.',
            seatId,
            userId,
            expiresIn: 300
        });
    } catch (error) {
        console.error('Hold error:', error);
        return res.status(500).json({ error: 'Internal Server Error' });
    }
});

// POST /api/book/confirm -> Step 2: Payment verified, route to event-specific queue
router.post('/confirm', async (req, res) => {
    try {
        const { seatId, userId, paymentIntentId, expectedVersion, eventType } = req.body;
        if (!seatId || !userId || !paymentIntentId || expectedVersion === undefined || !eventType) {
            return res.status(400).json({ error: 'Missing required fields for confirmation' });
        }

        if (!VALID_EVENT_TYPES.includes(eventType)) {
            return res.status(400).json({ error: 'Invalid eventType. Must be concert, movie, or comedy.' });
        }

        const channel = getChannel();
        if (!channel) {
            return res.status(503).json({ error: 'Message queue service unavailable' });
        }

        const orderPayload = {
            seatId,
            userId,
            paymentIntentId,
            expectedVersion,
            eventType,
            timestamp: Date.now()
        };

        const routingKey = `order.${eventType}.created`;

        channel.publish(
            EXCHANGE_NAME,
            routingKey,
            Buffer.from(JSON.stringify(orderPayload)),
            { persistent: true }
        );

        return res.status(202).json({
            success: true,
            message: `Payment verified and order queued under ${routingKey}.`,
            paymentIntentId
        });
    } catch (error) {
        console.error('Confirmation error:', error);
        return res.status(500).json({ error: 'Internal Server Error' });
    }
});

module.exports = router;
