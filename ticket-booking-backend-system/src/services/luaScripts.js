const redisClient = require('../config/redis');

const LOCK_SEAT_SCRIPT = `
local seatKey = KEYS[1]
local userId = ARGV[1]
local ttl = tonumber(ARGV[2])

local status = redis.call('GET', seatKey)

if not status or status == 'AVAILABLE' then
    redis.call('SETEX', seatKey, ttl, 'LOCKED:' .. userId)
    return 1
else
    return 0
end
`;

async function acquireSeatLock(seatId, userId, ttlSeconds = 300) {
    const seatKey = `seat:${seatId}:status`;
    const result = await redisClient.eval(LOCK_SEAT_SCRIPT, {
        keys: [seatKey],
        arguments: [userId, ttlSeconds.toString()]
    });
    return result === 1;
}

async function releaseSeatLock(seatId) {
    const seatKey = `seat:${seatId}:status`;
    await redisClient.del(seatKey);
}

module.exports = { acquireSeatLock, releaseSeatLock };
