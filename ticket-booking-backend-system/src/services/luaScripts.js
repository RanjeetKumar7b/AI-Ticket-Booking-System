const redisClient = require("../config/redis");

// 1. Acquire seat lock
const LOCK_SEAT_SCRIPT = `
local seatKey = KEYS[1]
local userId = ARGV[1]
local ttl = tonumber(ARGV[2])

local status = redis.call('GET', seatKey)

if not status or status == 'AVAILABLE' then
    redis.call('SET', seatKey, 'LOCKED:' .. userId, 'EX', ttl)
    return 1
else
    return 0
end
`;

// 2. Confirm seat lock ownership and transition to CONFIRMING
const CONFIRM_SEAT_SCRIPT = `
local seatKey = KEYS[1]
local userId = ARGV[1]

local currentOwner = redis.call('GET', seatKey)

if not currentOwner then
    return -1
end

if currentOwner ~= 'LOCKED:' .. userId then
    return -2
end

local ttl = redis.call('TTL', seatKey)

redis.call(
    'SET',
    seatKey,
    'CONFIRMING:' .. userId,
    'EX',
    ttl
)

return 1
`;

// 3. Release seat lock only if the user owns it
const RELEASE_SEAT_SCRIPT = `
local seatKey = KEYS[1]
local userId = ARGV[1]

local currentOwner = redis.call('GET', seatKey)

if currentOwner == 'LOCKED:' .. userId
   or currentOwner == 'CONFIRMING:' .. userId then
    return redis.call('DEL', seatKey)
end

return 0
`;

async function acquireSeatLock(seatId, userId, ttlSeconds = 300) {
  const seatKey = `seat:${seatId}:status`;

  const result = await redisClient.eval(LOCK_SEAT_SCRIPT, {
    keys: [seatKey],
    arguments: [String(userId), String(ttlSeconds)],
  });

  return result === 1;
}

async function confirmSeatLock(seatId, userId) {
  const seatKey = `seat:${seatId}:status`;

  return redisClient.eval(CONFIRM_SEAT_SCRIPT, {
    keys: [seatKey],
    arguments: [String(userId)],
  });
}

async function releaseSeatLock(seatId, userId) {
  const seatKey = `seat:${seatId}:status`;

  return redisClient.eval(RELEASE_SEAT_SCRIPT, {
    keys: [seatKey],
    arguments: [String(userId)],
  });
}

module.exports = {
  acquireSeatLock,
  confirmSeatLock,
  releaseSeatLock,
};
