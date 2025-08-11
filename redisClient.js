// redisClient.js
const Redis = require('ioredis');  // or 'redis' package if you prefer

// Connect using Redis URL from environment variable or fallback to localhost
const redisClient = new Redis(process.env.REDIS_URL || 'redis://127.0.0.1:6379');

redisClient.on('connect', () => {
  console.log('🔴 Redis client connected');
});

redisClient.on('error', (err) => {
  console.error('Redis client error:', err);
});

module.exports = redisClient;
