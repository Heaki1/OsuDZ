const redis = require('redis');
const { config } = require('./env');

// Redis configuration
const url = config.REDIS_URL;
let redisOptions = { url };

if (url && url.startsWith('rediss://')) {
  const { hostname } = new URL(url);
  redisOptions.socket = {
    tls: true,
    servername: hostname,
    rejectUnauthorized: false
  };
}

// Create Redis client
const redisClient = redis.createClient(redisOptions);

// Event handlers
redisClient.on('error', (err) => {
  console.error('[Redis Error]', err.message);
});

redisClient.on('connect', () => {
  console.log('✅ Connected to Redis');
});

redisClient.on('ready', () => {
  console.log('🔴 Redis client ready');
});

redisClient.on('end', () => {
  console.log('🔴 Redis connection ended');
});

// Connect to Redis
async function connectRedis() {
  try {
    await redisClient.connect();
    return true;
  } catch (err) {
    console.error('❌ Redis connection failed:', err.message);
    return false;
  }
}

// Disconnect from Redis
async function disconnectRedis() {
  try {
    await redisClient.quit();
    console.log('🔴 Redis connection closed');
  } catch (err) {
    console.error('Redis close error:', err.message);
  }
}

// Progress tracking helpers
async function saveProgress(key, value) {
  try {
    await redisClient.set(`progress:${key}`, value);
  } catch (err) {
    console.warn('Failed to save progress:', err.message);
  }
}

async function getProgress(key) {
  try {
    return await redisClient.get(`progress:${key}`);
  } catch (err) {
    console.warn('Failed to get progress:', err.message);
    return null;
  }
}

// Cache utilities
const getCacheKey = (prefix, ...parts) => {
  return `${prefix}:${parts.map(p => String(p).toLowerCase()).join(':')}`;
};

async function getCached(key, fetchFunction, ttl = config.DEFAULT_CACHE_TTL) {
  try {
    const cached = await redisClient.get(key);
    if (cached) return JSON.parse(cached);
    
    const data = await fetchFunction();
    await redisClient.set(key, JSON.stringify(data), { EX: ttl });
    return data;
  } catch (err) {
    console.warn('Cache error, falling back to direct fetch:', err.message);
    return await fetchFunction();
  }
}

async function setCached(key, data, ttl = config.DEFAULT_CACHE_TTL) {
  try {
    await redisClient.set(key, JSON.stringify(data), { EX: ttl });
    return true;
  } catch (err) {
    console.warn('Cache set error:', err.message);
    return false;
  }
}

async function invalidateCache(pattern) {
  try {
    const keys = await redisClient.keys(pattern);
    if (keys.length > 0) {
      await redisClient.del(...keys);
      console.debug(`Invalidated ${keys.length} cache keys`);
    }
  } catch (err) {
    console.warn('Cache invalidation failed:', err.message);
  }
}

async function clearAllCache() {
  try {
    await redisClient.flushAll();
    console.log('🧹 All cache cleared');
  } catch (err) {
    console.error('Clear cache failed:', err.message);
  }
}

// Cache statistics
async function getCacheStats() {
  try {
    const info = await redisClient.info('stats');
    return info;
  } catch (err) {
    console.error('Failed to get cache stats:', err.message);
    return null;
  }
}

// Test Redis connection
async function testRedisConnection() {
  try {
    await redisClient.ping();
    return true;
  } catch (err) {
    console.error('Redis ping failed:', err.message);
    return false;
  }
}

module.exports = {
  redisClient,
  connectRedis,
  disconnectRedis,
  saveProgress,
  getProgress,
  getCacheKey,
  getCached,
  setCached,
  invalidateCache,
  clearAllCache,
  getCacheStats,
  testRedisConnection
};