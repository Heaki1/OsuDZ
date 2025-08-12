const { redisClient, getCached, setCached, invalidateCache, getCacheKey } = require('../config/redis');
const { config } = require('../config/env');

// Cache service with enhanced functionality
class CacheService {
  constructor() {
    this.defaultTTL = config.DEFAULT_CACHE_TTL;
    this.prefixes = {
      player: 'player',
      leaderboard: 'leaderboard',
      rankings: 'rankings',
      search: 'search',
      analytics: 'analytics',
      beatmap: 'beatmap',
      discovery: 'discovery',
      achievements: 'achievements'
    };
  }

  // Player-related caching
  async cachePlayer(username, data, ttl = config.PLAYER_CACHE_TTL) {
    const key = getCacheKey(this.prefixes.player, username);
    return await setCached(key, data, ttl);
  }

  async getPlayerCache(username) {
    const key = getCacheKey(this.prefixes.player, username);
    try {
      const cached = await redisClient.get(key);
      return cached ? JSON.parse(cached) : null;
    } catch (err) {
      console.warn('Failed to get player cache:', err.message);
      return null;
    }
  }

  async invalidatePlayerCache(username) {
    const pattern = getCacheKey(this.prefixes.player, username) + '*';
    return await invalidateCache(pattern);
  }

  // Leaderboard caching
  async cacheLeaderboard(beatmapId, data, ttl = this.defaultTTL) {
    const key = getCacheKey(this.prefixes.leaderboard, beatmapId);
    return await setCached(key, data, ttl);
  }

  async getLeaderboardCache(beatmapId) {
    const key = getCacheKey(this.prefixes.leaderboard, beatmapId);
    try {
      const cached = await redisClient.get(key);
      return cached ? JSON.parse(cached) : null;
    } catch (err) {
      console.warn('Failed to get leaderboard cache:', err.message);
      return null;
    }
  }

  async invalidateLeaderboardCache(beatmapId = null) {
    const pattern = beatmapId 
      ? getCacheKey(this.prefixes.leaderboard, beatmapId)
      : getCacheKey(this.prefixes.leaderboard, '*');
    return await invalidateCache(pattern);
  }

  // Rankings caching
  async cacheRankings(params, data, ttl = config.RANKINGS_CACHE_TTL) {
    const { sort, limit, offset, timeframe } = params;
    const key = getCacheKey(this.prefixes.rankings, sort, limit, offset, timeframe);
    return await setCached(key, data, ttl);
  }

  async getRankingsCache(params) {
    const { sort, limit, offset, timeframe } = params;
    const key = getCacheKey(this.prefixes.rankings, sort, limit, offset, timeframe);
    try {
      const cached = await redisClient.get(key);
      return cached ? JSON.parse(cached) : null;
    } catch (err) {
      console.warn('Failed to get rankings cache:', err.message);
      return null;
    }
  }

  async invalidateRankingsCache() {
    const pattern = getCacheKey(this.prefixes.rankings, '*');
    return await invalidateCache(pattern);
  }

  // Search caching
  async cacheSearch(query, type, data, ttl = config.SEARCH_CACHE_TTL) {
    const key = getCacheKey(this.prefixes.search, type, query);
    return await setCached(key, data, ttl);
  }

  async getSearchCache(query, type) {
    const key = getCacheKey(this.prefixes.search, type, query);
    try {
      const cached = await redisClient.get(key);
      return cached ? JSON.parse(cached) : null;
    } catch (err) {
      console.warn('Failed to get search cache:', err.message);
      return null;
    }
  }

  async invalidateSearchCache() {
    const pattern = getCacheKey(this.prefixes.search, '*');
    return await invalidateCache(pattern);
  }

  // Analytics caching
  async cacheAnalytics(type, data, ttl = config.ANALYTICS_CACHE_TTL) {
    const key = getCacheKey(this.prefixes.analytics, type);
    return await setCached(key, data, ttl);
  }

  async getAnalyticsCache(type) {
    const key = getCacheKey(this.prefixes.analytics, type);
    try {
      const cached = await redisClient.get(key);
      return cached ? JSON.parse(cached) : null;
    } catch (err) {
      console.warn('Failed to get analytics cache:', err.message);
      return null;
    }
  }

  async invalidateAnalyticsCache() {
    const pattern = getCacheKey(this.prefixes.analytics, '*');
    return await invalidateCache(pattern);
  }

  // Beatmap metadata caching
  async cacheBeatmap(beatmapId, data, ttl = this.defaultTTL) {
    const key = getCacheKey(this.prefixes.beatmap, beatmapId);
    return await setCached(key, data, ttl);
  }

  async getBeatmapCache(beatmapId) {
    const key = getCacheKey(this.prefixes.beatmap, beatmapId);
    try {
      const cached = await redisClient.get(key);
      return cached ? JSON.parse(cached) : null;
    } catch (err) {
      console.warn('Failed to get beatmap cache:', err.message);
      return null;
    }
  }

  // Achievement caching
  async cacheAchievements(username, data, ttl = this.defaultTTL) {
    const key = getCacheKey(this.prefixes.achievements, username);
    return await setCached(key, data, ttl);
  }

  async getAchievementsCache(username) {
    const key = getCacheKey(this.prefixes.achievements, username);
    try {
      const cached = await redisClient.get(key);
      return cached ? JSON.parse(cached) : null;
    } catch (err) {
      console.warn('Failed to get achievements cache:', err.message);
      return null;
    }
  }

  async invalidateAchievementsCache(username = null) {
    const pattern = username 
      ? getCacheKey(this.prefixes.achievements, username)
      : getCacheKey(this.prefixes.achievements, '*');
    return await invalidateCache(pattern);
  }

  // Generic caching methods
  async set(key, data, ttl = this.defaultTTL) {
    return await setCached(key, data, ttl);
  }

  async get(key) {
    try {
      const cached = await redisClient.get(key);
      return cached ? JSON.parse(cached) : null;
    } catch (err) {
      console.warn('Failed to get cache:', err.message);
      return null;
    }
  }

  async del(key) {
    try {
      return await redisClient.del(key);
    } catch (err) {
      console.warn('Failed to delete cache key:', err.message);
      return false;
    }
  }

  async exists(key) {
    try {
      return await redisClient.exists(key);
    } catch (err) {
      console.warn('Failed to check cache existence:', err.message);
      return false;
    }
  }

  async expire(key, seconds) {
    try {
      return await redisClient.expire(key, seconds);
    } catch (err) {
      console.warn('Failed to set cache expiration:', err.message);
      return false;
    }
  }

  async ttl(key) {
    try {
      return await redisClient.ttl(key);
    } catch (err) {
      console.warn('Failed to get cache TTL:', err.message);
      return -1;
    }
  }

  // Batch operations
  async mget(keys) {
    try {
      const values = await redisClient.mGet(keys);
      return values.map(value => value ? JSON.parse(value) : null);
    } catch (err) {
      console.warn('Failed to get multiple cache keys:', err.message);
      return new Array(keys.length).fill(null);
    }
  }

  async mset(keyValuePairs, ttl = this.defaultTTL) {
    try {
      const pipeline = redisClient.multi();
      
      for (const [key, value] of keyValuePairs) {
        pipeline.set(key, JSON.stringify(value), { EX: ttl });
      }
      
      return await pipeline.exec();
    } catch (err) {
      console.warn('Failed to set multiple cache keys:', err.message);
      return false;
    }
  }

  // Cache warming
  async warmCache(warmingFunction, key, ttl = this.defaultTTL) {
    try {
      const data = await warmingFunction();
      if (data !== null && data !== undefined) {
        await this.set(key, data, ttl);
        return data;
      }
      return null;
    } catch (err) {
      console.warn('Failed to warm cache:', err.message);
      return null;
    }
  }

  // Cache invalidation patterns
  async invalidateAll() {
    try {
      await redisClient.flushAll();
      console.log('🧹 All cache cleared');
      return true;
    } catch (err) {
      console.error('Failed to clear all cache:', err.message);
      return false;
    }
  }

  async invalidateByPattern(pattern) {
    return await invalidateCache(pattern);
  }

  async invalidatePlayerRelatedCache(username) {
    const patterns = [
      getCacheKey(this.prefixes.player, username) + '*',
      getCacheKey(this.prefixes.achievements, username) + '*',
      getCacheKey(this.prefixes.rankings, '*'), // Rankings might change
      getCacheKey(this.prefixes.analytics, '*') // Analytics might change
    ];

    let totalInvalidated = 0;
    for (const pattern of patterns) {
      try {
        const keys = await redisClient.keys(pattern);
        if (keys.length > 0) {
          await redisClient.del(...keys);
          totalInvalidated += keys.length;
        }
      } catch (err) {
        console.warn(`Failed to invalidate pattern ${pattern}:`, err.message);
      }
    }

    console.debug(`Invalidated ${totalInvalidated} cache keys for player ${username}`);
    return totalInvalidated;
  }

  async invalidateLeaderboardRelatedCache(beatmapId = null) {
    const patterns = [
      beatmapId 
        ? getCacheKey(this.prefixes.leaderboard, beatmapId) + '*'
        : getCacheKey(this.prefixes.leaderboard, '*'),
      getCacheKey(this.prefixes.analytics, '*')
    ];

    let totalInvalidated = 0;
    for (const pattern of patterns) {
      try {
        const keys = await redisClient.keys(pattern);
        if (keys.length > 0) {
          await redisClient.del(...keys);
          totalInvalidated += keys.length;
        }
      } catch (err) {
        console.warn(`Failed to invalidate pattern ${pattern}:`, err.message);
      }
    }

    console.debug(`Invalidated ${totalInvalidated} cache keys for beatmap ${beatmapId || 'all'}`);
    return totalInvalidated;
  }

  // Cache statistics
  async getCacheStats() {
    try {
      const info = await redisClient.info('stats');
      const memory = await redisClient.info('memory');
      const keyspace = await redisClient.info('keyspace');
      
      return {
        info: info,
        memory: memory,
        keyspace: keyspace,
        timestamp: Date.now()
      };
    } catch (err) {
      console.error('Failed to get cache stats:', err.message);
      return null;
    }
  }

  async getCacheSize() {
    try {
      const keys = await redisClient.keys('*');
      return keys.length;
    } catch (err) {
      console.error('Failed to get cache size:', err.message);
      return 0;
    }
  }

  async getCacheHealth() {
    try {
      const start = Date.now();
      await redisClient.ping();
      const pingTime = Date.now() - start;
      
      const size = await this.getCacheSize();
      
      return {
        status: 'healthy',
        pingTime: pingTime,
        totalKeys: size,
        timestamp: Date.now()
      };
    } catch (err) {
      return {
        status: 'unhealthy',
        error: err.message,
        timestamp: Date.now()
      };
    }
  }

  // Utility methods
  generateKey(prefix, ...parts) {
    return getCacheKey(prefix, ...parts);
  }

  async getOrSet(key, fetchFunction, ttl = this.defaultTTL) {
    return await getCached(key, fetchFunction, ttl);
  }
}

// Helper function for cache-aside pattern
async function cacheAside(key, fetchFunction, ttl = config.DEFAULT_CACHE_TTL) {
  return await getCached(key, fetchFunction, ttl);
}

// Helper function for write-through pattern
async function writeThrough(key, data, writeFunction, ttl = config.DEFAULT_CACHE_TTL) {
  try {
    // Write to primary storage
    const result = await writeFunction(data);
    
    // Write to cache
    await setCached(key, result, ttl);
    
    return result;
  } catch (err) {
    console.error('Write-through cache failed:', err.message);
    throw err;
  }
}

// Helper function for write-behind pattern
async function writeBehind(key, data, writeFunction, ttl = config.DEFAULT_CACHE_TTL) {
  try {
    // Write to cache immediately
    await setCached(key, data, ttl);
    
    // Schedule write to primary storage
    setTimeout(async () => {
      try {
        await writeFunction(data);
      } catch (err) {
        console.error('Write-behind storage write failed:', err.message);
      }
    }, 0);
    
    return data;
  } catch (err) {
    console.error('Write-behind cache failed:', err.message);
    throw err;
  }
}

// Create singleton instance
const cacheService = new CacheService();

module.exports = {
  CacheService,
  cacheService,
  cacheAside,
  writeThrough,
  writeBehind
};