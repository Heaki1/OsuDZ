const axios = require('axios');
const Bottleneck = require('bottleneck');
const { config } = require('../config/env');

// osu! API client configuration
const client_id = config.OSU_CLIENT_ID;
const client_secret = config.OSU_CLIENT_SECRET;
let access_token = null;
let token_expiry = 0;

// Rate limiter for API calls
const limiter = new Bottleneck({ 
  maxConcurrent: config.OSU_API_MAX_CONCURRENT, 
  minTime: config.OSU_API_MIN_TIME 
});

// Get access token for osu! API
async function getAccessToken() {
  const now = Date.now();
  if (access_token && now < token_expiry) return access_token;
  
  try {
    const response = await axios.post('https://osu.ppy.sh/oauth/token', {
      client_id, 
      client_secret, 
      grant_type: 'client_credentials', 
      scope: 'public'
    });
    
    access_token = response.data.access_token;
    token_expiry = now + (response.data.expires_in * 1000) - 10000;
    console.log('🔑 Obtained new osu! token');
    return access_token;
  } catch (err) {
    console.error('❌ Failed to get access token:', err.message);
    throw err;
  }
}

// Get user data
async function getUser(userId) {
  const token = await getAccessToken();
  
  return await limiter.schedule(async () => {
    const response = await axios.get(`https://osu.ppy.sh/api/v2/users/${userId}/osu`, {
      headers: { Authorization: `Bearer ${token}` }
    });
    return response.data;
  });
}

// Get user scores (best/recent)
async function getUserScores(userId, type = 'best', limit = 50, mode = 'osu') {
  const token = await getAccessToken();
  
  return await limiter.schedule(async () => {
    const response = await axios.get(`https://osu.ppy.sh/api/v2/users/${userId}/scores/${type}`, {
      headers: { Authorization: `Bearer ${token}` },
      params: { mode, limit }
    });
    return response.data;
  });
}

// Get beatmap scores
async function getBeatmapScores(beatmapId, limit = 50) {
  const token = await getAccessToken();
  
  return await limiter.schedule(async () => {
    const response = await axios.get(`https://osu.ppy.sh/api/v2/beatmaps/${beatmapId}/scores`, {
      headers: { Authorization: `Bearer ${token}` },
      params: { limit }
    });
    return response.data;
  });
}

// Get beatmap data
async function getBeatmap(beatmapId) {
  const token = await getAccessToken();
  
  return await limiter.schedule(async () => {
    const response = await axios.get(`https://osu.ppy.sh/api/v2/beatmaps/${beatmapId}`, {
      headers: { Authorization: `Bearer ${token}` }
    });
    return response.data;
  });
}

// Get country rankings
async function getCountryRankings(country = 'DZ', mode = 'osu', type = 'performance', cursor = null) {
  const token = await getAccessToken();
  
  return await limiter.schedule(async () => {
    const params = { country, mode, type };
    if (cursor) params.cursor_string = cursor;
    
    const response = await axios.get('https://osu.ppy.sh/api/v2/rankings/osu/performance', {
      headers: { Authorization: `Bearer ${token}` },
      params
    });
    return response.data;
  });
}

// Search beatmapsets
async function searchBeatmapsets(params = {}) {
  const token = await getAccessToken();
  
  return await limiter.schedule(async () => {
    const defaultParams = { 
      mode: 'osu', 
      nsfw: false, 
      sort: 'ranked_desc', 
      s: 'ranked' 
    };
    
    const response = await axios.get('https://osu.ppy.sh/api/v2/beatmapsets/search', {
      headers: { Authorization: `Bearer ${token}` },
      params: { ...defaultParams, ...params }
    });
    return response.data;
  });
}

// Search users
async function searchUsers(query, mode = 'user') {
  const token = await getAccessToken();
  
  return await limiter.schedule(async () => {
    const response = await axios.get('https://osu.ppy.sh/api/v2/search', {
      headers: { Authorization: `Bearer ${token}` },
      params: { mode, query }
    });
    return response.data;
  });
}

// Get multiplayer matches
async function getMultiplayerMatches(limit = 50) {
  const token = await getAccessToken();
  
  return await limiter.schedule(async () => {
    try {
      const response = await axios.get('https://osu.ppy.sh/api/v2/multiplayer/matches', {
        headers: { Authorization: `Bearer ${token}` },
        params: { limit }
      });
      return response.data;
    } catch (err) {
      // This endpoint might not be available, return empty result
      return { matches: [] };
    }
  });
}

// Get multiplayer match details
async function getMultiplayerMatch(matchId) {
  const token = await getAccessToken();
  
  return await limiter.schedule(async () => {
    const response = await axios.get(`https://osu.ppy.sh/api/v2/multiplayer/matches/${matchId}`, {
      headers: { Authorization: `Bearer ${token}` }
    });
    return response.data;
  });
}

// Get beatmap multiplayer scores (alternative approach)
async function getBeatmapMultiplayerScores(beatmapId, limit = 10) {
  const token = await getAccessToken();
  
  return await limiter.schedule(async () => {
    try {
      const response = await axios.get(`https://osu.ppy.sh/api/v2/beatmaps/${beatmapId}/multiplayer`, {
        headers: { Authorization: `Bearer ${token}` },
        params: { limit }
      });
      return response.data;
    } catch (err) {
      // Fallback to empty result if endpoint doesn't exist
      return { matches: [] };
    }
  });
}

// Batch get users (with rate limiting)
async function getBatchUsers(userIds, batchSize = 10) {
  const results = [];
  
  for (let i = 0; i < userIds.length; i += batchSize) {
    const batch = userIds.slice(i, i + batchSize);
    const batchPromises = batch.map(userId => 
      getUser(userId).catch(err => {
        console.warn(`Failed to get user ${userId}:`, err.message);
        return null;
      })
    );
    
    const batchResults = await Promise.all(batchPromises);
    results.push(...batchResults.filter(result => result !== null));
    
    // Small delay between batches
    if (i + batchSize < userIds.length) {
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }
  
  return results;
}

// Retry wrapper for API calls
async function withRetry(apiCall, maxRetries = 3, baseDelay = 1000) {
  let lastError;
  
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await apiCall();
    } catch (error) {
      lastError = error;
      
      // Don't retry certain errors
      if (error.response?.status === 404 || error.response?.status === 401) {
        throw error;
      }
      
      if (attempt < maxRetries - 1) {
        const delay = baseDelay * Math.pow(2, attempt);
        console.warn(`API call failed (attempt ${attempt + 1}), retrying in ${delay}ms:`, error.message);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }
  
  throw lastError;
}

// Test API connection
async function testApiConnection() {
  try {
    const token = await getAccessToken();
    return !!token;
  } catch (err) {
    console.error('osu! API connection test failed:', err.message);
    return false;
  }
}

// Get API rate limit status
function getRateLimitStatus() {
  return {
    running: limiter.running(),
    pending: limiter.pending(),
    done: limiter.done
  };
}

module.exports = {
  getAccessToken,
  getUser,
  getUserScores,
  getBeatmapScores,
  getBeatmap,
  getCountryRankings,
  searchBeatmapsets,
  searchUsers,
  getMultiplayerMatches,
  getMultiplayerMatch,
  getBeatmapMultiplayerScores,
  getBatchUsers,
  withRetry,
  testApiConnection,
  getRateLimitStatus,
  limiter
};