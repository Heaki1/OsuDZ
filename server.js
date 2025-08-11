require('dotenv').config();
const express = require('express');
const axios = require('axios');
const cors = require('cors');
const path = require('path');
const Bottleneck = require('bottleneck');
const { Pool } = require('pg');
const redis = require('redis');
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');
const rateLimit = require('express-rate-limit');
const helmet = require('helmet');
const { createServer } = require('http');
const WebSocket = require('ws');

// ==================== ENVIRONMENT VALIDATION ====================
const requiredEnvVars = [
  'OSU_CLIENT_ID', 'OSU_CLIENT_SECRET', 
  'DATABASE_URL', 'REDIS_URL', 'JWT_SECRET'
];

requiredEnvVars.forEach(envVar => {
  if (!process.env[envVar]) {
    console.error(`❌ Missing required environment variable: ${envVar}`);
    process.exit(1);
  }
});

// ==================== APP INITIALIZATION ====================
const app = express();
const port = process.env.PORT || 3000;

app.set('trust proxy', 1);

// Enhanced security middleware
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'", "'unsafe-inline'"],
      styleSrc: ["'self'", "'unsafe-inline'", "https://cdnjs.cloudflare.com"],
      fontSrc: ["'self'", "https://cdnjs.cloudflare.com"]
    }
  }
}));

// Enhanced rate limiting
const createRateLimit = (windowMs, max, message) => rateLimit({
  windowMs,
  max: (req) => req.user ? max * 2 : max, // Higher limits for authenticated users
  message: { success: false, error: message },
  standardHeaders: true,
  legacyHeaders: false,
});

app.use('/api/', createRateLimit(15 * 60 * 1000, 100, 'Too many API requests'));
app.use('/api/admin/', createRateLimit(15 * 60 * 1000, 20, 'Too many admin requests'));

// ==================== LOGGING SYSTEM ====================
const colors = {
  reset: '\x1b[0m', bright: '\x1b[1m', red: '\x1b[31m', green: '\x1b[32m',
  yellow: '\x1b[33m', blue: '\x1b[34m', magenta: '\x1b[35m', cyan: '\x1b[36m'
};

function log(level, ...args) {
  const ts = new Date().toISOString().replace('T', ' ').split('.')[0];
  const levelColors = { INFO: colors.green, WARN: colors.yellow, ERROR: colors.red, DEBUG: colors.blue };
  const color = levelColors[level] || colors.reset;
  console.log(`${color}[${ts}] ${level}:${colors.reset}`, ...args);
}

// ==================== REDIS SETUP ====================
const url = process.env.REDIS_URL;
let redisOptions = { url };

if (url && url.startsWith('rediss://')) {
  const { hostname } = new URL(url);
  redisOptions.socket = {
    tls: true,
    servername: hostname,
    rejectUnauthorized: false
  };
}

const redisClient = redis.createClient(redisOptions);

redisClient.on('error', (err) => {
  log('ERROR', '[Redis Error]', err.message);
});

redisClient.on('connect', () => {
  log('INFO', '✅ Connected to Redis');
});

redisClient.connect().catch((err) => {
  log('ERROR', '❌ Redis connection failed:', err.message);
});

// ==================== DATABASE SETUP ====================
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

(async () => {
  try {
    const res = await pool.query('SELECT current_database(), current_schema()');
    log('INFO', `Connected to DB: ${res.rows[0].current_database}, schema: ${res.rows[0].current_schema}`);

    // === Auto-create missing columns for players ===
    await pool.query(`
      ALTER TABLE players 
      ADD COLUMN IF NOT EXISTS last_seen TIMESTAMP DEFAULT now();
    `);

    // === Auto-create missing columns for algeria_top50 ===
    await pool.query(`
      ALTER TABLE algeria_top50
      ADD COLUMN IF NOT EXISTS artist TEXT,
      ADD COLUMN IF NOT EXISTS difficulty_name TEXT,
      ADD COLUMN IF NOT EXISTS pp REAL;
    `);

    // === Auto-create missing columns for player_stats ===
    await pool.query(`
      ALTER TABLE player_stats
      ADD COLUMN IF NOT EXISTS user_id BIGINT,
      ADD COLUMN IF NOT EXISTS last_seen TIMESTAMP DEFAULT now(),
      ADD COLUMN IF NOT EXISTS join_date TIMESTAMP;
    `);

    log('INFO', '✅ Schema check completed (missing columns added if needed)');
  } catch (err) {
    log('ERROR', 'DB check failed:', err.message);
  }
})();

// Database helper functions
async function query(sql, params = []) { 
  const client = await pool.connect();
  try {
    return await client.query(sql, params);
  } finally {
    client.release();
  }
}

async function getRows(sql, params = []) { 
  return (await query(sql, params)).rows; 
}

async function getRow(sql, params = []) { 
  return (await query(sql, params)).rows[0]; 
}

// Progress tracking helpers
async function saveProgress(key, value) {
  try {
    await redisClient.set(`progress:${key}`, value);
  } catch (err) {
    log('WARN', 'Failed to save progress:', err.message);
  }
}

async function getProgress(key) {
  try {
    return await redisClient.get(`progress:${key}`);
  } catch (err) {
    log('WARN', 'Failed to get progress:', err.message);
    return null;
  }
}

// ==================== CACHING UTILITIES ====================
const getCacheKey = (prefix, ...parts) => {
  return `${prefix}:${parts.map(p => String(p).toLowerCase()).join(':')}`;
};

async function getCached(key, fetchFunction, ttl = 300) {
  try {
    const cached = await redisClient.get(key);
    if (cached) return JSON.parse(cached);
    
    const data = await fetchFunction();
    await redisClient.setex(key, ttl, JSON.stringify(data));
    return data;
  } catch (err) {
    log('WARN', 'Cache error, falling back to direct fetch:', err.message);
    return await fetchFunction();
  }
}

async function invalidateCache(pattern) {
  try {
    const keys = await redisClient.keys(pattern);
    if (keys.length > 0) {
      await redisClient.del(...keys);
      log('DEBUG', `Invalidated ${keys.length} cache keys`);
    }
  } catch (err) {
    log('WARN', 'Cache invalidation failed:', err.message);
  }
}

// ==================== OSU! API CLIENT ====================
const client_id = process.env.OSU_CLIENT_ID;
const client_secret = process.env.OSU_CLIENT_SECRET;
let access_token = null;
let token_expiry = 0;

async function getAccessToken() {
  const now = Date.now();
  if (access_token && now < token_expiry) return access_token;
  
  try {
    const response = await axios.post('https://osu.ppy.sh/oauth/token', {
      client_id, client_secret, grant_type: 'client_credentials', scope: 'public'
    });
    access_token = response.data.access_token;
    token_expiry = now + (response.data.expires_in * 1000) - 10000;
    log('INFO', '🔑 Obtained new osu! token');
    return access_token;
  } catch (err) {
    log('ERROR', '❌ Failed to get access token:', err.message);
    throw err;
  }
}

// Rate limiter for API calls
const limiter = new Bottleneck({ maxConcurrent: 3, minTime: 600 });

// ==================== DATABASE SCHEMA ====================
async function ensureTables() {
  try {
    // Core leaderboard table
    await query(`
      CREATE TABLE IF NOT EXISTS algeria_top50 (
        beatmap_id BIGINT,
        beatmap_title TEXT,
        artist TEXT,
        difficulty_name TEXT,
        player_id BIGINT,
        username TEXT,
        rank INTEGER,
        score BIGINT,
        accuracy REAL,
        accuracy_text TEXT,
        mods TEXT,
        pp REAL DEFAULT 0,
        difficulty_rating REAL DEFAULT 0,
        max_combo INTEGER DEFAULT 0,
        count_300 INTEGER DEFAULT 0,
        count_100 INTEGER DEFAULT 0,
        count_50 INTEGER DEFAULT 0,
        count_miss INTEGER DEFAULT 0,
        play_date BIGINT,
        last_updated BIGINT,
        PRIMARY KEY (beatmap_id, player_id)
      );
    `);

    // Enhanced player statistics
    await query(`
      CREATE TABLE IF NOT EXISTS player_stats (
        username TEXT PRIMARY KEY,
        user_id BIGINT UNIQUE,
        total_scores INTEGER DEFAULT 0,
        avg_rank REAL DEFAULT 0,
        best_score BIGINT DEFAULT 0,
        total_pp REAL DEFAULT 0,
        weighted_pp REAL DEFAULT 0,
        first_places INTEGER DEFAULT 0,
        top_10_places INTEGER DEFAULT 0,
        accuracy_avg REAL DEFAULT 0,
        playcount INTEGER DEFAULT 0,
        total_playtime INTEGER DEFAULT 0,
        level REAL DEFAULT 1,
        global_rank INTEGER DEFAULT 0,
        country_rank INTEGER DEFAULT 0,
        join_date BIGINT,
        last_seen BIGINT,
        last_calculated BIGINT DEFAULT 0,
        is_active BOOLEAN DEFAULT true,
        avatar_url TEXT,
        cover_url TEXT
      );
    `);

    // Skill tracking system
    await query(`
      CREATE TABLE IF NOT EXISTS skill_tracking (
        id SERIAL PRIMARY KEY,
        username TEXT,
        skill_type TEXT,
        skill_value REAL,
        confidence REAL DEFAULT 0.5,
        calculated_at BIGINT,
        FOREIGN KEY (username) REFERENCES player_stats(username) ON DELETE CASCADE
      );
    `);

    // Achievements system
    await query(`
      CREATE TABLE IF NOT EXISTS achievements (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE,
        description TEXT,
        category TEXT,
        icon TEXT,
        points INTEGER DEFAULT 0,
        created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000
      );
    `);

    await query(`
      CREATE TABLE IF NOT EXISTS player_achievements (
        id SERIAL PRIMARY KEY,
        username TEXT,
        achievement_id INTEGER,
        unlocked_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000,
        progress REAL DEFAULT 1.0,
        FOREIGN KEY (username) REFERENCES player_stats(username) ON DELETE CASCADE,
        FOREIGN KEY (achievement_id) REFERENCES achievements(id),
        UNIQUE(username, achievement_id)
      );
    `);

    // Activity tracking
    await query(`
      CREATE TABLE IF NOT EXISTS player_activity (
        id SERIAL PRIMARY KEY,
        username TEXT,
        activity_type TEXT,
        activity_data JSONB,
        timestamp BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000,
        FOREIGN KEY (username) REFERENCES player_stats(username) ON DELETE CASCADE
      );
    `);

    // Daily statistics
    await query(`
      CREATE TABLE IF NOT EXISTS daily_stats (
        date DATE PRIMARY KEY,
        active_players INTEGER DEFAULT 0,
        new_scores INTEGER DEFAULT 0,
        new_players INTEGER DEFAULT 0,
        total_pp_gained REAL DEFAULT 0,
        average_accuracy REAL DEFAULT 0,
        top_score BIGINT DEFAULT 0
      );
    `);

    // Beatmap metadata
    await query(`
      CREATE TABLE IF NOT EXISTS beatmap_metadata (
        beatmap_id BIGINT PRIMARY KEY,
        beatmapset_id BIGINT,
        artist TEXT,
        title TEXT,
        version TEXT,
        creator TEXT,
        difficulty_rating REAL,
        cs REAL,
        ar REAL,
        od REAL,
        hp REAL,
        length INTEGER,
        bpm REAL,
        max_combo INTEGER,
        tags TEXT[],
        genre_id INTEGER,
        language_id INTEGER,
        play_count INTEGER DEFAULT 0,
        favorite_count INTEGER DEFAULT 0,
        ranked_date BIGINT,
        last_updated BIGINT
      );
    `);

    // Player discovery tables
    await query(`
      CREATE TABLE IF NOT EXISTS player_discovery_log (
        id SERIAL PRIMARY KEY,
        username TEXT,
        user_id BIGINT,
        discovery_method TEXT,
        discovery_timestamp BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000,
        is_new_player BOOLEAN DEFAULT false,
        player_data JSONB
      );
    `);

    // Social features
    await query(`
      CREATE TABLE IF NOT EXISTS player_relationships (
        id SERIAL PRIMARY KEY,
        follower_username TEXT,
        following_username TEXT,
        relationship_type TEXT DEFAULT 'follow',
        created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000,
        FOREIGN KEY (follower_username) REFERENCES player_stats(username) ON DELETE CASCADE,
        FOREIGN KEY (following_username) REFERENCES player_stats(username) ON DELETE CASCADE,
        UNIQUE(follower_username, following_username)
      );
    `);

    await query(`
      CREATE TABLE IF NOT EXISTS player_comments (
        id SERIAL PRIMARY KEY,
        target_username TEXT,
        commenter_username TEXT,
        comment_text TEXT,
        created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000,
        is_deleted BOOLEAN DEFAULT false,
        FOREIGN KEY (target_username) REFERENCES player_stats(username) ON DELETE CASCADE,
        FOREIGN KEY (commenter_username) REFERENCES player_stats(username) ON DELETE CASCADE
      );
    `);

    // Create indexes for performance
    const indexes = [
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_score ON algeria_top50(score DESC)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_rank ON algeria_top50(rank ASC)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_pp ON algeria_top50(pp DESC)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_updated ON algeria_top50(last_updated DESC)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_username ON algeria_top50(username)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_beatmap ON algeria_top50(beatmap_id)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_player_stats_pp ON player_stats(weighted_pp DESC)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_player_activity_time ON player_activity(timestamp DESC)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_skill_tracking_username ON skill_tracking(username)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_discovery_log_timestamp ON player_discovery_log(discovery_timestamp DESC)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_beatmap_metadata_difficulty ON beatmap_metadata(difficulty_rating)'
    ];

    for (const indexSql of indexes) {
      try {
        await query(indexSql);
      } catch (err) {
        if (!err.message.includes('already exists')) {
          log('WARN', 'Index creation failed:', err.message);
        }
      }
    }

    await insertDefaultAchievements();
    log('INFO', '✅ Database schema ensured');
  } catch (err) {
    log('ERROR', '❌ Database setup failed:', err.message);
    throw err;
  }
}

// Default achievements
async function insertDefaultAchievements() {
  const achievements = [
    { name: 'First Steps', description: 'Set your first score', category: 'milestone', icon: '🎯', points: 10 },
    { name: 'Century Club', description: 'Achieve 100 total scores', category: 'milestone', icon: '💯', points: 50 },
    { name: 'Perfectionist', description: 'Get your first SS rank', category: 'accuracy', icon: '✨', points: 100 },
    { name: 'Speed Demon', description: 'Get a score with DT mod', category: 'mods', icon: '⚡', points: 25 },
    { name: 'Precision Master', description: 'Get a score with HR mod', category: 'mods', icon: '🎯', points: 25 },
    { name: 'In the Dark', description: 'Get a score with HD mod', category: 'mods', icon: '🌑', points: 25 },
    { name: 'Top Player', description: 'Reach #1 on any beatmap', category: 'ranking', icon: '👑', points: 200 },
    { name: 'Consistency King', description: 'Set scores on 5 consecutive days', category: 'activity', icon: '📅', points: 75 },
    { name: 'PP Collector', description: 'Reach 1000 total PP', category: 'performance', icon: '💎', points: 150 },
    { name: 'Dedication', description: 'Play for 100 hours total', category: 'activity', icon: '⏰', points: 100 }
  ];

  for (const achievement of achievements) {
    await query(`
      INSERT INTO achievements (name, description, category, icon, points)
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (name) DO NOTHING
    `, [achievement.name, achievement.description, achievement.category, achievement.icon, achievement.points]);
  }
}

// ==================== PLAYER DISCOVERY SYSTEM ====================
class PlayerDiscoveryService {
  constructor() {
    this.discoveryMethods = [
      'country_rankings',
      'recent_activity', 
      'user_search',
      'multiplayer_matches'
    ];
  }

  // Method 1: Monitor Algeria country rankings
  async discoverFromCountryRankings() {
    try {
      const token = await getAccessToken();
      let cursor = null;
      let page = 1;
      const maxPages = 10;
      let totalFound = 0;

      while (page <= maxPages) {
        const params = {
          country: 'DZ',
          mode: 'osu',
          type: 'performance'
        };
        
        if (cursor) params.cursor_string = cursor;

        const response = await axios.get('https://osu.ppy.sh/api/v2/rankings/osu/performance', {
          headers: { Authorization: `Bearer ${token}` },
          params
        });

        const rankings = response.data.ranking || [];
        if (rankings.length === 0) break;

        for (const playerRanking of rankings) {
          const registered = await this.registerPlayer(playerRanking.user, 'country_rankings');
          if (registered) totalFound++;
        }

        cursor = response.data.cursor?.page;
        if (!cursor) break;
        
        page++;
        await new Promise(resolve => setTimeout(resolve, 1000));
      }

      log('INFO', `🇩🇿 Country rankings: found ${totalFound} players (${page-1} pages)`);
      return totalFound;
    } catch (err) {
      log('ERROR', '❌ Country rankings discovery failed:', err.message);
      return 0;
    }
  }

  // Method 2: Monitor recent scores from popular beatmaps
  async discoverFromRecentScores() {
    try {
      const popularBeatmaps = await getRows(`
        SELECT beatmap_id, COUNT(DISTINCT username) as player_count
        FROM algeria_top50 
        GROUP BY beatmap_id 
        ORDER BY player_count DESC 
        LIMIT 30
      `);

      const token = await getAccessToken();
      let totalFound = 0;

      for (const beatmap of popularBeatmaps) {
        try {
          const response = await axios.get(
            `https://osu.ppy.sh/api/v2/beatmaps/${beatmap.beatmap_id}/scores`,
            {
              headers: { Authorization: `Bearer ${token}` },
              params: { limit: 50 }
            }
          );

          const scores = response.data.scores || [];
          const algerianScores = scores.filter(score => 
            score.user?.country?.code === 'DZ'
          );

          for (const score of algerianScores) {
            const registered = await this.registerPlayer(score.user, 'recent_scores');
            if (registered) totalFound++;
          }

          await new Promise(resolve => setTimeout(resolve, 800));
        } catch (err) {
          log('WARN', `Failed to check beatmap ${beatmap.beatmap_id}:`, err.message);
        }
      }

      log('INFO', `📊 Recent scores: found ${totalFound} new players`);
      return totalFound;
    } catch (err) {
      log('ERROR', '❌ Recent scores discovery failed:', err.message);
      return 0;
    }
  }

  // Method 3: Search for Algerian players
  async discoverFromUserSearch() {
    try {
      const token = await getAccessToken();
      let totalFound = 0;
      
      const searchTerms = ['algeria', 'dz', 'algerie'];

      for (const term of searchTerms) {
        try {
          const response = await axios.get('https://osu.ppy.sh/api/v2/search', {
            headers: { Authorization: `Bearer ${token}` },
            params: {
              mode: 'user',
              query: term
            }
          });

          const users = response.data.user?.data || [];
          const algerianUsers = users.filter(user => 
            user.country?.code === 'DZ'
          );

          for (const user of algerianUsers) {
            const registered = await this.registerPlayer(user, 'user_search');
            if (registered) totalFound++;
          }

          await new Promise(resolve => setTimeout(resolve, 2000));
        } catch (err) {
          log('WARN', `Search term '${term}' failed:`, err.message);
        }
      }

      log('INFO', `🔍 User search: found ${totalFound} new players`);
      return totalFound;
    } catch (err) {
      log('ERROR', '❌ User search discovery failed:', err.message);
      return 0;
    }
  }

  // Core player registration
  async registerPlayer(userData, discoveryMethod = 'unknown') {
    try {
      if (!userData || !userData.id || userData.country?.code !== 'DZ') {
        return false;
      }

      // Check if player already exists
      const existingPlayer = await getRow(`
        SELECT username, last_seen FROM player_stats 
        WHERE user_id = $1 OR username ILIKE $2
      `, [userData.id, userData.username]);

      const now = Date.now();
      const isNewPlayer = !existingPlayer;

      if (isNewPlayer) {
        log('INFO', `🆕 New Algerian player: ${userData.username} (${discoveryMethod})`);
        
        // Send Discord notification
        if (process.env.DISCORD_WEBHOOK_URL) {
          await this.sendNewPlayerNotification(userData, discoveryMethod);
        }

        // Broadcast to clients
        broadcastToClients({
          type: 'new_player_discovered',
          player: {
            username: userData.username,
            userId: userData.id,
            discoveryMethod,
            timestamp: now
          }
        });
      }

      // Insert/update player
      await query(`
        INSERT INTO player_stats (
          username, user_id, join_date, last_seen, avatar_url, cover_url,
          global_rank, country_rank, level, playcount, total_playtime, is_active
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, true)
        ON CONFLICT (username) DO UPDATE SET
          user_id = COALESCE(EXCLUDED.user_id, player_stats.user_id),
          last_seen = EXCLUDED.last_seen,
          avatar_url = COALESCE(EXCLUDED.avatar_url, player_stats.avatar_url),
          cover_url = COALESCE(EXCLUDED.cover_url, player_stats.cover_url),
          global_rank = COALESCE(EXCLUDED.global_rank, player_stats.global_rank),
          country_rank = COALESCE(EXCLUDED.country_rank, player_stats.country_rank),
          level = COALESCE(EXCLUDED.level, player_stats.level),
          playcount = COALESCE(EXCLUDED.playcount, player_stats.playcount),
          total_playtime = COALESCE(EXCLUDED.total_playtime, player_stats.total_playtime),
          is_active = true
      `, [
        userData.username,
        userData.id,
        userData.join_date ? new Date(userData.join_date).getTime() : now,
        now,
        userData.avatar_url,
        userData.cover_url || userData.cover?.url,
        userData.statistics?.global_rank,
        userData.statistics?.country_rank,
        userData.statistics?.level?.current,
        userData.statistics?.play_count,
        userData.statistics?.play_time
      ]);

      // Log discovery
      await query(`
        INSERT INTO player_discovery_log (username, user_id, discovery_method, is_new_player, player_data)
        VALUES ($1, $2, $3, $4, $5)
      `, [
        userData.username,
        userData.id,
        discoveryMethod,
        isNewPlayer,
        JSON.stringify(userData)
      ]);

      // If new player, fetch their history
      if (isNewPlayer) {
        setTimeout(() => {
          this.fetchPlayerHistory(userData.username, userData.id);
        }, 5000);
      }

      return isNewPlayer;
    } catch (err) {
      log('ERROR', `❌ Failed to register ${userData?.username}:`, err.message);
      return false;
    }
  }

  // Fetch comprehensive player history
  async fetchPlayerHistory(username, userId) {
    try {
      log('INFO', `📥 Fetching history for ${username}`);
      
      const token = await getAccessToken();
      
      // Get best scores
      const bestResponse = await axios.get(
        `https://osu.ppy.sh/api/v2/users/${userId}/scores/best`,
        {
          headers: { Authorization: `Bearer ${token}` },
          params: { limit: 100, mode: 'osu' }
        }
      );

      const bestScores = bestResponse.data || [];
      
      // Process scores for leaderboard positions
      let processedScores = 0;
      for (const score of bestScores.slice(0, 50)) { // Limit to avoid rate limits
        try {
          await limiter.schedule(() => 
            this.checkScoreOnLeaderboard(score, username)
          );
          processedScores++;
        } catch (err) {
          log('WARN', `Failed to process score on beatmap ${score.beatmap.id}:`, err.message);
        }
      }

      await this.updatePlayerStats(username, bestScores);
      await checkAchievements(username);
      
      log('INFO', `✅ Completed history fetch for ${username} (${processedScores} scores)`);
      
    } catch (err) {
      log('ERROR', `❌ Failed to fetch history for ${username}:`, err.message);
    }
  }

  // Check if score appears on leaderboard
  async checkScoreOnLeaderboard(score, expectedUsername) {
    try {
      const token = await getAccessToken();
      
      const response = await axios.get(
        `https://osu.ppy.sh/api/v2/beatmaps/${score.beatmap.id}/scores`,
        {
          headers: { Authorization: `Bearer ${token}` },
          params: { limit: 50 }
        }
      );

      const leaderboardScores = response.data.scores || [];
      const algerianScores = leaderboardScores.filter(s => 
        s.user?.country?.code === 'DZ'
      );

      if (algerianScores.length > 0) {
        const beatmapTitle = `${score.beatmapset.artist} - ${score.beatmapset.title} [${score.beatmap.version}]`;
        await saveBeatmapScores(score.beatmap.id, beatmapTitle, algerianScores, score.beatmap);
      }

    } catch (err) {
      // Silently handle - this is expected for many beatmaps
    }
  }

  // Update player statistics
  async updatePlayerStats(username, scores) {
    try {
      if (scores.length === 0) return;

      const totalPP = scores.reduce((sum, s) => sum + (s.pp || 0), 0);
      const avgAccuracy = scores.reduce((sum, s) => sum + (s.accuracy || 0), 0) / scores.length;
      const bestScore = Math.max(...scores.map(s => s.score || 0));
      
      // Calculate weighted PP
      const weightedPP = scores.reduce((sum, score, index) => {
        return sum + (score.pp || 0) * Math.pow(0.95, index);
      }, 0);

      await query(`
        UPDATE player_stats SET
          total_pp = $2,
          weighted_pp = $3,
          accuracy_avg = $4,
          best_score = $5,
          playcount = $6,
          last_calculated = $7
        WHERE username = $1
      `, [username, totalPP, weightedPP, avgAccuracy, bestScore, scores.length, Date.now()]);

    } catch (err) {
      log('ERROR', `Failed to update stats for ${username}:`, err.message);
    }
  }

// Discord notification (disabled temporarily)
async sendNewPlayerNotification(userData, discoveryMethod) {
    return; // skip sending anything
}

// The code below is disabled for now
/*
try {
  const embed = {
    title: "New Algerian Player Discovered! 🇩🇿✨",
    description: `Welcome **${userData.username}** to the Algeria osu! community!`,
    fields: [
      { name: "Player", value: `[${userData.username}](https://osu.ppy.sh/users/${userData.id})`, inline: true },
      { name: "Discovery", value: discoveryMethod.replace('_', ' '), inline: true },
      { name: "Rank", value: `#${userData.statistics?.country_rank || 'N/A'} DZ`, inline: true }
    ],
    color: 0x00ff88,
    timestamp: new Date().toISOString(),
    thumbnail: { url: userData.avatar_url },
    footer: { text: "Algeria osu! Leaderboards" }
  };

  await axios.post(process.env.DISCORD_WEBHOOK_URL, { embeds: [embed] });
} catch (err) {
  log('ERROR', 'Discord notification failed:', err.message);
}
*/


  // Run complete discovery
  async runDiscovery() {
    log('INFO', '🔍 Starting player discovery...');
    
    const startTime = Date.now();
    const results = {
      countryRankings: await this.discoverFromCountryRankings(),
      recentScores: await this.discoverFromRecentScores(),
      userSearch: await this.discoverFromUserSearch(),
      multiplayerMatches: await this.discoverFromMultiplayerMatches()
    };

    const total = Object.values(results).reduce((sum, count) => sum + count, 0);
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    
    log('INFO', `✅ Discovery completed in ${duration}s - Found ${total} new players`);
    
    broadcastToClients({
      type: 'discovery_complete',
      results,
      total,
      duration,
      timestamp: Date.now()
    });

    return results;
  }

  // Method 4: Discover players from recent multiplayer matches
  async discoverFromMultiplayerMatches() {
    try {
      const token = await getAccessToken();
      let totalFound = 0;
      let matchIds = [];
      try {
        const res = await axios.get('https://osu.ppy.sh/api/v2/multiplayer/matches', {
          headers: { Authorization: `Bearer ${token}` },
          params: { limit: 30 }
        });
        if (Array.isArray(res.data.matches)) {
          matchIds = res.data.matches.map(m => m.id).slice(0, 30);
        }
      } catch (e) {
        log('DEBUG', 'Multiplayer match list endpoint failed, will attempt alternative scanning');
      }

      if (matchIds.length === 0) {
        const recentBeatmaps = await getRows(`
          SELECT beatmap_id FROM beatmap_metadata
          ORDER BY last_updated DESC
          LIMIT 20
        `);
        for (const bm of recentBeatmaps) {
          try {
            const res = await axios.get(`https://osu.ppy.sh/api/v2/beatmaps/${bm.beatmap_id}/multiplayer`, {
              headers: { Authorization: `Bearer ${token}` },
              params: { limit: 10 }
            });
            if (res.data.matches) {
              for (const m of res.data.matches) {
                if (m.id) matchIds.push(m.id);
              }
            }
            await new Promise(r => setTimeout(r, 400));
          } catch (err) {
            // ignore per-beatmap errors
          }
        }
      }

      for (const matchId of [...new Set(matchIds)].slice(0, 60)) {
        try {
          const res = await axios.get(`https://osu.ppy.sh/api/v2/multiplayer/matches/${matchId}`, {
            headers: { Authorization: `Bearer ${token}` }
          });
          const match = res.data;
          const participants = match?.matches ? (match.matches.flatMap(x => x.scores || [])) : (match?.scores || []);
          if (Array.isArray(participants)) {
            for (const p of participants) {
              const u = p.user || p;
              if (u && u.country?.code === 'DZ') {
                const registered = await this.registerPlayer(u, 'multiplayer_matches');
                if (registered) totalFound++;
              }
            }
          }
          await new Promise(r => setTimeout(r, 500));
        } catch (err) {
          log('DEBUG', `Multiplayer match ${matchId} fetch failed: ${err.message}`);
        }
      }

      log('INFO', `🎮 Multiplayer discovery: found ${totalFound} players`);
      return totalFound;
    } catch (err) {
      log('ERROR', 'Multiplayer discovery failed:', err.message);
      return 0;
    }
  }

   // Updated runDiscovery
  async runDiscovery() {
    const results = {
      countryRankings: await this.discoverFromCountryRankings(),
      recentScores: await this.discoverFromRecentScores(),
      userSearch: await this.discoverFromUserSearch(),
      multiplayerMatches: await this.discoverFromMultiplayerMatches()
    };
    broadcastToClients({ type: 'discovery_complete', results });
    return results;
  }
}

// Initialize player discovery
const playerDiscovery = new PlayerDiscoveryService();

// ==================== SKILL CALCULATION SYSTEM ====================
class SkillCalculator {
  static calculateAimSkill(scores) {
    const aimScores = scores.filter(s => s.mods?.includes('HR') || s.difficulty_rating > 5.0);
    if (aimScores.length === 0) return 0;
    
    const avgPP = aimScores.reduce((sum, s) => sum + (s.pp || 0), 0) / aimScores.length;
    const accuracyFactor = aimScores.reduce((sum, s) => sum + (s.accuracy || 0), 0) / aimScores.length;
    
    return Math.min(10, (avgPP / 100) * accuracyFactor * 1.2);
  }

  static calculateSpeedSkill(scores) {
    const speedScores = scores.filter(s => s.mods?.includes('DT') || s.difficulty_rating > 4.5);
    if (speedScores.length === 0) return 0;
    
    const dtScores = speedScores.filter(s => s.mods?.includes('DT')).length;
    const dtRatio = dtScores / scores.length;
    const avgPP = speedScores.reduce((sum, s) => sum + (s.pp || 0), 0) / speedScores.length;
    
    return Math.min(10, (avgPP / 80) * (1 + dtRatio));
  }

  static calculateAccuracySkill(scores) {
    if (scores.length === 0) return 0;
    
    const avgAccuracy = scores.reduce((sum, s) => sum + (s.accuracy || 0), 0) / scores.length;
    const highAccuracyScores = scores.filter(s => (s.accuracy || 0) > 0.98).length;
    const consistencyBonus = highAccuracyScores / scores.length;
    
    return Math.min(10, avgAccuracy * 10 * (1 + consistencyBonus));
  }

  static calculateReadingSkill(scores) {
    const readingScores = scores.filter(s => s.mods?.includes('HD') || s.mods?.includes('HR'));
    if (readingScores.length === 0) return Math.min(10, scores.length * 0.1);
    
    const hdScores = readingScores.filter(s => s.mods?.includes('HD')).length;
    const hrScores = readingScores.filter(s => s.mods?.includes('HR')).length;
    const modVariety = (hdScores + hrScores) / scores.length;
    
    return Math.min(10, 3 + (modVariety * 7));
  }

  static calculateConsistencySkill(scores) {
    if (scores.length < 5) return 0;
    
    const missRates = scores.map(s => (s.count_miss || 0) / Math.max(1, s.max_combo || 100));
    const avgMissRate = missRates.reduce((sum, rate) => sum + rate, 0) / missRates.length;
    const consistency = Math.max(0, 1 - avgMissRate * 2);
    
    return Math.min(10, consistency * 10);
  }
}

// ==================== ACHIEVEMENT SYSTEM ====================
async function checkAchievements(username) {
  try {
    const playerScores = await getRows(`
      SELECT * FROM algeria_top50 WHERE username = $1 ORDER BY last_updated DESC
    `, [username]);
    
    const playerStats = await getRow(`
      SELECT * FROM player_stats WHERE username = $1
    `, [username]);
    
    if (!playerStats) return;

    const achievementChecks = [
      { name: 'First Steps', condition: () => playerScores.length >= 1 },
      { name: 'Century Club', condition: () => playerScores.length >= 100 },
      { name: 'PP Collector', condition: () => (playerStats.total_pp || 0) >= 1000 },
      { name: 'Perfectionist', condition: () => playerScores.some(s => (s.accuracy || 0) >= 1.0) },
      { name: 'Speed Demon', condition: () => playerScores.some(s => s.mods?.includes('DT')) },
      { name: 'Precision Master', condition: () => playerScores.some(s => s.mods?.includes('HR')) },
      { name: 'In the Dark', condition: () => playerScores.some(s => s.mods?.includes('HD')) },
      { name: 'Top Player', condition: () => playerScores.some(s => s.rank === 1) },
      { name: 'Dedication', condition: () => (playerStats.total_playtime || 0) >= 360000 }
    ];

    for (const check of achievementChecks) {
      if (check.condition()) {
        const achievement = await getRow(`SELECT id FROM achievements WHERE name = $1`, [check.name]);
        if (achievement) {
          await query(`
            INSERT INTO player_achievements (username, achievement_id)
            VALUES ($1, $2) ON CONFLICT DO NOTHING
          `, [username, achievement.id]);
        }
      }
    }
  } catch (err) {
    log('ERROR', 'Achievement check failed:', err.message);
  }
}

// ==================== LEADERBOARD FETCHING ====================
async function fetchLeaderboard(beatmapId, beatmapTitle) {
  const maxRetries = 3;
  let attempt = 0;
  
  while (attempt < maxRetries) {
    try {
      const token = await getAccessToken();
      
      const [scoresRes, beatmapRes] = await Promise.all([
        axios.get(`https://osu.ppy.sh/api/v2/beatmaps/${beatmapId}/scores`, {
          headers: { Authorization: `Bearer ${token}` }
        }),
        axios.get(`https://osu.ppy.sh/api/v2/beatmaps/${beatmapId}`, {
          headers: { Authorization: `Bearer ${token}` }
        }).catch(() => ({ data: null }))
      ]);
      
      const scores = scoresRes.data.scores || [];
      const beatmapInfo = beatmapRes.data;
      const algerianScores = scores.filter(s => s.user?.country?.code === 'DZ');
      
      if (algerianScores.length > 0) {
        await saveBeatmapScores(beatmapId, beatmapTitle, algerianScores, beatmapInfo);
        
        broadcastToClients({
          type: 'new_scores',
          beatmapId,
          beatmapTitle,
          scoresCount: algerianScores.length,
          topScore: algerianScores[0]
        });
      }
      
      if (beatmapInfo) {
        await saveBeatmapMetadata(beatmapInfo);
      }
      
      return;
      
    } catch (err) {
      attempt++;
      if (attempt >= maxRetries) {
        log('WARN', `Failed to fetch ${beatmapId} after ${maxRetries} attempts:`, err.message);
        break;
      }
      
      const backoffTime = Math.min(1000 * Math.pow(2, attempt), 10000);
      await new Promise(resolve => setTimeout(resolve, backoffTime));
    }
  }
}

async function saveBeatmapScores(beatmapId, beatmapTitle, algerianScores, beatmapInfo) {
  const now = Date.now();
  const client = await pool.connect();
  
  try {
    await client.query('BEGIN');
    
    for (let i = 0; i < algerianScores.length; i++) {
      const s = algerianScores[i];
      const mods = s.mods?.length ? s.mods.join(',') : 'None';
      
      // Check for new #1 score
      const existingTop = await client.query(
        'SELECT username, rank FROM algeria_top50 WHERE beatmap_id = $1 ORDER BY rank ASC LIMIT 1',
        [beatmapId]
      );
      
      const isNewFirst = i === 0 && (!existingTop.rows[0] || existingTop.rows[0].username !== s.user.username);
      
      await client.query(`
        INSERT INTO algeria_top50
          (beatmap_id, beatmap_title, artist, difficulty_name, player_id, username, rank, score, 
           accuracy, accuracy_text, mods, pp, difficulty_rating, max_combo, count_300, count_100, 
           count_50, count_miss, play_date, last_updated)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
        ON CONFLICT (beatmap_id, player_id) DO UPDATE SET
          beatmap_title = EXCLUDED.beatmap_title,
          artist = EXCLUDED.artist,
          difficulty_name = EXCLUDED.difficulty_name,
          username = EXCLUDED.username,
          rank = EXCLUDED.rank,
          score = EXCLUDED.score,
          accuracy = EXCLUDED.accuracy,
          accuracy_text = EXCLUDED.accuracy_text,
          mods = EXCLUDED.mods,
          pp = EXCLUDED.pp,
          difficulty_rating = EXCLUDED.difficulty_rating,
          max_combo = EXCLUDED.max_combo,
          count_300 = EXCLUDED.count_300,
          count_100 = EXCLUDED.count_100,
          count_50 = EXCLUDED.count_50,
          count_miss = EXCLUDED.count_miss,
          play_date = EXCLUDED.play_date,
          last_updated = EXCLUDED.last_updated
      `, [
        beatmapId, beatmapTitle,
        beatmapInfo?.beatmapset?.artist || 'Unknown',
        beatmapInfo?.version || 'Unknown',
        s.user.id, s.user.username, i + 1, s.score,
        s.accuracy, `${(s.accuracy * 100).toFixed(2)}%`,
        mods, s.pp || 0, beatmapInfo?.difficulty_rating || 0,
        s.max_combo || 0, s.statistics?.count_300 || 0,
        s.statistics?.count_100 || 0, s.statistics?.count_50 || 0,
        s.statistics?.count_miss || 0,
        new Date(s.created_at).getTime(), now
      ]);
      
      if (isNewFirst) {
        await client.query(`
          INSERT INTO player_activity (username, activity_type, activity_data)
          VALUES ($1, 'new_first_place', $2)
        `, [s.user.username, JSON.stringify({
          beatmapId, beatmapTitle, score: s.score, pp: s.pp, mods
        })]);
        
        if (process.env.DISCORD_WEBHOOK_URL) {
          await sendDiscordNotification(s, beatmapTitle, beatmapId, 'new_first');
        }
      }
    }
    
    await client.query('COMMIT');
    
    // Update player stats for all affected players
    for (const score of algerianScores) {
      await updatePlayerStats(score.user.username);
      await checkAchievements(score.user.username);
    }
    
    await invalidateCache(`leaderboard:*`);
    await invalidateCache(`player:*`);
    
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}

async function saveBeatmapMetadata(beatmapInfo) {
  try {
    const beatmapset = beatmapInfo.beatmapset || {};
    
    await query(`
      INSERT INTO beatmap_metadata (
        beatmap_id, beatmapset_id, artist, title, version, creator,
        difficulty_rating, cs, ar, od, hp, length, bpm, max_combo,
        play_count, favorite_count, ranked_date, last_updated
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
      ON CONFLICT (beatmap_id) DO UPDATE SET
        artist = EXCLUDED.artist,
        title = EXCLUDED.title,
        version = EXCLUDED.version,
        creator = EXCLUDED.creator,
        difficulty_rating = EXCLUDED.difficulty_rating,
        cs = EXCLUDED.cs,
        ar = EXCLUDED.ar,
        od = EXCLUDED.od,
        hp = EXCLUDED.hp,
        length = EXCLUDED.length,
        bpm = EXCLUDED.bpm,
        max_combo = EXCLUDED.max_combo,
        play_count = EXCLUDED.play_count,
        favorite_count = EXCLUDED.favorite_count,
        last_updated = EXCLUDED.last_updated
    `, [
      beatmapInfo.id, beatmapset.id, beatmapset.artist, beatmapset.title,
      beatmapInfo.version, beatmapset.creator, beatmapInfo.difficulty_rating,
      beatmapInfo.cs, beatmapInfo.ar, beatmapInfo.accuracy, beatmapInfo.drain,
      beatmapInfo.total_length, beatmapInfo.bpm, beatmapInfo.max_combo,
      beatmapInfo.playcount, beatmapset.favourite_count,
      beatmapset.ranked_date ? new Date(beatmapset.ranked_date).getTime() : null,
      Date.now()
    ]);
  } catch (err) {
    log('ERROR', 'Beatmap metadata save failed:', err.message);
  }
}

async function updatePlayerStats(username) {
  try {
    const playerScores = await getRows(`
      SELECT * FROM algeria_top50 WHERE username = $1
    `, [username]);
    
    if (playerScores.length === 0) return;
    
    const now = Date.now();
    const totalScores = playerScores.length;
    const avgRank = playerScores.reduce((sum, s) => sum + s.rank, 0) / totalScores;
    const bestScore = Math.max(...playerScores.map(s => s.score));
    const totalPP = playerScores.reduce((sum, s) => sum + (s.pp || 0), 0);
    const firstPlaces = playerScores.filter(s => s.rank === 1).length;
    const top10Places = playerScores.filter(s => s.rank <= 10).length;
    const avgAccuracy = playerScores.reduce((sum, s) => sum + (s.accuracy || 0), 0) / totalScores;
    
    const sortedByPP = playerScores.sort((a, b) => (b.pp || 0) - (a.pp || 0));
    const weightedPP = sortedByPP.reduce((sum, score, index) => {
      return sum + (score.pp || 0) * Math.pow(0.95, index);
    }, 0);
    
    await query(`
      INSERT INTO player_stats (
        username, total_scores, avg_rank, best_score, total_pp, weighted_pp,
        first_places, top_10_places, accuracy_avg, last_calculated
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      ON CONFLICT (username) DO UPDATE SET
        total_scores = EXCLUDED.total_scores,
        avg_rank = EXCLUDED.avg_rank,
        best_score = EXCLUDED.best_score,
        total_pp = EXCLUDED.total_pp,
        weighted_pp = EXCLUDED.weighted_pp,
        first_places = EXCLUDED.first_places,
        top_10_places = EXCLUDED.top_10_places,
        accuracy_avg = EXCLUDED.accuracy_avg,
        last_calculated = EXCLUDED.last_calculated
    `, [username, totalScores, avgRank, bestScore, totalPP, weightedPP, 
        firstPlaces, top10Places, avgAccuracy, now]);
    
    await updatePlayerSkills(username, playerScores);
    
  } catch (err) {
    log('ERROR', 'Player stats update failed:', err.message);
  }
}

async function updatePlayerSkills(username, playerScores) {
  try {
    const skills = {
      aim: SkillCalculator.calculateAimSkill(playerScores),
      speed: SkillCalculator.calculateSpeedSkill(playerScores),
      accuracy: SkillCalculator.calculateAccuracySkill(playerScores),
      reading: SkillCalculator.calculateReadingSkill(playerScores),
      consistency: SkillCalculator.calculateConsistencySkill(playerScores)
    };
    
    const now = Date.now();
    
    for (const [skillType, skillValue] of Object.entries(skills)) {
      await query(`
        INSERT INTO skill_tracking (username, skill_type, skill_value, calculated_at)
        VALUES ($1, $2, $3, $4)
      `, [username, skillType, skillValue, now]);
    }
    
    // Keep only last 30 entries per skill type
    await query(`
      DELETE FROM skill_tracking 
      WHERE username = $1 AND id NOT IN (
        SELECT id FROM skill_tracking 
        WHERE username = $1 
        ORDER BY calculated_at DESC 
        LIMIT 150
      )
    `, [username]);
    
  } catch (err) {
    log('ERROR', 'Skill tracking update failed:', err.message);
  }
}

async function sendDiscordNotification(score, beatmapTitle, beatmapId, type = 'new_first') {
  if (!process.env.DISCORD_WEBHOOK_URL) return;
  
  try {
    const embed = {
      title: "New Algerian #1 Score! 🇩🇿👑",
      description: `**${score.username}** achieved rank #1!`,
      fields: [
        { name: "Beatmap", value: `[${beatmapTitle}](https://osu.ppy.sh/beatmaps/${beatmapId})`, inline: false },
        { name: "Score", value: Number(score.score).toLocaleString(), inline: true },
        { name: "Accuracy", value: (score.accuracy * 100).toFixed(2) + '%', inline: true },
        { name: "Mods", value: score.mods?.join(',') || 'None', inline: true },
        { name: "PP", value: score.pp ? score.pp.toFixed(0) + 'pp' : 'N/A', inline: true },
        { name: "Combo", value: score.max_combo ? score.max_combo + 'x' : 'N/A', inline: true },
        { name: "Misses", value: score.statistics?.count_miss || 0, inline: true }
      ],
      color: 0xff66aa,
      timestamp: new Date().toISOString(),
      thumbnail: { url: `https://a.ppy.sh/${score.user.id}` },
      footer: { text: "Algeria osu! Leaderboards" }
    };
    
    await axios.post(process.env.DISCORD_WEBHOOK_URL, { embeds: [embed] });
    log('INFO', `📢 Discord notification sent for ${score.username}'s score`);
  } catch (err) {
    log('ERROR', 'Discord notification failed:', err.message);
  }
}

// ==================== DAILY STATISTICS ====================
async function calculateDailyStats() {
  try {
    const today = new Date().toISOString().split('T')[0];
    const yesterday = Date.now() - (24 * 60 * 60 * 1000);
    
    const [activePlayersResult, newScoresResult, totalPPResult, avgAccuracyResult, topScoreResult] = await Promise.all([
      getRow(`
        SELECT COUNT(DISTINCT username) as count 
        FROM algeria_top50 
        WHERE last_updated > $1
      `, [yesterday]),
      
      getRow(`
        SELECT COUNT(*) as count 
        FROM algeria_top50 
        WHERE last_updated > $1
      `, [yesterday]),
      
      getRow(`
        SELECT SUM(pp) as total 
        FROM algeria_top50 
        WHERE last_updated > $1 AND pp > 0
      `, [yesterday]),
      
      getRow(`
  SELECT AVG(accuracy) as avg 
  FROM algeria_top50 
  WHERE last_updated > $1 AND accuracy::numeric > 0
`, [yesterday]),
      
      getRow(`
        SELECT MAX(score) as max 
        FROM algeria_top50 
        WHERE last_updated > $1
      `, [yesterday])
    ]);
    
    await query(`
      INSERT INTO daily_stats (date, active_players, new_scores, total_pp_gained, average_accuracy, top_score)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (date) DO UPDATE SET
        active_players = EXCLUDED.active_players,
        new_scores = EXCLUDED.new_scores,
        total_pp_gained = EXCLUDED.total_pp_gained,
        average_accuracy = EXCLUDED.average_accuracy,
        top_score = EXCLUDED.top_score
    `, [
      today,
      parseInt(activePlayersResult.count) || 0,
      parseInt(newScoresResult.count) || 0,
      parseFloat(totalPPResult.total) || 0,
      parseFloat(avgAccuracyResult.avg) || 0,
      parseInt(topScoreResult.max) || 0
    ]);
    
    log('INFO', `📊 Daily stats calculated for ${today}`);
  } catch (err) {
    log('ERROR', 'Daily stats calculation failed:', err.message);
  }
}

// ==================== MAIN UPDATE FUNCTION ====================
async function updateLeaderboards() {
  log('INFO', "🔄 Starting leaderboards update...");
  try {
    const beatmaps = await getAllBeatmaps();
    await saveProgress("total_beatmaps", beatmaps.length);
    
    // Priority: Update known beatmaps first
    const priorityBeatmaps = await getRows(`
      SELECT beatmap_id, MIN(beatmap_title) AS beatmap_title
      FROM algeria_top50
      GROUP BY beatmap_id
      ORDER BY MIN(last_updated) ASC
      LIMIT 100
    `);
    
    if (priorityBeatmaps.length > 0) {
      log('INFO', `⚡ Priority scanning ${priorityBeatmaps.length} known beatmaps`);
      for (const bm of priorityBeatmaps) {
        await limiter.schedule(() => fetchLeaderboard(bm.beatmap_id, bm.beatmap_title));
      }
    }
    
    // Regular scanning
    let startIndex = parseInt(await getProgress("last_index") || "0", 10);
    if (startIndex >= beatmaps.length) {
      startIndex = 0;
      await saveProgress("last_index", "0");
    }
    
    log('INFO', `📌 Regular scanning from index ${startIndex}/${beatmaps.length}`);
    const batchSize = 50; // Reduced for better stability
    
    for (let i = startIndex; i < Math.min(startIndex + batchSize, beatmaps.length); i++) {
      const bm = beatmaps[i];
      await limiter.schedule(() => fetchLeaderboard(bm.id, bm.title));
      await saveProgress("last_index", i + 1);
      
      if (i % 10 === 0) {
        broadcastToClients({
          type: 'scan_progress',
          progress: {
            current: i,
            total: beatmaps.length,
            percentage: ((i / beatmaps.length) * 100).toFixed(2)
          }
        });
      }
    }
    
    await calculateDailyStats();
    await invalidateCache('*');
    
    log('INFO', "✅ Leaderboard update completed");
    broadcastToClients({
      type: 'scan_complete',
      timestamp: Date.now()
    });
  } catch (err) {
    log('ERROR', '❌ Leaderboard update failed:', err.message);
    broadcastToClients({
      type: 'scan_error',
      error: err.message,
      timestamp: Date.now()
    });
  }
}

async function getAllBeatmaps() {
  let allBeatmaps = [];
  let page = 1;
  const maxPages = 30; // Reduced for faster scanning
  
  while (page <= maxPages) {
    try {
      const token = await getAccessToken();
      const res = await axios.get('https://osu.ppy.sh/api/v2/beatmapsets/search', {
        headers: { Authorization: `Bearer ${token}` },
        params: { mode: 'osu', nsfw: false, sort: 'ranked_desc', page, 's': 'ranked' }
      });
      
      const sets = res.data.beatmapsets || [];
      if (sets.length === 0) break;
      
      const beatmaps = sets.flatMap(set =>
        (set.beatmaps || [])
          .filter(bm => bm.difficulty_rating >= 2.0)
          .map(bm => ({ 
            id: bm.id, 
            title: `${set.artist} - ${set.title} [${bm.version}]`, 
            difficulty: bm.difficulty_rating 
          }))
      );
      
      allBeatmaps.push(...beatmaps);
      page++;
      await new Promise(resolve => setTimeout(resolve, 1000));
    } catch (err) {
      log('ERROR', `❌ Failed to fetch beatmap page ${page}:`, err.message);
      break;
    }
  }
  
  return allBeatmaps;
}

// ==================== MIDDLEWARE ====================
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// Error handler middleware
const asyncHandler = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

// Authentication middleware
const authenticateToken = (req, res, next) => {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];
  
  if (!token) return res.sendStatus(401);
  
  jwt.verify(token, process.env.JWT_SECRET, (err, user) => {
    if (err) return res.sendStatus(403);
    req.user = user;
    next();
  });
};

// Input validation helper
const validateInput = (rules) => {
  return (req, res, next) => {
    const errors = [];
    
    Object.entries(rules).forEach(([field, rule]) => {
      const value = req.params[field] || req.query[field] || req.body[field];
      
      if (rule.required && (!value || value.toString().trim() === '')) {
        errors.push(`${field} is required`);
      }
      
      if (value && rule.type === 'number' && isNaN(Number(value))) {
        errors.push(`${field} must be a number`);
      }
      
      if (value && rule.minLength && value.toString().length < rule.minLength) {
        errors.push(`${field} must be at least ${rule.minLength} characters`);
      }
      
      if (value && rule.maxLength && value.toString().length > rule.maxLength) {
        errors.push(`${field} must be at most ${rule.maxLength} characters`);
      }
    });
    
    if (errors.length > 0) {
      return res.status(400).json({ success: false, errors });
    }
    
    next();
  };
};

// ==================== API ENDPOINTS ====================

// Health check
app.get('/health', asyncHandler(async (req, res) => {
  try {
    await query('SELECT 1');
    await redisClient.ping();
    const token = await getAccessToken();
    
    res.json({
      status: 'healthy',
      timestamp: new Date().toISOString(),
      services: {
        database: 'connected',
        redis: 'connected',
        osuApi: token ? 'connected' : 'disconnected'
      }
    });
  } catch (error) {
    res.status(503).json({
      status: 'unhealthy',
      error: error.message
    });
  }
}));

// Enhanced leaderboards endpoint
app.get('/api/leaderboards', asyncHandler(async (req, res) => {
  const {
    limit = 100,
    offset = 0,
    sort = 'score',
    order = 'DESC',
    minDifficulty = 0,
    maxDifficulty = 15,
    mods,
    player,
    timeRange
  } = req.query;

  const allowedSort = [
    'score', 'rank', 'last_updated', 'pp',
    'difficulty_rating', 'accuracy'
  ];
  const sortColumn = allowedSort.includes(sort) ? sort : 'score';
  const sortOrder = order.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';

  let params = [];
  let whereClauses = [];

  // Filters
  if (minDifficulty) {
    params.push(parseFloat(minDifficulty));
    whereClauses.push(`difficulty_rating >= $${params.length}`);
  }
  if (maxDifficulty) {
    params.push(parseFloat(maxDifficulty));
    whereClauses.push(`difficulty_rating <= $${params.length}`);
  }
  if (mods) {
    params.push(mods);
    whereClauses.push(`mods = $${params.length}`);
  }
  if (player) {
    params.push(player);
    whereClauses.push(`username ILIKE $${params.length}`);
  }

  const whereClause = whereClauses.length ? `WHERE ${whereClauses.join(' AND ')}` : '';

  // Pagination parameters (sync paramCount with current length)
  let paramCount = params.length;
  params.push(parseInt(limit), parseInt(offset));

  const sql = `
    SELECT *,
           ROW_NUMBER() OVER (ORDER BY ${sortColumn} ${sortOrder}) as rank
    FROM player_stats
    ${whereClause}
    ORDER BY ${sortColumn} ${sortOrder}
    LIMIT $${++paramCount} OFFSET $${++paramCount}
  `;

  const data = await getRows(sql, params);

  res.json({
    success: true,
    data,
    meta: {
      sort: sortColumn,
      order: sortOrder,
      limit: parseInt(limit),
      offset: parseInt(offset),
      hasMore: data.length === parseInt(limit)
    }
  });
}));

// Enhanced player profile endpoint
app.get('/api/players/:username', 
  validateInput({
    username: { required: true, minLength: 2, maxLength: 15 }
  }),
  asyncHandler(async (req, res) => {
    const { username } = req.params;
    const cacheKey = getCacheKey('player', username);
    
    const data = await getCached(cacheKey, async () => {
      const [playerStats, recentScores, bestScores, skills, achievements, activity] = await Promise.all([
        getRow(`SELECT * FROM player_stats WHERE username ILIKE $1`, [`%${username}%`]),
        getRows(`
          SELECT * FROM algeria_top50 
          WHERE username ILIKE $1 
          ORDER BY last_updated DESC 
          LIMIT 10
        `, [`%${username}%`]),
        getRows(`
          SELECT * FROM algeria_top50 
          WHERE username ILIKE $1 
          ORDER BY pp DESC 
          LIMIT 10
        `, [`%${username}%`]),
        getRows(`
          SELECT skill_type, skill_value, calculated_at
          FROM skill_tracking 
          WHERE username ILIKE $1 
          ORDER BY calculated_at DESC
          LIMIT 25
        `, [`%${username}%`]),
        getRows(`
          SELECT a.name, a.description, a.icon, a.points, pa.unlocked_at
          FROM player_achievements pa
          JOIN achievements a ON pa.achievement_id = a.id
          WHERE pa.username ILIKE $1
          ORDER BY pa.unlocked_at DESC
        `, [`%${username}%`]),
        getRows(`
          SELECT activity_type, activity_data, timestamp
          FROM player_activity
          WHERE username ILIKE $1
          ORDER BY timestamp DESC
          LIMIT 10
        `, [`%${username}%`])
      ]);
      
      if (!playerStats) return null;
      
      // Calculate skill progression
      const skillProgression = {};
      skills.forEach(skill => {
        if (!skillProgression[skill.skill_type]) {
          skillProgression[skill.skill_type] = [];
        }
        skillProgression[skill.skill_type].push({
          value: parseFloat(skill.skill_value),
          timestamp: parseInt(skill.calculated_at)
        });
      });
      
      // Calculate rank among all players
      const rankResult = await getRow(`
        SELECT COUNT(*) + 1 as rank
        FROM player_stats
        WHERE weighted_pp > $1
      `, [playerStats.weighted_pp || 0]);
      
      return {
        ...playerStats,
        countryRank: parseInt(rankResult.rank),
        recentScores,
        bestScores,
        skillProgression,
        achievements,
        recentActivity: activity
      };
    }, 300);
    
    if (!data) {
      return res.status(404).json({ success: false, error: 'Player not found' });
    }
    
    res.json({ success: true, data });
  })
);

// Player rankings endpoint
app.get('/api/rankings', asyncHandler(async (req, res) => {
  const { 
    sort = 'weighted_pp', 
    limit = 50, 
    offset = 0, 
    timeframe = 'all',
    minScores = 5 
  } = req.query;
  
  const cacheKey = getCacheKey('rankings', sort, limit, offset, timeframe, minScores);
  
  const data = await getCached(cacheKey, async () => {
    const allowedSort = ['weighted_pp', 'total_pp', 'first_places', 'avg_rank', 'accuracy_avg', 'total_scores'];
    const sortColumn = allowedSort.includes(sort.toLowerCase()) ? sort.toLowerCase() : 'weighted_pp';
    const sortOrder = sort === 'avg_rank' ? 'ASC' : 'DESC';
    
    let whereClause = `WHERE total_scores >= $1`;
    let params = [parseInt(minScores)];
    let paramCount = 1;
    
    if (timeframe !== 'all') {
      const timeRanges = {
        '24h': 24 * 60 * 60 * 1000,
        '7d': 7 * 24 * 60 * 60 * 1000,
        '30d': 30 * 24 * 60 * 60 * 1000
      };
      
      const cutoff = Date.now() - (timeRanges[timeframe] || 0);
      whereClause += ` AND last_calculated >= ${++paramCount}`;
      params.push(cutoff);
    }
    
    params.push(parseInt(limit), parseInt(offset));
    
    return await getRows(`
      SELECT *,
             ROW_NUMBER() OVER (ORDER BY ${sortColumn} ${sortOrder}) as rank
      FROM player_stats 
      ${whereClause}
      ORDER BY ${sortColumn} ${sortOrder}
      LIMIT $${++paramCount} OFFSET $${++paramCount}
    `, params);
  }, 300);
  
  res.json({
    success: true,
    data,
    meta: {
      sort,
      limit: parseInt(limit),
      offset: parseInt(offset),
      hasMore: data.length === parseInt(limit)
    }
  });
}));

// Player comparison endpoint
app.get('/api/compare/:username1/:username2', 
  validateInput({
    username1: { required: true, minLength: 2 },
    username2: { required: true, minLength: 2 }
  }),
  asyncHandler(async (req, res) => {
    const { username1, username2 } = req.params;
    const cacheKey = getCacheKey('compare', username1, username2);
    
    const data = await getCached(cacheKey, async () => {
      const [player1, player2] = await Promise.all([
        getRow(`SELECT * FROM player_stats WHERE username ILIKE $1`, [`%${username1}%`]),
        getRow(`SELECT * FROM player_stats WHERE username ILIKE $1`, [`%${username2}%`])
      ]);
      
      if (!player1 || !player2) return null;
      
      const [skills1, skills2] = await Promise.all([
        getRows(`
          SELECT skill_type, AVG(skill_value) as avg_skill
          FROM skill_tracking 
          WHERE username ILIKE $1
          GROUP BY skill_type
        `, [`%${username1}%`]),
        getRows(`
          SELECT skill_type, AVG(skill_value) as avg_skill
          FROM skill_tracking 
          WHERE username ILIKE $1
          GROUP BY skill_type
        `, [`%${username2}%`])
      ]);
      
      const skillComparison = {};
      ['aim', 'speed', 'accuracy', 'reading', 'consistency'].forEach(skill => {
        const skill1 = skills1.find(s => s.skill_type === skill);
        const skill2 = skills2.find(s => s.skill_type === skill);
        
        skillComparison[skill] = {
          player1: skill1 ? parseFloat(skill1.avg_skill) : 0,
          player2: skill2 ? parseFloat(skill2.avg_skill) : 0,
          difference: (skill1 ? parseFloat(skill1.avg_skill) : 0) - (skill2 ? parseFloat(skill2.avg_skill) : 0)
        };
      });
      
      return {
        player1,
        player2,
        skillComparison,
        statComparison: {
          totalPP: {
            player1: player1.total_pp || 0,
            player2: player2.total_pp || 0,
            difference: (player1.total_pp || 0) - (player2.total_pp || 0)
          },
          weightedPP: {
            player1: player1.weighted_pp || 0,
            player2: player2.weighted_pp || 0,
            difference: (player1.weighted_pp || 0) - (player2.weighted_pp || 0)
          },
          accuracy: {
            player1: player1.accuracy_avg || 0,
            player2: player2.accuracy_avg || 0,
            difference: (player1.accuracy_avg || 0) - (player2.accuracy_avg || 0)
          },
          firstPlaces: {
            player1: player1.first_places || 0,
            player2: player2.first_places || 0,
            difference: (player1.first_places || 0) - (player2.first_places || 0)
          }
        }
      };
    }, 600);
    
    if (!data) {
      return res.status(404).json({ success: false, error: 'One or both players not found' });
    }
    
    res.json({ success: true, data });
  })
);

// Search endpoint
app.get('/api/search', 
  validateInput({
    q: { required: true, minLength: 2, maxLength: 50 }
  }),
  asyncHandler(async (req, res) => {
    const { q, type = 'all', limit = 20 } = req.query;
    const searchTerm = q.trim();
    const cacheKey = getCacheKey('search', type, searchTerm, limit);
    
    const data = await getCached(cacheKey, async () => {
      const results = {};
      
      if (type === 'all' || type === 'players') {
        results.players = await getRows(`
          SELECT username, weighted_pp, first_places, avatar_url
          FROM player_stats
          WHERE username ILIKE $1
          ORDER BY weighted_pp DESC
          LIMIT $2
        `, [`%${searchTerm}%`, parseInt(limit)]);
      }
      
      if (type === 'all' || type === 'beatmaps') {
        results.beatmaps = await getRows(`
          SELECT DISTINCT 
            bm.beatmap_id, 
            bm.artist, 
            bm.title, 
            bm.version,
            bm.difficulty_rating,
            COUNT(ats.username) as algerian_players
          FROM beatmap_metadata bm
          LEFT JOIN algeria_top50 ats ON bm.beatmap_id = ats.beatmap_id
          WHERE bm.artist ILIKE $1 OR bm.title ILIKE $1 OR bm.version ILIKE $1
          GROUP BY bm.beatmap_id, bm.artist, bm.title, bm.version, bm.difficulty_rating
          ORDER BY algerian_players DESC
          LIMIT $2
        `, [`%${searchTerm}%`, parseInt(limit)]);
      }
      
      return results;
    }, 600);
    
    res.json({ success: true, query: searchTerm, data });
  })
);

// Analytics endpoints
app.get('/api/analytics/overview', asyncHandler(async (req, res) => {
  const cacheKey = 'analytics:overview';
  
  const data = await getCached(cacheKey, async () => {
    const [
      totalStats, recentActivity, topPerformers, 
      skillDistribution, modUsage, difficultyDistribution
    ] = await Promise.all([
      getRow(`
        SELECT 
          COUNT(DISTINCT username) as total_players,
          COUNT(*) as total_scores,
          COUNT(DISTINCT beatmap_id) as total_beatmaps,
          AVG(accuracy) as avg_accuracy,
          MAX(score) as highest_score,
          SUM(pp) as total_pp
        FROM algeria_top50
      `),
      getRow(`
        SELECT COUNT(*) as active_24h
        FROM algeria_top50
        WHERE last_updated > $1
      `, [Date.now() - (24 * 60 * 60 * 1000)]),
      getRows(`
        SELECT username, weighted_pp, first_places
        FROM player_stats
        ORDER BY weighted_pp DESC
        LIMIT 5
      `),
      getRows(`
        SELECT skill_type, AVG(skill_value) as avg_value
        FROM skill_tracking
        WHERE calculated_at > $1
        GROUP BY skill_type
      `, [Date.now() - (30 * 24 * 60 * 60 * 1000)]),
      getRows(`
        SELECT 
          mods, 
          COUNT(*) as usage_count,
          AVG(accuracy) as avg_accuracy,
          AVG(pp) as avg_pp
        FROM algeria_top50
        WHERE mods != 'None'
        GROUP BY mods
        ORDER BY usage_count DESC
        LIMIT 10
      `),
      getRows(`
        SELECT 
          FLOOR(difficulty_rating) as difficulty_range,
          COUNT(*) as score_count,
          AVG(accuracy) as avg_accuracy
        FROM algeria_top50
        GROUP BY FLOOR(difficulty_rating)
        ORDER BY difficulty_range ASC
      `)
    ]);
    
    return {
      totalStats: {
        ...totalStats,
        active24h: parseInt(recentActivity.active_24h)
      },
      topPerformers,
      skillDistribution,
      modUsage,
      difficultyDistribution
    };
  }, 900);
  
  res.json({ success: true, data });
}));

// Player Discovery API endpoints
app.post('/api/admin/discover-players', authenticateToken, asyncHandler(async (req, res) => {
  if (req.user.role !== 'admin') {
    return res.status(403).json({ success: false, error: 'Admin access required' });
  }

  // Run discovery in background
  playerDiscovery.runDiscovery().catch(err => {
    log('ERROR', '❌ Manual player discovery failed:', err.message);
  });

  res.json({ success: true, message: 'Player discovery started' });
}));

app.post('/api/webhook/player-register', 
  validateInput({
    username: { minLength: 2, maxLength: 15 },
    userId: { type: 'number' }
  }),
  asyncHandler(async (req, res) => {
    const { username, userId, source = 'webhook' } = req.body;
    
    if (!username && !userId) {
      return res.status(400).json({ success: false, error: 'Username or userId required' });
    }

    const token = await getAccessToken();
    const response = await axios.get(`https://osu.ppy.sh/api/v2/users/${userId || username}/osu`, {
      headers: { Authorization: `Bearer ${token}` }
    });

    const userData = response.data;
    
    if (userData.country?.code !== 'DZ') {
      return res.status(400).json({ 
        success: false, 
        error: 'Player is not from Algeria' 
      });
    }

    const registered = await playerDiscovery.registerPlayer(userData, source);
    
    if (registered) {
      res.json({ 
        success: true, 
        message: `Player ${userData.username} registered successfully`,
        player: {
          username: userData.username,
          userId: userData.id,
          countryRank: userData.statistics?.country_rank
        }
      });
    } else {
      res.status(500).json({ success: false, error: 'Registration failed' });
    }
  })
);

app.get('/api/discovery/stats', asyncHandler(async (req, res) => {
  const stats = await getRow(`
    SELECT 
      COUNT(*) as total_players,
      COUNT(CASE WHEN last_seen > $1 THEN 1 END) as active_players,
      COUNT(CASE WHEN join_date > $2 THEN 1 END) as new_this_week
    FROM player_stats
    WHERE is_active = true
  `, [
    Date.now() - (7 * 24 * 60 * 60 * 1000), 
    Date.now() - (7 * 24 * 60 * 60 * 1000)  
  ]);

  const discoveryMethods = await getRows(`
    SELECT 
      discovery_method as method,
      COUNT(*) as count
    FROM player_discovery_log
    WHERE discovery_timestamp > $1
    GROUP BY discovery_method
    ORDER BY count DESC
  `, [Date.now() - (30 * 24 * 60 * 60 * 1000)]);

  res.json({
    success: true,
    data: {
      ...stats,
      discoveryMethods
    }
  });
}));

// Admin endpoints
app.post('/api/admin/force-scan', authenticateToken, asyncHandler(async (req, res) => {
  if (req.user.role !== 'admin') {
    return res.status(403).json({ success: false, error: 'Admin access required' });
  }
  
  log('INFO', '🔧 Manual scan triggered via API');
  updateLeaderboards().catch(err => {
    log('ERROR', '❌ Manual scan failed:', err.message);
  });
  
  res.json({ success: true, message: 'Manual scan started' });
}));

// Error handling middleware
app.use((err, req, res, next) => {
  log('ERROR', 'Unhandled error:', err);
  
  if (err.name === 'ValidationError') {
    return res.status(400).json({ success: false, error: 'Validation failed', details: err.message });
  }
  
  if (err.code === 'ECONNREFUSED') {
    return res.status(503).json({ success: false, error: 'Database connection failed' });
  }
  
  res.status(500).json({ 
    success: false, 
    error: process.env.NODE_ENV === 'production' ? 'Internal server error' : err.message 
  });
});

// 404 handler
app.use('*', (req, res) => {
  res.status(404).json({ success: false, error: 'Endpoint not found' });
});

// ==================== WEBSOCKET SERVER ====================
const server = createServer(app);
const wss = new WebSocket.Server({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  log('INFO', '🔌 New WebSocket connection from', req.connection.remoteAddress);
  ws.send(JSON.stringify({ type: 'connected', timestamp: Date.now() }));
  
  ws.on('close', () => {
    log('DEBUG', '🔌 WebSocket connection closed');
  });
  
  ws.on('error', (error) => {
    log('ERROR', '🔌 WebSocket error:', error.message);
  });
});

// Broadcast helper
function broadcastToClients(data) {
  const message = JSON.stringify(data);
  let sentCount = 0;
  
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      try {
        client.send(message);
        sentCount++;
      } catch (err) {
        log('ERROR', 'Failed to send WebSocket message:', err.message);
      }
    }
  });
  
  if (sentCount > 0) {
    log('DEBUG', `📡 Broadcasted to ${sentCount} clients: ${data.type}`);
  }
}

// Make broadcast function globally available
global.broadcastToClients = broadcastToClients;

// ==================== SCHEDULED TASKS ====================
function schedulePlayerDiscovery() {
  // Quick discovery every 30 minutes (country rankings only)
  setInterval(async () => {
    try {
      await playerDiscovery.discoverFromCountryRankings();
    } catch (err) {
      log('ERROR', 'Scheduled country rankings discovery failed:', err.message);
    }
  }, 30 * 60 * 1000);

  // Comprehensive discovery every 4 hours
  setInterval(async () => {
    try {
      await playerDiscovery.runDiscovery();
    } catch (err) {
      log('ERROR', 'Scheduled comprehensive discovery failed:', err.message);
    }
  }, 4 * 60 * 60 * 1000);

  // Leaderboard update every 2 hours
  setInterval(async () => {
    try {
      await updateLeaderboards();
    } catch (err) {
      log('ERROR', 'Scheduled leaderboard update failed:', err.message);
    }
  }, 2 * 60 * 60 * 1000);

  // Daily statistics calculation
  setInterval(async () => {
    try {
      await calculateDailyStats();
    } catch (err) {
      log('ERROR', 'Daily stats calculation failed:', err.message);
    }
  }, 24 * 60 * 60 * 1000);

  // Cleanup old skill tracking data (weekly)
  setInterval(async () => {
    try {
      const cutoff = Date.now() - (90 * 24 * 60 * 60 * 1000); // 90 days
      const result = await query(`
        DELETE FROM skill_tracking 
        WHERE calculated_at < $1
      `, [cutoff]);
      
      if (result.rowCount > 0) {
        log('INFO', `🧹 Cleaned up ${result.rowCount} old skill tracking entries`);
      }
    } catch (err) {
      log('ERROR', 'Skill tracking cleanup failed:', err.message);
    }
  }, 7 * 24 * 60 * 60 * 1000);

  log('INFO', '📅 Scheduled tasks initialized');
}

// ==================== INITIALIZATION ====================
async function initializeServer() {
  try {
    // Ensure database schema
    await ensureTables();
    
    // Schedule background tasks
    schedulePlayerDiscovery();
    
    // Run initial player discovery after startup delay
    setTimeout(async () => {
      log('INFO', '🚀 Running initial player discovery...');
      try {
        await playerDiscovery.runDiscovery();
      } catch (err) {
        log('ERROR', '❌ Initial player discovery failed:', err.message);
      }
    }, 30000); // 30 second delay
    
    // Run initial leaderboard update after longer delay
    setTimeout(async () => {
      log('INFO', '🚀 Running initial leaderboard update...');
      try {
        await updateLeaderboards();
      } catch (err) {
        log('ERROR', '❌ Initial leaderboard update failed:', err.message);
      }
    }, 60000); // 1 minute delay
    
    log('INFO', '✅ Server initialization completed');
  } catch (err) {
    log('ERROR', '❌ Server initialization failed:', err.message);
    throw err;
  }
}

// ==================== GRACEFUL SHUTDOWN ====================
process.on('SIGINT', async () => {
  log('INFO', '🛑 Shutting down gracefully...');
  
  // Close WebSocket server
  wss.close(() => {
    log('INFO', '🔌 WebSocket server closed');
  });
  
  // Close HTTP server
  server.close(() => {
    log('INFO', '🌐 HTTP server closed');
  });
  
  // Close database connections
  try {
    await pool.end();
    log('INFO', '🗄️ Database pool closed');
  } catch (err) {
    log('ERROR', 'Database pool close error:', err.message);
  }
  
  // Close Redis connection
  try {
    await redisClient.quit();
    log('INFO', '🔴 Redis connection closed');
  } catch (err) {
    log('ERROR', 'Redis close error:', err.message);
  }
  
  log('INFO', '✅ Graceful shutdown completed');
  process.exit(0);
});

process.on('uncaughtException', (err) => {
  log('ERROR', '💥 Uncaught exception:', err);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  log('ERROR', '💥 Unhandled rejection at:', promise, 'reason:', reason);
  process.exit(1);
});

// ==================== START SERVER ====================
server.listen(port, async () => {
  log('INFO', `✅ Algeria osu! server running on port ${port}`);
  log('INFO', `🔌 WebSocket server: ws://localhost:${port}/ws`);
  log('INFO', `🌐 API endpoints: http://localhost:${port}/api/`);
  log('INFO', `📊 Health check: http://localhost:${port}/health`);
  
  // Initialize server components
  await initializeServer();
});

// Export for testing
module.exports = { app, server, playerDiscovery };

// ---------- BACKGROUND SCHEDULER ----------
const DISCOVERY_INTERVAL_MS = parseInt(process.env.DISCOVERY_INTERVAL_MS || `${4 * 60 * 60 * 1000}`);
const RANKINGS_INTERVAL_MS = parseInt(process.env.RANKINGS_INTERVAL_MS || `${30 * 60 * 1000}`);
const LEADERBOARD_INTERVAL_MS = parseInt(process.env.LEADERBOARD_INTERVAL_MS || `${2 * 60 * 60 * 1000}`);
const DAILY_JOB_HOUR_UTC = parseInt(process.env.DAILY_JOB_HOUR_UTC || '0');

const RUN_BACKGROUND_JOBS = (process.env.RUN_BG_JOBS || 'true') === 'true';

if (RUN_BACKGROUND_JOBS) {
  setInterval(async () => {
    try {
      log('INFO', 'Scheduled: country rankings check');
      await playerDiscovery.discoverFromCountryRankings();
    } catch (e) {
      log('ERROR', 'Scheduled country rankings check failed:', e.message);
    }
  }, RANKINGS_INTERVAL_MS);

  setInterval(async () => {
    try {
      log('INFO', 'Scheduled: full discovery run');
      await playerDiscovery.runDiscovery();
    } catch (e) {
      log('ERROR', 'Scheduled discovery failed:', e.message);
    }
  }, DISCOVERY_INTERVAL_MS);

  setInterval(async () => {
    try {
      log('INFO', 'Scheduled: leaderboard update');
      await updateLeaderboards();
    } catch (e) {
      log('ERROR', 'Scheduled leaderboard update failed:', e.message);
    }
  }, LEADERBOARD_INTERVAL_MS);

  (async function scheduleDaily() {
    try {
      const now = new Date();
      const next = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), DAILY_JOB_HOUR_UTC, 0, 0));
      if (next <= now) next.setUTCDate(next.getUTCDate() + 1);
      const delay = next - now;
      setTimeout(async function dailyRunner() {
        try {
          await calculateDailyStats();
        } catch (e) {
          log('ERROR', 'Daily stats job failed:', e.message);
        } finally {
          setTimeout(dailyRunner, 24 * 60 * 60 * 1000);
        }
      }, delay);
    } catch (err) {
      log('ERROR', 'Daily scheduler failed:', err.message);
    }
  })();
}
// ---------- END BACKGROUND SCHEDULER ----------

// Graceful shutdown
async function shutdown(code = 0) {
  try {
    log('INFO', 'Shutting down...');
    if (wss) {
      try { wss.clients.forEach(c => c.terminate()); } catch(e) {}
      try { wss.close(); } catch(e) {}
    }
    if (server) {
      try { server.close(); } catch(e) {}
    }
    try { await redisClient.quit(); } catch (e) {}
    try { await pool.end(); } catch (e) {}
    log('INFO', 'Shutdown complete');
    process.exit(code);
  } catch (err) {
    log('ERROR', 'Shutdown error:', err.message);
    process.exit(1);
  }
}

process.on('SIGTERM', () => shutdown(0));