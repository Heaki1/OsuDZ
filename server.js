require('dotenv').config();
const express = require('express');
const axios = require('axios');
const cors = require('cors');
const path = require('path');
const Bottleneck = require('bottleneck');
const { Pool } = require('pg');
const redis = require('redis');
const WebSocket = require('ws');
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');
const rateLimit = require('express-rate-limit');
const helmet = require('helmet');

const app = express();
const port = process.env.PORT || 3000;

// Enhanced security
app.use(helmet());

// Rate limiting
const httpLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100,
  message: 'Too many requests from this IP'
});
app.use(httpLimiter);

// Enhanced logging with colors and levels
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

// Redis setup for caching
const redisClient = redis.createClient({
  url: process.env.REDIS_URL
});
redisClient.on('error', (err) => log('ERROR', 'Redis error:', err));

// WebSocket server for real-time updates
const wss = new WebSocket.Server({ port: process.env.WS_PORT || 8080 });
const broadcastToClients = (data) => {
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(data));
    }
  });
};

// Enhanced database setup with connection pooling
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 30, idleTimeoutMillis: 30000, connectionTimeoutMillis: 2000,
});

async function query(sql, params = []) { return pool.query(sql, params); }
async function getRows(sql, params = []) { return (await pool.query(sql, params)).rows; }
async function getRow(sql, params = []) { return (await pool.query(sql, params)).rows[0]; }

// Comprehensive database schema
async function ensureTables() {
  // Core leaderboard table with enhanced fields
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
      skill_type TEXT, -- 'aim', 'speed', 'accuracy', 'reading', 'consistency'
      skill_value REAL,
      confidence REAL DEFAULT 0.5,
      calculated_at BIGINT,
      FOREIGN KEY (username) REFERENCES player_stats(username)
    );
  `);

  // Player achievements system
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
      FOREIGN KEY (username) REFERENCES player_stats(username),
      FOREIGN KEY (achievement_id) REFERENCES achievements(id),
      UNIQUE(username, achievement_id)
    );
  `);

  // Seasonal tracking
  await query(`
    CREATE TABLE IF NOT EXISTS seasons (
      id SERIAL PRIMARY KEY,
      name TEXT,
      start_date BIGINT,
      end_date BIGINT,
      is_active BOOLEAN DEFAULT false
    );
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS seasonal_stats (
      id SERIAL PRIMARY KEY,
      season_id INTEGER,
      username TEXT,
      total_pp REAL DEFAULT 0,
      rank_position INTEGER,
      scores_count INTEGER DEFAULT 0,
      improvement_rate REAL DEFAULT 0,
      FOREIGN KEY (season_id) REFERENCES seasons(id),
      FOREIGN KEY (username) REFERENCES player_stats(username)
    );
  `);

  // Enhanced analytics tables
  await query(`
    CREATE TABLE IF NOT EXISTS player_activity (
      id SERIAL PRIMARY KEY,
      username TEXT,
      activity_type TEXT, -- 'score_set', 'rank_improved', 'achievement_unlocked'
      activity_data JSONB,
      timestamp BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000,
      FOREIGN KEY (username) REFERENCES player_stats(username)
    );
  `);

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

  // Beatmap metadata and recommendations
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

  // Player preferences and social features
  await query(`
    CREATE TABLE IF NOT EXISTS player_preferences (
      username TEXT PRIMARY KEY,
      preferred_mods TEXT[],
      preferred_difficulty_range NUMRANGE,
      preferred_length_range NUMRANGE,
      notification_settings JSONB,
      privacy_settings JSONB,
      theme_preference TEXT DEFAULT 'dark',
      language_preference TEXT DEFAULT 'en',
      FOREIGN KEY (username) REFERENCES player_stats(username)
    );
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS player_relationships (
      id SERIAL PRIMARY KEY,
      follower_username TEXT,
      following_username TEXT,
      relationship_type TEXT DEFAULT 'follow', -- 'follow', 'friend', 'block'
      created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000,
      FOREIGN KEY (follower_username) REFERENCES player_stats(username),
      FOREIGN KEY (following_username) REFERENCES player_stats(username),
      UNIQUE(follower_username, following_username)
    );
  `);

  // Comments and social interaction
  await query(`
    CREATE TABLE IF NOT EXISTS player_comments (
      id SERIAL PRIMARY KEY,
      target_username TEXT,
      commenter_username TEXT,
      comment_text TEXT,
      created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000,
      is_deleted BOOLEAN DEFAULT false,
      FOREIGN KEY (target_username) REFERENCES player_stats(username),
      FOREIGN KEY (commenter_username) REFERENCES player_stats(username)
    );
  `);

  // Enhanced indexes for performance
  const indexes = [
    'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_score ON algeria_top50(score DESC)',
    'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_rank ON algeria_top50(rank ASC)',
    'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_pp ON algeria_top50(pp DESC)',
    'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_updated ON algeria_top50(last_updated DESC)',
    'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_username ON algeria_top50(username)',
    'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_beatmap ON algeria_top50(beatmap_id)',
    'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_mods ON algeria_top50(mods)',
    'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_player_stats_pp ON player_stats(total_pp DESC)',
    'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_player_stats_rank ON player_stats(country_rank ASC)',
    'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_player_activity_time ON player_activity(timestamp DESC)',
    'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_skill_tracking_username ON skill_tracking(username)',
    'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_achievements_category ON achievements(category)',
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

  // Insert default achievements
  await insertDefaultAchievements();
  
  log('INFO', '✅ Comprehensive database schema ensured');
}

// Default achievements system
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
    { name: 'Dedication', description: 'Play for 100 hours total', category: 'activity', icon: '⏰', points: 100 },
    { name: 'Improvement', description: 'Improve rank by 10 positions', category: 'progress', icon: '📈', points: 50 },
    { name: 'Community Member', description: 'Leave your first comment', category: 'social', icon: '💬', points: 20 }
  ];

  for (const achievement of achievements) {
    await query(`
      INSERT INTO achievements (name, description, category, icon, points)
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (name) DO NOTHING
    `, [achievement.name, achievement.description, achievement.category, achievement.icon, achievement.points]);
  }
}

// Caching utilities
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
    }
  } catch (err) {
    log('WARN', 'Cache invalidation failed:', err.message);
  }
}

// Enhanced API client
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

// Skill calculation algorithms
class SkillCalculator {
  static calculateAimSkill(scores) {
    // Calculate aim skill based on high AR, jump patterns, and cursor movement
    const aimScores = scores.filter(s => s.mods?.includes('HR') || s.difficulty_rating > 5.0);
    if (aimScores.length === 0) return 0;
    
    const avgPP = aimScores.reduce((sum, s) => sum + (s.pp || 0), 0) / aimScores.length;
    const accuracyFactor = aimScores.reduce((sum, s) => sum + (s.accuracy || 0), 0) / aimScores.length;
    
    return Math.min(10, (avgPP / 100) * accuracyFactor * 1.2);
  }

  static calculateSpeedSkill(scores) {
    // Calculate speed skill based on BPM, DT usage, and streaming capability
    const speedScores = scores.filter(s => s.mods?.includes('DT') || s.difficulty_rating > 4.5);
    if (speedScores.length === 0) return 0;
    
    const dtScores = speedScores.filter(s => s.mods?.includes('DT')).length;
    const dtRatio = dtScores / scores.length;
    const avgPP = speedScores.reduce((sum, s) => sum + (s.pp || 0), 0) / speedScores.length;
    
    return Math.min(10, (avgPP / 80) * (1 + dtRatio));
  }

  static calculateAccuracySkill(scores) {
    // Calculate accuracy skill based on consistent high accuracy
    if (scores.length === 0) return 0;
    
    const avgAccuracy = scores.reduce((sum, s) => sum + (s.accuracy || 0), 0) / scores.length;
    const highAccuracyScores = scores.filter(s => (s.accuracy || 0) > 0.98).length;
    const consistencyBonus = highAccuracyScores / scores.length;
    
    return Math.min(10, avgAccuracy * 10 * (1 + consistencyBonus));
  }

  static calculateReadingSkill(scores) {
    // Calculate reading skill based on AR, HD usage, and complex patterns
    const readingScores = scores.filter(s => s.mods?.includes('HD') || s.mods?.includes('HR'));
    if (readingScores.length === 0) return Math.min(10, scores.length * 0.1);
    
    const hdScores = readingScores.filter(s => s.mods?.includes('HD')).length;
    const hrScores = readingScores.filter(s => s.mods?.includes('HR')).length;
    const modVariety = (hdScores + hrScores) / scores.length;
    
    return Math.min(10, 3 + (modVariety * 7));
  }

  static calculateConsistencySkill(scores) {
    // Calculate consistency based on score distribution and miss count
    if (scores.length < 5) return 0;
    
    const missRates = scores.map(s => (s.count_miss || 0) / Math.max(1, s.max_combo || 100));
    const avgMissRate = missRates.reduce((sum, rate) => sum + rate, 0) / missRates.length;
    const consistency = Math.max(0, 1 - avgMissRate * 2);
    
    return Math.min(10, consistency * 10);
  }
}

// Achievement checking system
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
      // Milestone achievements
      { name: 'First Steps', condition: () => playerScores.length >= 1 },
      { name: 'Century Club', condition: () => playerScores.length >= 100 },
      { name: 'PP Collector', condition: () => (playerStats.total_pp || 0) >= 1000 },
      
      // Accuracy achievements
      { name: 'Perfectionist', condition: () => playerScores.some(s => (s.accuracy || 0) >= 1.0) },
      
      // Mod achievements
      { name: 'Speed Demon', condition: () => playerScores.some(s => s.mods?.includes('DT')) },
      { name: 'Precision Master', condition: () => playerScores.some(s => s.mods?.includes('HR')) },
      { name: 'In the Dark', condition: () => playerScores.some(s => s.mods?.includes('HD')) },
      
      // Ranking achievements
      { name: 'Top Player', condition: () => playerScores.some(s => s.rank === 1) },
      
      // Performance achievements
      { name: 'Dedication', condition: () => (playerStats.total_playtime || 0) >= 360000 } // 100 hours in seconds
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

// Enhanced leaderboard fetching with comprehensive data
async function fetchLeaderboard(beatmapId, beatmapTitle) {
  const maxRetries = 4;
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
        
        // Real-time update broadcast
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
      if (attempt >= maxRetries) break;
      
      const backoffTime = Math.min(1000 * Math.pow(2, attempt), 30000);
      await new Promise(resolve => setTimeout(resolve, backoffTime));
    }
  }
}

// Enhanced score saving with activity tracking
async function saveBeatmapScores(beatmapId, beatmapTitle, algerianScores, beatmapInfo) {
  const now = Date.now();
  const client = await pool.connect();
  
  try {
    await client.query('BEGIN');
    
    for (let i = 0; i < algerianScores.length; i++) {
      const s = algerianScores[i];
      const mods = s.mods?.length ? s.mods.join(',') : 'None';
      
      // Check if this is a new #1 score
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
      
      // Track activity for new #1 scores
      if (isNewFirst) {
        await client.query(`
          INSERT INTO player_activity (username, activity_type, activity_data)
          VALUES ($1, 'new_first_place', $2)
        `, [s.user.username, JSON.stringify({
          beatmapId, beatmapTitle, score: s.score, pp: s.pp, mods
        })]);
        
        // Send Discord notification
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
    
    // Invalidate relevant caches
    await invalidateCache(`leaderboard:*`);
    await invalidateCache(`player:*`);
    
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}

// Comprehensive player stats calculation
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
    
    // Calculate weighted PP (similar to osu!'s system)
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
    
    // Calculate and update skill ratings
    await updatePlayerSkills(username, playerScores);
    
  } catch (err) {
    log('ERROR', 'Player stats update failed:', err.message);
  }
}

// Skill tracking update
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
    
    // Keep only last 30 skill entries per player per skill type
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

// Beatmap metadata saving
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

// Enhanced Discord notifications
async function sendDiscordNotification(score, beatmapTitle, beatmapId, type = 'new_first') {
  if (!process.env.DISCORD_WEBHOOK_URL) return;
  
  try {
    let embed;
    
    if (type === 'new_first') {
      embed = {
        title: "New Algerian #1 Score! 🇩🇿👑",
        description: `**${score.username}** achieved rank #1!`,
        fields: [
          { name: "Beatmap", value: `[${beatmapTitle}](https://osu.ppy.sh/beatmaps/${beatmapId})`, inline: false },
          { name: "Score", value: Number(score.score).toLocaleString(), inline: true },
          { name: "Accuracy", value: (score.accuracy * 100).toFixed(2) + '%', inline: true },
          { name: "Mods", value: score.mods?.join(',') || 'None', inline: true },
          { name: "PP", value: score.pp ? score.pp.toFixed(0) + 'pp' : 'N/A', inline: true },
          { name: "Max Combo", value: score.max_combo ? score.max_combo + 'x' : 'N/A', inline: true },
          { name: "Misses", value: score.statistics?.count_miss || 0, inline: true }
        ],
        color: 0xff66aa,
        timestamp: new Date().toISOString(),
        thumbnail: { url: `https://a.ppy.sh/${score.user.id}` },
        footer: { text: "Algeria osu! Leaderboards" }
      };
    }
    
    await axios.post(process.env.DISCORD_WEBHOOK_URL, { embeds: [embed] });
    log('INFO', `📢 Discord notification sent for ${score.username}'s score`);
  } catch (err) {
    log('ERROR', 'Discord notification failed:', err.message);
  }
}

// Daily statistics calculation
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
        WHERE last_updated > $1 AND accuracy > 0
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

// Recommendation engine
class RecommendationEngine {
  static async getRecommendedBeatmaps(username, limit = 10) {
    try {
      // Get player's skill profile and preferences
      const playerStats = await getRow(`
        SELECT * FROM player_stats WHERE username = $1
      `, [username]);
      
      if (!playerStats) return [];
      
      const playerScores = await getRows(`
        SELECT * FROM algeria_top50 WHERE username = $1 ORDER BY pp DESC LIMIT 50
      `, [username]);
      
      const skills = await getRows(`
        SELECT skill_type, AVG(skill_value) as avg_skill 
        FROM skill_tracking 
        WHERE username = $1 
        GROUP BY skill_type
      `, [username]);
      
      const skillMap = {};
      skills.forEach(s => skillMap[s.skill_type] = parseFloat(s.avg_skill));
      
      // Get player's preferred difficulty range
      const avgDifficulty = playerScores.reduce((sum, s) => sum + (s.difficulty_rating || 0), 0) / playerScores.length;
      const difficultyRange = [Math.max(1, avgDifficulty - 1), avgDifficulty + 1.5];
      
      // Find beatmaps that match player's skill level but they haven't played
      const recommendations = await getRows(`
        SELECT bm.*, COUNT(ats.beatmap_id) as algerian_plays
        FROM beatmap_metadata bm
        LEFT JOIN algeria_top50 ats ON bm.beatmap_id = ats.beatmap_id
        WHERE bm.difficulty_rating BETWEEN $1 AND $2
        AND bm.beatmap_id NOT IN (
          SELECT beatmap_id FROM algeria_top50 WHERE username = $3
        )
        GROUP BY bm.beatmap_id
        ORDER BY algerian_plays DESC, bm.favorite_count DESC
        LIMIT $4
      `, [difficultyRange[0], difficultyRange[1], username, limit]);
      
      return recommendations;
    } catch (err) {
      log('ERROR', 'Recommendation generation failed:', err.message);
      return [];
    }
  }
}

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

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

// Enhanced API Endpoints

// Comprehensive leaderboard with advanced filtering
app.get('/api/leaderboards', async (req, res) => {
  try {
    const {
      limit = 100, offset = 0, sort = 'score', order = 'DESC',
      minDifficulty = 0, maxDifficulty = 15, mods, player, timeRange, skillLevel
    } = req.query;
    
    const cacheKey = `leaderboard:${JSON.stringify(req.query)}`;
    
    const data = await getCached(cacheKey, async () => {
      let whereClause = 'WHERE 1=1';
      let params = [];
      let paramCount = 0;
      
      if (minDifficulty > 0) {
        whereClause += ` AND difficulty_rating >= ${++paramCount}`;
        params.push(parseFloat(minDifficulty));
      }
      
      if (maxDifficulty < 15) {
        whereClause += ` AND difficulty_rating <= ${++paramCount}`;
        params.push(parseFloat(maxDifficulty));
      }
      
      if (mods && mods !== 'all') {
        whereClause += ` AND mods ILIKE ${++paramCount}`;
        params.push(`%${mods}%`);
      }
      
      if (player) {
        whereClause += ` AND username ILIKE ${++paramCount}`;
        params.push(`%${player}%`);
      }
      
      if (timeRange) {
        const timeRanges = {
          '24h': 24 * 60 * 60 * 1000,
          '7d': 7 * 24 * 60 * 60 * 1000,
          '30d': 30 * 24 * 60 * 60 * 1000
        };
        
        const cutoff = Date.now() - (timeRanges[timeRange] || timeRanges['30d']);
        whereClause += ` AND last_updated >= ${++paramCount}`;
        params.push(cutoff);
      }
      
      params.push(parseInt(limit), parseInt(offset));
      
      const allowedSort = ['score', 'rank', 'last_updated', 'pp', 'difficulty_rating', 'accuracy'];
      const sortColumn = allowedSort.includes(sort) ? sort : 'score';
      const sortOrder = ['ASC', 'DESC'].includes(order.toUpperCase()) ? order.toUpperCase() : 'DESC';
      
      return await getRows(
        `SELECT *, 
                RANK() OVER (ORDER BY ${sortColumn} ${sortOrder}) as global_rank
         FROM algeria_top50 
         ${whereClause}
         ORDER BY ${sortColumn} ${sortOrder} 
         LIMIT ${++paramCount} OFFSET ${++paramCount}`,
        params
      );
    }, 180); // 3 minute cache
    
    res.json({
      success: true,
      data,
      pagination: {
        limit: parseInt(limit),
        offset: parseInt(offset),
        hasMore: data.length === parseInt(limit)
      }
    });
  } catch (err) {
    log('ERROR', '❌ Leaderboard API error:', err.message);
    res.status(500).json({ success: false, error: 'Database error' });
  }
});

// Enhanced player profile with comprehensive stats
app.get('/api/players/:username', async (req, res) => {
  try {
    const { username } = req.params;
    const cacheKey = `player:${username.toLowerCase()}`;
    
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
          LIMIT 50
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
          LIMIT 20
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
    }, 300); // 5 minute cache
    
    if (!data) {
      return res.status(404).json({ success: false, error: 'Player not found' });
    }
    
    res.json({ success: true, data });
  } catch (err) {
    log('ERROR', '❌ Player profile API error:', err.message);
    res.status(500).json({ success: false, error: 'Database error' });
  }
});

// Player rankings with multiple sorting options
app.get('/api/rankings', async (req, res) => {
  try {
    const { 
      sort = 'weighted_pp', 
      limit = 50, 
      offset = 0, 
      timeframe = 'all',
      minScores = 5 
    } = req.query;
    
    const cacheKey = `rankings:${sort}:${limit}:${offset}:${timeframe}:${minScores}`;
    
    const data = await getCached(cacheKey, async () => {
      const allowedSort = ['weighted_pp', 'total_pp', 'first_places', 'avg_rank', 'accuracy_avg', 'total_scores'];
      const sortColumn = allowedSort.includes(sort) ? sort : 'weighted_pp';
      const sortOrder = sort === 'avg_rank' ? 'ASC' : 'DESC';
      
      let whereClause = `WHERE total_scores >= $1`;
      let params = [parseInt(minScores)];
      let paramCount = 1;
      
      // Add timeframe filtering if needed
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
      
      const rankings = await getRows(`
        SELECT *,
               ROW_NUMBER() OVER (ORDER BY ${sortColumn} ${sortOrder}) as rank
        FROM player_stats 
        ${whereClause}
        ORDER BY ${sortColumn} ${sortOrder}
        LIMIT ${++paramCount} OFFSET ${++paramCount}
      `, params);
      
      // Add rank change calculation
      for (const player of rankings) {
        const previousRank = await getRow(`
          SELECT ROW_NUMBER() OVER (ORDER BY ${sortColumn} ${sortOrder}) as old_rank
          FROM player_stats 
          WHERE username = $1
        `, [player.username]);
        
        player.rankChange = previousRank ? player.rank - previousRank.old_rank : 0;
      }
      
      return rankings;
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
  } catch (err) {
    log('ERROR', '❌ Rankings API error:', err.message);
    res.status(500).json({ success: false, error: 'Database error' });
  }
});

// Skill leaderboards
app.get('/api/skills/:skillType', async (req, res) => {
  try {
    const { skillType } = req.params;
    const { limit = 20 } = req.query;
    
    const allowedSkills = ['aim', 'speed', 'accuracy', 'reading', 'consistency'];
    if (!allowedSkills.includes(skillType)) {
      return res.status(400).json({ success: false, error: 'Invalid skill type' });
    }
    
    const cacheKey = `skills:${skillType}:${limit}`;
    
    const data = await getCached(cacheKey, async () => {
      return await getRows(`
        SELECT 
          st.username,
          AVG(st.skill_value) as avg_skill,
          MAX(st.skill_value) as peak_skill,
          COUNT(*) as data_points,
          ps.total_scores,
          ps.weighted_pp,
          ROW_NUMBER() OVER (ORDER BY AVG(st.skill_value) DESC) as rank
        FROM skill_tracking st
        JOIN player_stats ps ON st.username = ps.username
        WHERE st.skill_type = $1
        AND st.calculated_at > $2
        GROUP BY st.username, ps.total_scores, ps.weighted_pp
        HAVING COUNT(*) >= 3
        ORDER BY avg_skill DESC
        LIMIT $3
      `, [skillType, Date.now() - (30 * 24 * 60 * 60 * 1000), parseInt(limit)]);
    }, 600); // 10 minute cache
    
    res.json({ success: true, data, skillType });
  } catch (err) {
    log('ERROR', '❌ Skill leaderboard error:', err.message);
    res.status(500).json({ success: false, error: 'Database error' });
  }
});

// Beatmap-specific leaderboards
app.get('/api/beatmaps/:id/scores', async (req, res) => {
  try {
    const { id } = req.params;
    const cacheKey = `beatmap:${id}:scores`;
    
    const data = await getCached(cacheKey, async () => {
      const [scores, metadata] = await Promise.all([
        getRows(`
          SELECT * FROM algeria_top50
          WHERE beatmap_id = $1
          ORDER BY rank ASC
        `, [id]),
        getRow(`
          SELECT * FROM beatmap_metadata
          WHERE beatmap_id = $1
        `, [id])
      ]);
      
      return { scores, metadata };
    }, 300);
    
    if (data.scores.length === 0) {
      return res.status(404).json({ success: false, error: 'No Algerian scores found' });
    }
    
    res.json({ success: true, ...data });
  } catch (err) {
    log('ERROR', '❌ Beatmap scores error:', err.message);
    res.status(500).json({ success: false, error: 'Database error' });
  }
});

// Recent activity feed
app.get('/api/activity', async (req, res) => {
  try {
    const { limit = 50, type, username } = req.query;
    const cacheKey = `activity:${limit}:${type}:${username}`;
    
    const data = await getCached(cacheKey, async () => {
      let whereClause = '1=1';
      let params = [];
      let paramCount = 0;
      
      if (type) {
        whereClause += ` AND activity_type = ${++paramCount}`;
        params.push(type);
      }
      
      if (username) {
        whereClause += ` AND username ILIKE ${++paramCount}`;
        params.push(`%${username}%`);
      }
      
      params.push(parseInt(limit));
      
      return await getRows(`
        SELECT pa.*, ps.avatar_url
        FROM player_activity pa
        LEFT JOIN player_stats ps ON pa.username = ps.username
        WHERE ${whereClause}
        ORDER BY timestamp DESC
        LIMIT ${++paramCount}
      `, params);
    }, 120); // 2 minute cache
    
    res.json({ success: true, data });
  } catch (err) {
    log('ERROR', '❌ Activity feed error:', err.message);
    res.status(500).json({ success: false, error: 'Database error' });
  }
});

// Player comparison
app.get('/api/compare/:username1/:username2', async (req, res) => {
  try {
    const { username1, username2 } = req.params;
    const cacheKey = `compare:${username1.toLowerCase()}:${username2.toLowerCase()}`;
    
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
  } catch (err) {
    log('ERROR', '❌ Player comparison error:', err.message);
    res.status(500).json({ success: false, error: 'Database error' });
  }
});

// Recommendations endpoint
app.get('/api/recommendations/:username', async (req, res) => {
  try {
    const { username } = req.params;
    const { limit = 10 } = req.query;
    
    const cacheKey = `recommendations:${username.toLowerCase()}:${limit}`;
    
    const data = await getCached(cacheKey, async () => {
      return await RecommendationEngine.getRecommendedBeatmaps(username, parseInt(limit));
    }, 1800); // 30 minute cache
    
    res.json({ success: true, data });
  } catch (err) {
    log('ERROR', '❌ Recommendations error:', err.message);
    res.status(500).json({ success: false, error: 'Database error' });
  }
});

// Analytics endpoints
app.get('/api/analytics/overview', async (req, res) => {
  try {
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
    }, 900); // 15 minute cache
    
    res.json({ success: true, data });
  } catch (err) {
    log('ERROR', '❌ Analytics overview error:', err.message);
    res.status(500).json({ success: false, error: 'Database error' });
  }
});

// Daily statistics endpoint
app.get('/api/analytics/daily', async (req, res) => {
  try {
    const { days = 30 } = req.query;
    const cacheKey = `analytics:daily:${days}`;
    
    const data = await getCached(cacheKey, async () => {
      return await getRows(`
        SELECT * FROM daily_stats
        ORDER BY date DESC
        LIMIT $1
      `, [parseInt(days)]);
    }, 3600); // 1 hour cache
    
    res.json({ success: true, data });
  } catch (err) {
    log('ERROR', '❌ Daily analytics error:', err.message);
    res.status(500).json({ success: false, error: 'Database error' });
  }
});

// Achievement system endpoints
app.get('/api/achievements', async (req, res) => {
  try {
    const { category, limit = 50 } = req.query;
    const cacheKey = `achievements:${category}:${limit}`;
    
    const data = await getCached(cacheKey, async () => {
      let whereClause = '1=1';
      let params = [];
      let paramCount = 0;
      
      if (category) {
        whereClause += ` AND category = ${++paramCount}`;
        params.push(category);
      }
      
      params.push(parseInt(limit));
      
      const achievements = await getRows(`
        SELECT a.*, COUNT(pa.username) as unlock_count
        FROM achievements a
        LEFT JOIN player_achievements pa ON a.id = pa.achievement_id
        WHERE ${whereClause}
        GROUP BY a.id
        ORDER BY a.points DESC
        LIMIT ${++paramCount}
      `, params);
      
      return achievements;
    }, 1800); // 30 minute cache
    
    res.json({ success: true, data });
  } catch (err) {
    log('ERROR', '❌ Achievements error:', err.message);
    res.status(500).json({ success: false, error: 'Database error' });
  }
});

// Social features - Following system
app.post('/api/social/follow', authenticateToken, async (req, res) => {
  try {
    const { targetUsername } = req.body;
    const followerUsername = req.user.username;
    
    if (followerUsername === targetUsername) {
      return res.status(400).json({ success: false, error: 'Cannot follow yourself' });
    }
    
    // Check if target user exists
    const targetExists = await getRow(`
      SELECT username FROM player_stats WHERE username ILIKE $1
    `, [`%${targetUsername}%`]);
    
    if (!targetExists) {
      return res.status(404).json({ success: false, error: 'User not found' });
    }
    
    await query(`
      INSERT INTO player_relationships (follower_username, following_username, relationship_type)
      VALUES ($1, $2, 'follow')
      ON CONFLICT (follower_username, following_username) DO NOTHING
    `, [followerUsername, targetExists.username]);
    
    // Add activity
    await query(`
      INSERT INTO player_activity (username, activity_type, activity_data)
      VALUES ($1, 'followed_player', $2)
    `, [followerUsername, JSON.stringify({ targetUsername: targetExists.username })]);
    
    res.json({ success: true, message: 'Successfully followed user' });
  } catch (err) {
    log('ERROR', '❌ Follow error:', err.message);
    res.status(500).json({ success: false, error: 'Database error' });
  }
});

app.delete('/api/social/follow/:username', authenticateToken, async (req, res) => {
  try {
    const { username } = req.params;
    const followerUsername = req.user.username;
    
    const result = await query(`
      DELETE FROM player_relationships 
      WHERE follower_username = $1 AND following_username ILIKE $2
    `, [followerUsername, `%${username}%`]);
    
    if (result.rowCount === 0) {
      return res.status(404).json({ success: false, error: 'Not following this user' });
    }
    
    res.json({ success: true, message: 'Successfully unfollowed user' });
  } catch (err) {
    log('ERROR', '❌ Unfollow error:', err.message);
    res.status(500).json({ success: false, error: 'Database error' });
  }
});

// Comments system
app.post('/api/players/:username/comments', authenticateToken, async (req, res) => {
  try {
    const { username } = req.params;
    const { comment } = req.body;
    const commenterUsername = req.user.username;
    
    if (!comment || comment.trim().length === 0) {
      return res.status(400).json({ success: false, error: 'Comment cannot be empty' });
    }
    
    if (comment.length > 1000) {
      return res.status(400).json({ success: false, error: 'Comment too long' });
    }
    
    // Check if target user exists
    const targetExists = await getRow(`
      SELECT username FROM player_stats WHERE username ILIKE $1
    `, [`%${username}%`]);
    
    if (!targetExists) {
      return res.status(404).json({ success: false, error: 'User not found' });
    }
    
    await query(`
      INSERT INTO player_comments (target_username, commenter_username, comment_text)
      VALUES ($1, $2, $3)
    `, [targetExists.username, commenterUsername, comment.trim()]);
    
    // Add activity
    await query(`
      INSERT INTO player_activity (username, activity_type, activity_data)
      VALUES ($1, 'left_comment', $2)
    `, [commenterUsername, JSON.stringify({ 
      targetUsername: targetExists.username, 
      commentPreview: comment.trim().substring(0, 50) 
    })]);
    
    res.json({ success: true, message: 'Comment added successfully' });
  } catch (err) {
    log('ERROR', '❌ Comment error:', err.message);
    res.status(500).json({ success: false, error: 'Database error' });
  }
});

app.get('/api/players/:username/comments', async (req, res) => {
  try {
    const { username } = req.params;
    const { limit = 20, offset = 0 } = req.query;
    
    const cacheKey = `comments:${username.toLowerCase()}:${limit}:${offset}`;
    
    const data = await getCached(cacheKey, async () => {
      return await getRows(`
        SELECT 
          pc.*,
          ps.avatar_url as commenter_avatar
        FROM player_comments pc
        LEFT JOIN player_stats ps ON pc.commenter_username = ps.username
        WHERE pc.target_username ILIKE $1 AND pc.is_deleted = false
        ORDER BY pc.created_at DESC
        LIMIT $2 OFFSET $3
      `, [`%${username}%`, parseInt(limit), parseInt(offset)]);
    }, 300); // 5 minute cache
    
    res.json({ success: true, data });
  } catch (err) {
    log('ERROR', '❌ Comments fetch error:', err.message);
    res.status(500).json({ success: false, error: 'Database error' });
  }
});

// Search endpoints
app.get('/api/search', async (req, res) => {
  try {
    const { q, type = 'all', limit = 20 } = req.query;
    
    if (!q || q.trim().length < 2) {
      return res.status(400).json({ success: false, error: 'Search query too short' });
    }
    
    const searchTerm = q.trim();
    const cacheKey = `search:${type}:${searchTerm}:${limit}`;
    
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
    }, 600); // 10 minute cache
    
    res.json({ success: true, query: searchTerm, data });
  } catch (err) {
    log('ERROR', '❌ Search error:', err.message);
    res.status(500).json({ success: false, error: 'Database error' });
  }
});

// Seasonal tracking endpoints
app.get('/api/seasons', async (req, res) => {
  try {
    const seasons = await getRows(`
      SELECT * FROM seasons
      ORDER BY start_date DESC
    `);
    
    res.json({ success: true, data: seasons });
  } catch (err) {
    log('ERROR', '❌ Seasons error:', err.message);
    res.status(500).json({ success: false, error: 'Database error' });
  }
});

app.get('/api/seasons/:id/rankings', async (req, res) => {
  try {
    const { id } = req.params;
    const { limit = 50 } = req.query;
    
    const cacheKey = `season:${id}:rankings:${limit}`;
    
    const data = await getCached(cacheKey, async () => {
      return await getRows(`
        SELECT 
          ss.*,
          ps.username,
          ps.avatar_url,
          ROW_NUMBER() OVER (ORDER BY ss.total_pp DESC) as position
        FROM seasonal_stats ss
        JOIN player_stats ps ON ss.username = ps.username
        WHERE ss.season_id = $1
        ORDER BY ss.total_pp DESC
        LIMIT $2
      `, [parseInt(id), parseInt(limit)]);
    }, 900); // 15 minute cache
    
    res.json({ success: true, data });
  } catch (err) {
    log('ERROR', '❌ Seasonal rankings error:', err.message);
    res.status(500).json({ success: false, error: 'Database error' });
  }
});

// Export endpoints
app.get('/api/export/:type', async (req, res) => {
  try {
    const { type } = req.params;
    const { format = 'json', limit = 1000 } = req.query;
    
    let data;
    let filename;
    
    switch (type) {
      case 'leaderboard':
        data = await getRows(`
          SELECT * FROM algeria_top50 
          ORDER BY score DESC 
          LIMIT $1
        `, [parseInt(limit)]);
        filename = 'algeria_leaderboard';
        break;
        
      case 'players':
        data = await getRows(`
          SELECT * FROM player_stats 
          ORDER BY weighted_pp DESC 
          LIMIT $1
        `, [parseInt(limit)]);
        filename = 'algeria_players';
        break;
        
      default:
        return res.status(400).json({ success: false, error: 'Invalid export type' });
    }
    
    if (format === 'csv') {
      const headers = Object.keys(data[0] || {});
      const csvContent = [
        headers.join(','),
        ...data.map(row => headers.map(header => `"${row[header] || ''}"`).join(','))
      ].join('\n');
      
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', `attachment; filename=${filename}.csv`);
      res.send(csvContent);
    } else {
      res.json({ success: true, data, exported: data.length });
    }
  } catch (err) {
    log('ERROR', '❌ Export error:', err.message);
    res.status(500).json({ success: false, error: 'Export failed' });
  }
});

// Admin endpoints
app.post('/api/admin/force-scan', authenticateToken, async (req, res) => {
  try {
    if (req.user.role !== 'admin') {
      return res.status(403).json({ success: false, error: 'Admin access required' });
    }
    
    log('INFO', '🔧 Manual scan triggered via API');
    
    // Run scan in background
    updateLeaderboards().catch(err => {
      log('ERROR', '❌ Manual scan failed:', err.message);
    });
    
    res.json({ success: true, message: 'Manual scan started' });
  } catch (err) {
    log('ERROR', '❌ Force scan error:', err.message);
    res.status(500).json({ success: false, error: 'Failed to start scan' });
  }
});

app.post('/api/admin/recalculate-stats', authenticateToken, async (req, res) => {
  try {
    if (req.user.role !== 'admin') {
      return res.status(403).json({ success: false, error: 'Admin access required' });
    }
    
    const players = await getRows(`SELECT DISTINCT username FROM algeria_top50`);
    
    let processed = 0;
    for (const player of players) {
      await updatePlayerStats(player.username);
      await checkAchievements(player.username);
      processed++;
      
      if (processed % 10 === 0) {
        log('INFO', `📊 Recalculated stats for ${processed}/${players.length} players`);
      }
    }
    
    await invalidateCache('*');
    
    res.json({ 
      success: true, 
      message: `Recalculated stats for ${processed} players` 
    });
  } catch (err) {
    log('ERROR', '❌ Stats recalculation error:', err.message);
    res.status(500).json({ success: false, error: 'Recalculation failed' });
  }
});

// WebSocket handlers
wss.on('connection', (ws) => {
  log('INFO', '🔌 New WebSocket connection');
  
  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);
      
      if (data.type === 'subscribe') {
        ws.subscriptions = data.channels || ['all'];
        ws.send(JSON.stringify({ type: 'subscribed', channels: ws.subscriptions }));
      }
    } catch (err) {
      log('ERROR', 'WebSocket message error:', err.message);
    }
  });
  
  ws.on('close', () => {
    log('INFO', '🔌 WebSocket connection closed');
  });
});

// Enhanced update function with comprehensive features
async function updateLeaderboards() {
  log('INFO', "🔄 Starting comprehensive leaderboards update...");
  
  try {
    const beatmaps = await getAllBeatmaps();
    await saveProgress("total_beatmaps", beatmaps.length);

    // Priority scanning for known maps
    const priorityBeatmaps = await getRows(`
      SELECT beatmap_id, MIN(beatmap_title) AS beatmap_title
      FROM algeria_top50
      GROUP BY beatmap_id
      ORDER BY MIN(last_updated) ASC
      LIMIT 200
    `);
    
    if (priorityBeatmaps.length > 0) {
      log('INFO', `⚡ Priority scanning ${priorityBeatmaps.length} known beatmaps`);
      
      for (const bm of priorityBeatmaps) {
        await limiter.schedule(() => fetchLeaderboard(bm.beatmap_id, bm.beatmap_title));
      }
    }

    // Regular scanning with progress tracking
    let startIndex = parseInt(await getProgress("last_index") || "0", 10);
    if (startIndex >= beatmaps.length) {
      startIndex = 0;
      await saveProgress("last_index", "0");
    }

    log('INFO', `📌 Regular scanning from index ${startIndex}/${beatmaps.length}`);
    
    const batchSize = 100;
    for (let i = startIndex; i < Math.min(startIndex + batchSize, beatmaps.length); i++) {
      const bm = beatmaps[i];
      await limiter.schedule(() => fetchLeaderboard(bm.id, bm.title));
      await saveProgress("last_index", i + 1);
      
      // Progress broadcast
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
    
    // Calculate daily statistics
    await calculateDailyStats();
    
    // Invalidate relevant caches
    await invalidateCache('leaderboard:*');
    await invalidateCache('rankings:*');
    await invalidateCache('analytics:*');
    
    log('INFO', "✅ Comprehensive leaderboard update completed");
    
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
  const maxPages = 50;
  
  while (page <= maxPages) {
    try {
      const token = await getAccessToken();
      
      const res = await axios.get('https://osu.ppy.sh/api/v2/beatmapsets/search', {
        headers: { Authorization: `Bearer ${token}` },
        params: { 
          mode: 'osu', 
          nsfw: false, 
          sort: 'ranked_desc', 
          page,
          's': 'ranked'
        }
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

// Enhanced error handling and graceful shutdown
process.on('SIGINT', async () => {
  log('INFO', '🛑 Shutting down gracefully...');
  
  // Close WebSocket server
  wss.close();
  
  // Close database connections
  await pool.end();
  
  // Close Redis connection
  redisClient.quit();
  
  process.exit(0);
});

process.on('uncaughtException', (err) => {
  log('ERROR', '💥 Uncaught exception:', err);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  log('ERROR', '💥 Unhandled rejection at:', promise, 'reason:', reason);
});

// Rate limiter for API
const limiter = new Bottleneck({ maxConcurrent: 3, minTime: 600 });

// Start server
app.listen(port, async () => {
  log('INFO', `✅ Enhanced Algeria osu! server running on port ${port}`);
  log('INFO', `🔌 WebSocket server running on port ${process.env.WS_PORT || 8080}`);
  log('INFO', `📊 Admin dashboard: http://localhost:${port}/admin`);
  
  try {
    // Connect to Redis
    await redisClient.connect();
    log('INFO', '🔴 Redis connected');
    
    // Initialize database
    await ensureTables();
    
    // Initial update
    setTimeout(updateLeaderboards, 5000);
    
    // Schedule regular updates (every 30 minutes)
    setInterval(updateLeaderboards, 30 * 60 * 1000);
    
    // Daily stats calculation (every hour)
    setInterval(calculateDailyStats, 60 * 60 * 1000);
    
    log('INFO', '🚀 All systems operational - Enhanced backend ready!');
  } catch (err) {
    log('ERROR', '❌ Startup failed:', err.message);
  }
});
          