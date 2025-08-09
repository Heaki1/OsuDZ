// Enhanced server.js with advanced features, caching, monitoring, and new endpoints
require('dotenv').config();
const express = require('express');
const axios = require('axios');
const cors = require('cors');
const path = require('path');
const Bottleneck = require('bottleneck');
const { Pool } = require('pg');

const app = express();
const port = process.env.PORT || 3000;

// Enhanced logging with colors and levels
const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m'
};

function log(level, ...args) {
  const ts = new Date().toISOString().replace('T', ' ').split('.')[0];
  const levelColors = {
    INFO: colors.green,
    WARN: colors.yellow,
    ERROR: colors.red,
    DEBUG: colors.blue
  };
  const color = levelColors[level] || colors.reset;
  console.log(`${color}[${ts}] ${level}:${colors.reset}`, ...args);
}

// Enhanced database setup with connection pooling
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

async function query(sql, params = []) { return pool.query(sql, params); }
async function getRows(sql, params = []) { return (await pool.query(sql, params)).rows; }
async function getRow(sql, params = []) { return (await pool.query(sql, params)).rows[0]; }

// Enhanced table creation with indexes and additional tables
async function ensureTables() {
  // Main leaderboard table
  await query(`
    CREATE TABLE IF NOT EXISTS algeria_top50 (
      beatmap_id BIGINT,
      beatmap_title TEXT,
      player_id BIGINT,
      username TEXT,
      rank INTEGER,
      score BIGINT,
      accuracy TEXT,
      mods TEXT,
      pp REAL DEFAULT 0,
      difficulty_rating REAL DEFAULT 0,
      last_updated BIGINT,
      PRIMARY KEY (beatmap_id, player_id)
    );
  `);

  // Player statistics table
  await query(`
    CREATE TABLE IF NOT EXISTS player_stats (
      username TEXT PRIMARY KEY,
      total_scores INTEGER DEFAULT 0,
      avg_rank REAL DEFAULT 0,
      best_score BIGINT DEFAULT 0,
      first_places INTEGER DEFAULT 0,
      last_calculated BIGINT DEFAULT 0
    );
  `);

  // Scan metrics table
  await query(`
    CREATE TABLE IF NOT EXISTS scan_metrics (
      id SERIAL PRIMARY KEY,
      scan_start BIGINT,
      scan_end BIGINT,
      beatmaps_scanned INTEGER,
      algerian_scores_found INTEGER,
      api_errors INTEGER,
      created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000
    );
  `);

  // Progress table
  await query(`
    CREATE TABLE IF NOT EXISTS progress (
      key TEXT PRIMARY KEY,
      value TEXT
    );
  `);

  // Create indexes for better performance
  await query(`
    CREATE INDEX IF NOT EXISTS idx_algeria_score ON algeria_top50(score DESC);
    CREATE INDEX IF NOT EXISTS idx_algeria_rank ON algeria_top50(rank ASC);
    CREATE INDEX IF NOT EXISTS idx_algeria_updated ON algeria_top50(last_updated DESC);
    CREATE INDEX IF NOT EXISTS idx_algeria_username ON algeria_top50(username);
    CREATE INDEX IF NOT EXISTS idx_algeria_beatmap ON algeria_top50(beatmap_id);
  `);

  log('INFO', '✅ Database tables and indexes ensured');
}

// Enhanced API client with better error handling
const client_id = process.env.OSU_CLIENT_ID;
const client_secret = process.env.OSU_CLIENT_SECRET;
let access_token = null;
let token_expiry = 0;

class APIRateLimiter {
  constructor() {
    this.consecutiveErrors = 0;
    this.lastErrorTime = 0;
  }
  
  async handleError(error) {
    this.consecutiveErrors++;
    this.lastErrorTime = Date.now();
    
    const backoffTime = Math.min(
      1000 * Math.pow(2, this.consecutiveErrors), 
      60000
    );
    
    log('WARN', `⏳ API error #${this.consecutiveErrors}, backing off ${backoffTime}ms`);
    await sleep(backoffTime);
  }
  
  resetErrors() {
    if (this.consecutiveErrors > 0) {
      log('INFO', '✅ API errors cleared');
      this.consecutiveErrors = 0;
    }
  }
}

const rateLimiter = new APIRateLimiter();

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

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
    log('INFO', '🔑 Obtained new osu! token (expires in', response.data.expires_in, 's)');
    rateLimiter.resetErrors();
    return access_token;
  } catch (err) {
    log('ERROR', '❌ Failed to get access token:', err.message);
    throw err;
  }
}

const limiter = new Bottleneck({ maxConcurrent: 3, minTime: 600 });
const sleep = ms => new Promise(res => setTimeout(res, ms));

// Scan metrics tracking
let currentScanMetrics = {
  startTime: null,
  beatmapsScanned: 0,
  algerianScoresFound: 0,
  apiErrors: 0
};

function trackScanMetrics(found, error = false) {
  if (!currentScanMetrics.startTime) {
    currentScanMetrics.startTime = Date.now();
  }
  currentScanMetrics.beatmapsScanned++;
  if (found > 0) currentScanMetrics.algerianScoresFound += found;
  if (error) currentScanMetrics.apiErrors++;
}

// Enhanced beatmap fetching with difficulty info
async function fetchLeaderboard(beatmapId, beatmapTitle) {
  const maxRetries = 4;
  let attempt = 0;
  
  while (attempt < maxRetries) {
    try {
      const token = await getAccessToken();
      
      // Fetch both scores and beatmap info
      const [scoresRes, beatmapRes] = await Promise.all([
        axios.get(`https://osu.ppy.sh/api/v2/beatmaps/${beatmapId}/scores`, {
          headers: { Authorization: `Bearer ${token}` }
        }),
        axios.get(`https://osu.ppy.sh/api/v2/beatmaps/${beatmapId}`, {
          headers: { Authorization: `Bearer ${token}` }
        }).catch(() => ({ data: null })) // Don't fail if beatmap info unavailable
      ]);
      
      const scores = scoresRes.data.scores || [];
      const beatmapInfo = beatmapRes.data;
      const algerianScores = scores.filter(s => s.user?.country?.code === 'DZ');
      
      if (algerianScores.length > 0) {
        const now = Date.now();
        const client = await pool.connect();
        
        try {
          await client.query('BEGIN');
          await client.query('DELETE FROM algeria_top50 WHERE beatmap_id = $1', [beatmapId]);
          
          for (let i = 0; i < algerianScores.length; i++) {
            const s = algerianScores[i];
            const mods = s.mods?.length ? s.mods.join(',') : 'None';
            const accuracyText = typeof s.accuracy === 'number'
              ? (s.accuracy * 100).toFixed(2) + '%'
              : (s.accuracy || 'N/A');
            
            await client.query(
              `INSERT INTO algeria_top50
                (beatmap_id, beatmap_title, player_id, username, rank, score, accuracy, mods, pp, difficulty_rating, last_updated)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
               ON CONFLICT (beatmap_id, player_id) DO UPDATE
                 SET beatmap_title = EXCLUDED.beatmap_title,
                     username = EXCLUDED.username,
                     rank = EXCLUDED.rank,
                     score = EXCLUDED.score,
                     accuracy = EXCLUDED.accuracy,
                     mods = EXCLUDED.mods,
                     pp = EXCLUDED.pp,
                     difficulty_rating = EXCLUDED.difficulty_rating,
                     last_updated = EXCLUDED.last_updated`,
              [
                beatmapId, beatmapTitle, s.user.id, s.user.username,
                i + 1, s.score, accuracyText, mods, s.pp || 0,
                beatmapInfo?.difficulty_rating || 0, now
              ]
            );
          }
          
          await client.query('COMMIT');
          log('INFO', `🇩🇿 Saved ${algerianScores.length} Algerian scores for map ${beatmapId}`);
          
          // Send Discord notification for new #1 scores
          if (algerianScores[0] && process.env.DISCORD_WEBHOOK_URL) {
            await sendDiscordNotification(algerianScores[0], beatmapTitle, beatmapId);
          }
          
        } catch (e) {
          await client.query('ROLLBACK');
          throw e;
        } finally {
          client.release();
        }
      }
      
      trackScanMetrics(algerianScores.length);
      rateLimiter.resetErrors();
      return;
      
    } catch (err) {
      const status = err.response?.status;
      log('WARN', `⚠️ fetchLeaderboard error for ${beatmapId} (attempt ${attempt + 1}):`, 
          err.response?.data || err.message);
      
      trackScanMetrics(0, true);
      
      if (status === 401) { 
        access_token = null; 
        attempt++; 
        await sleep(1000 * attempt); 
        continue; 
      }
      
      if (status === 429) { 
        await rateLimiter.handleError(err);
        attempt++; 
        continue; 
      }
      
      break;
    }
  }
}

// Discord webhook notifications
async function sendDiscordNotification(score, beatmapTitle, beatmapId) {
  if (!process.env.DISCORD_WEBHOOK_URL) return;
  
  try {
    const embed = {
      title: "New Algerian #1 Score! 🇩🇿",
      description: `**${score.username}** achieved rank #1!`,
      fields: [
        { name: "Beatmap", value: `[${beatmapTitle}](https://osu.ppy.sh/beatmaps/${beatmapId})`, inline: false },
        { name: "Score", value: Number(score.score).toLocaleString(), inline: true },
        { name: "Accuracy", value: (score.accuracy * 100).toFixed(2) + '%', inline: true },
        { name: "Mods", value: score.mods?.join(',') || 'None', inline: true },
        { name: "PP", value: score.pp ? score.pp.toFixed(0) + 'pp' : 'N/A', inline: true }
      ],
      color: 0xff66aa,
      timestamp: new Date().toISOString(),
      thumbnail: { url: `https://a.ppy.sh/${score.user.id}` }
    };
    
    await axios.post(process.env.DISCORD_WEBHOOK_URL, {
      embeds: [embed]
    });
    
    log('INFO', `📢 Discord notification sent for ${score.username}'s #1 on ${beatmapTitle}`);
  } catch (err) {
    log('ERROR', 'Discord notification failed:', err.message);
  }
}

// Enhanced beatmap fetching with filtering
async function getAllBeatmaps() {
  let allBeatmaps = [];
  let page = 1;
  const maxPages = 100; // Prevent infinite loops
  
  while (page <= maxPages) {
    try {
      const token = await getAccessToken();
      log('INFO', `📄 Fetching beatmap page ${page}...`);
      
      const res = await axios.get('https://osu.ppy.sh/api/v2/beatmapsets/search', {
        headers: { Authorization: `Bearer ${token}` },
        params: { 
          mode: 'osu', 
          nsfw: false, 
          sort: 'ranked_desc', 
          page,
          's': 'ranked' // Only ranked maps
        }
      });
      
      const sets = res.data.beatmapsets || [];
      if (sets.length === 0) break;
      
      const beatmaps = sets.flatMap(set =>
        (set.beatmaps || [])
          .filter(bm => bm.difficulty_rating >= 3.0) // Filter by difficulty
          .map(bm => ({ 
            id: bm.id, 
            title: `${set.artist} - ${set.title} [${bm.version}]`,
            difficulty: bm.difficulty_rating
          }))
      );
      
      allBeatmaps.push(...beatmaps);
      page++;
      await sleep(500);
      
    } catch (err) {
      log('ERROR', `❌ Failed to fetch beatmap page ${page}:`, err.response?.data || err.message);
      break;
    }
  }
  
  log('INFO', `📊 Total beatmaps fetched: ${allBeatmaps.length}`);
  return allBeatmaps;
}

// Progress management
async function getProgress(key) {
  const row = await getRow('SELECT value FROM progress WHERE key = $1', [key]);
  return row ? row.value : null;
}

async function saveProgress(key, value) {
  await query(
    `INSERT INTO progress (key, value) VALUES ($1, $2)
     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
    [key, value]
  );
}

// Player statistics calculation
async function calculatePlayerStats() {
  log('INFO', '📊 Calculating player statistics...');
  
  const stats = await getRows(`
    SELECT 
      username,
      COUNT(*) as total_scores,
      AVG(rank::numeric) as avg_rank,
      MAX(score) as best_score,
      COUNT(CASE WHEN rank = 1 THEN 1 END) as first_places
    FROM algeria_top50
    GROUP BY username
  `);
  
  const now = Date.now();
  
  for (const stat of stats) {
    await query(`
      INSERT INTO player_stats (username, total_scores, avg_rank, best_score, first_places, last_calculated)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (username) DO UPDATE SET
        total_scores = EXCLUDED.total_scores,
        avg_rank = EXCLUDED.avg_rank,
        best_score = EXCLUDED.best_score,
        first_places = EXCLUDED.first_places,
        last_calculated = EXCLUDED.last_calculated
    `, [
      stat.username, 
      parseInt(stat.total_scores), 
      parseFloat(stat.avg_rank),
      parseInt(stat.best_score), 
      parseInt(stat.first_places), 
      now
    ]);
  }
  
  log('INFO', `✅ Updated stats for ${stats.length} players`);
}

// Enhanced update function with metrics
async function updateLeaderboards() {
  log('INFO', "🔄 Starting Algerian leaderboards update...");
  
  // Reset scan metrics
  currentScanMetrics = {
    startTime: Date.now(),
    beatmapsScanned: 0,
    algerianScoresFound: 0,
    apiErrors: 0
  };
  
  const beatmaps = await getAllBeatmaps();
  await saveProgress("total_beatmaps", beatmaps.length);

  // Priority scanning with better query
  const priorityBeatmaps = await getRows(`
    SELECT beatmap_id, MIN(beatmap_title) AS beatmap_title
    FROM algeria_top50
    GROUP BY beatmap_id
    ORDER BY MIN(last_updated) ASC
    LIMIT 200
  `);
  
  if (priorityBeatmaps.length > 0) {
    log('INFO', `⚡ Priority scanning ${priorityBeatmaps.length} known Algerian beatmaps`);
    
    for (const bm of priorityBeatmaps) {
      await limiter.schedule(() => fetchLeaderboard(bm.beatmap_id, bm.beatmap_title));
      await sleep(500);
    }
  }

  // Regular scanning with wraparound
  let startIndex = parseInt(await getProgress("last_index") || "0", 10);
  if (startIndex >= beatmaps.length) {
    log('INFO', "🔁 Reached end of beatmaps, starting from beginning");
    startIndex = 0;
  }

  log('INFO', `📌 Scanning from index ${startIndex} of ${beatmaps.length}`);
  
  for (let i = startIndex; i < beatmaps.length; i++) {
    const bm = beatmaps[i];
    await limiter.schedule(() => fetchLeaderboard(bm.id, bm.title));
    await saveProgress("last_index", i + 1);
    await sleep(500);
  }
  
  // Save scan metrics
  await query(`
    INSERT INTO scan_metrics (scan_start, scan_end, beatmaps_scanned, algerian_scores_found, api_errors)
    VALUES ($1, $2, $3, $4, $5)
  `, [
    currentScanMetrics.startTime,
    Date.now(),
    currentScanMetrics.beatmapsScanned,
    currentScanMetrics.algerianScoresFound,
    currentScanMetrics.apiErrors
  ]);
  
  // Calculate player stats
  await calculatePlayerStats();
  
  log('INFO', "✅ Leaderboard update completed");
}

// Enhanced API Endpoints

// Main leaderboard with filtering
app.get('/api/algeria-top50', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit || '100', 10), 500);
    const offset = parseInt(req.query.offset || '0', 10);
    const sort = req.query.sort || 'score';
    const order = (req.query.order || 'DESC').toUpperCase();
    const minDifficulty = parseFloat(req.query.min_difficulty || '0');
    const mods = req.query.mods;
    const player = req.query.player;
    
    const allowedSort = ['score', 'rank', 'last_updated', 'pp', 'difficulty_rating'];
    if (!allowedSort.includes(sort)) {
      return res.status(400).json({ error: 'Invalid sort column' });
    }
    
    if (!['ASC', 'DESC'].includes(order)) {
      return res.status(400).json({ error: 'Invalid sort order' });
    }
    
    let whereClause = 'WHERE 1=1';
    let params = [];
    let paramCount = 0;
    
    if (minDifficulty > 0) {
      whereClause += ` AND difficulty_rating >= $${++paramCount}`;
      params.push(minDifficulty);
    }
    
    if (mods && mods !== 'all') {
      whereClause += ` AND mods ILIKE $${++paramCount}`;
      params.push(`%${mods}%`);
    }
    
    if (player) {
      whereClause += ` AND username ILIKE $${++paramCount}`;
      params.push(`%${player}%`);
    }
    
    params.push(limit, offset);
    
    const rows = await getRows(
      `SELECT * FROM algeria_top50 
       ${whereClause}
       ORDER BY ${sort} ${order} 
       LIMIT $${++paramCount} OFFSET $${++paramCount}`,
      params
    );
    
    res.json(rows);
  } catch (err) {
    log('ERROR', '❌ /api/algeria-top50 DB error:', err.message);
    res.status(500).json({ error: 'Database error' });
  }
});

// Player scores endpoint
app.get('/api/player-scores', async (req, res) => {
  try {
    const username = req.query.username;
    if (!username) return res.status(400).json({ error: 'Username required' });
    
    const rows = await getRows(
      `SELECT * FROM algeria_top50
       WHERE username ILIKE $1
       ORDER BY score DESC`,
      [`%${username}%`]
    );
    
    res.json(rows);
  } catch (err) {
    log('ERROR', '❌ /api/player-scores DB error:', err.message);
    res.status(500).json({ error: 'Database error' });
  }
});

// Player statistics endpoint
app.get('/api/player-stats/:username', async (req, res) => {
  try {
    const username = req.params.username;
    const stats = await getRow(`
      SELECT * FROM player_stats 
      WHERE username ILIKE $1
    `, [`%${username}%`]);
    
    if (!stats) {
      return res.status(404).json({ error: 'Player not found' });
    }
    
    res.json({
      ...stats,
      avg_rank: parseFloat(stats.avg_rank).toFixed(2)
    });
  } catch (err) {
    log('ERROR', '❌ Player stats error:', err.message);
    res.status(500).json({ error: 'Database error' });
  }
});

// Top players endpoint
app.get('/api/top-players', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit || '20', 10);
    const sort = req.query.sort || 'first_places';
    
    const allowedSort = ['first_places', 'total_scores', 'avg_rank', 'best_score'];
    if (!allowedSort.includes(sort)) {
      return res.status(400).json({ error: 'Invalid sort column' });
    }
    
    const order = sort === 'avg_rank' ? 'ASC' : 'DESC';
    
    const players = await getRows(`
      SELECT * FROM player_stats
      WHERE total_scores > 0
      ORDER BY ${sort} ${order}
      LIMIT $1
    `, [limit]);
    
    res.json(players.map(p => ({
      ...p,
      avg_rank: parseFloat(p.avg_rank).toFixed(2)
    })));
  } catch (err) {
    log('ERROR', '❌ Top players error:', err.message);
    res.status(500).json({ error: 'Database error' });
  }
});

// Recent activity endpoint
app.get('/api/recent-activity', async (req, res) => {
  try {
    const hours = parseInt(req.query.hours || '24', 10);
    const cutoff = Date.now() - (hours * 60 * 60 * 1000);
    
    const recent = await getRows(`
      SELECT * FROM algeria_top50
      WHERE last_updated > $1
      ORDER BY last_updated DESC
      LIMIT 100
    `, [cutoff]);
    
    res.json(recent);
  } catch (err) {
    log('ERROR', '❌ Recent activity error:', err.message);
    res.status(500).json({ error: 'Database error' });
  }
});

// Beatmap leaderboard endpoint
app.get('/api/beatmap/:id', async (req, res) => {
  try {
    const beatmapId = req.params.id;
    const scores = await getRows(`
      SELECT * FROM algeria_top50
      WHERE beatmap_id = $1
      ORDER BY rank ASC
    `, [beatmapId]);
    
    if (scores.length === 0) {
      return res.status(404).json({ error: 'No Algerian scores found for this beatmap' });
    }
    
    res.json(scores);
  } catch (err) {
    log('ERROR', '❌ Beatmap scores error:', err.message);
    res.status(500).json({ error: 'Database error' });
  }
});

// Enhanced stats endpoint
app.get('/api/stats', async (req, res) => {
  try {
    const [totalScores, totalBeatmaps, totalPlayers, lastUpdate, topScore] = await Promise.all([
      getRow(`SELECT COUNT(*) FROM algeria_top50`),
      getRow(`SELECT COUNT(DISTINCT beatmap_id) FROM algeria_top50`),
      getRow(`SELECT COUNT(DISTINCT username) FROM algeria_top50`),
      getRow(`SELECT MAX(last_updated) FROM algeria_top50`),
      getRow(`SELECT MAX(score) FROM algeria_top50`)
    ]);
    
    res.json({
      totalScores: parseInt(totalScores.count, 10),
      totalBeatmaps: parseInt(totalBeatmaps.count, 10),
      totalPlayers: parseInt(totalPlayers.count, 10),
      lastUpdated: lastUpdate.max ? parseInt(lastUpdate.max, 10) : null,
      topScore: topScore.max ? parseInt(topScore.max, 10) : null
    });
  } catch (err) {
    log('ERROR', '❌ /api/stats DB error:', err.message);
    res.status(500).json({ error: 'Database error' });
  }
});

// Scan progress endpoint
app.get('/api/scan-progress', async (req, res) => {
  try {
    const total = parseInt(await getProgress("total_beatmaps") || "0", 10);
    const processed = parseInt(await getProgress("last_index") || "0", 10);
    
    if (total === 0) {
      return res.json({ processed: 0, total: 0, percentage: "0.00" });
    }
    
    res.json({
      processed,
      total,
      percentage: ((processed / total) * 100).toFixed(2),
      currentMetrics: currentScanMetrics
    });
  } catch (err) {
    log('ERROR', '❌ /api/scan-progress error:', err.message);
    res.status(500).json({ error: 'Progress check failed' });
  }
});

// Scan metrics endpoint
app.get('/api/scan-metrics', async (req, res) => {
  try {
    const recentMetrics = await getRows(`
      SELECT * FROM scan_metrics 
      ORDER BY created_at DESC 
      LIMIT 10
    `);
    
    const runtime = currentScanMetrics.startTime ? 
      ((Date.now() - currentScanMetrics.startTime) / 1000 / 60).toFixed(1) : 0;
    
    res.json({
      current: {
        ...currentScanMetrics,
        runtimeMinutes: runtime,
        scoresPerMinute: runtime > 0 ? 
          (currentScanMetrics.algerianScoresFound / runtime).toFixed(2) : '0'
      },
      recent: recentMetrics
    });
  } catch (err) {
    log('ERROR', '❌ Scan metrics error:', err.message);
    res.status(500).json({ error: 'Metrics fetch failed' });
  }
});

// Health check endpoint
app.get('/health', async (req, res) => {
  try {
    await query('SELECT 1');
    const token = await getAccessToken();
    const lastScan = await getRow('SELECT MAX(last_updated) as last FROM algeria_top50');
    const timeSinceLastScan = Date.now() - (lastScan.last || 0);
    
    const health = {
      status: 'healthy',
      database: 'connected',
      osuApi: token ? 'authenticated' : 'error',
      lastScanMinutesAgo: Math.floor(timeSinceLastScan / 60000),
      timestamp: new Date().toISOString()
    };
    
    res.json(health);
  } catch (err) {
    log('ERROR', '❌ Health check failed:', err.message);
    res.status(503).json({
      status: 'unhealthy',
      error: err.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Enhanced scan status page
app.get('/scan-status', (req, res) => {
  res.send(`
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="UTF-8">
      <title>Enhanced Scan Progress</title>
      <style>
        body { font-family: 'Segoe UI', Arial, sans-serif; background: #f5f5f5; padding: 20px; margin: 0; }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 { color: #333; text-align: center; margin-bottom: 30px; }
        .card { background: white; border-radius: 8px; padding: 20px; margin-bottom: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .progress-container { width: 100%; background: #ddd; border-radius: 25px; overflow: hidden; height: 30px; margin: 15px 0; }
        .progress-bar { height: 100%; background: linear-gradient(45deg, #ff66aa, #ff3399); text-align: center; color: white; line-height: 30px; font-weight: bold; transition: width 0.5s ease; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; }
        .stat-item { text-align: center; padding: 15px; background: #f8f9fa; border-radius: 6px; }
        .stat-value { font-size: 2em; font-weight: bold; color: #ff3399; }
        .stat-label { color: #666; margin-top: 5px; }
        .metrics-table { width: 100%; border-collapse: collapse; margin-top: 15px; }
        .metrics-table th, .metrics-table td { padding: 8px 12px; text-align: left; border-bottom: 1px solid #eee; }
        .metrics-table th { background: #f8f9fa; font-weight: bold; }
        .status-indicator { display: inline-block; width: 12px; height: 12px; border-radius: 50%; margin-right: 8px; }
        .status-healthy { background: #28a745; }
        .status-warning { background: #ffc107; }
        .status-error { background: #dc3545; }
      </style>
    </head>
    <body>
      <div class="container">
        <h1>🇩🇿 osu! Algeria Leaderboard - Enhanced Scan Dashboard</h1>
        
        <div class="card">
          <h2>Current Scan Progress</h2>
          <div class="progress-container">
            <div class="progress-bar" id="progress" style="width: 0%">0%</div>
          </div>
          <div class="stats-grid">
            <div class="stat-item">
              <div class="stat-value" id="processed">0</div>
              <div class="stat-label">Processed</div>
            </div>
            <div class="stat-item">
              <div class="stat-value" id="total">0</div>
              <div class="stat-label">Total Beatmaps</div>
            </div>
            <div class="stat-item">
              <div class="stat-value" id="scoresFound">0</div>
              <div class="stat-label">Scores Found</div>
            </div>
            <div class="stat-item">
              <div class="stat-value" id="runtime">0</div>
              <div class="stat-label">Runtime (min)</div>
            </div>
          </div>
        </div>

        <div class="card">
          <h2>System Health</h2>
          <div id="healthStatus">Checking...</div>
        </div>

        <div class="card">
          <h2>Database Statistics</h2>
          <div class="stats-grid" id="dbStats">
            <div class="stat-item">
              <div class="stat-value" id="totalScores">0</div>
              <div class="stat-label">Total Scores</div>
            </div>
            <div class="stat-item">
              <div class="stat-value" id="totalBeatmaps">0</div>
              <div class="stat-label">Beatmaps</div>
            </div>
            <div class="stat-item">
              <div class="stat-value" id="totalPlayers">0</div>
              <div class="stat-label">Players</div>
            </div>
            <div class="stat-item">
              <div class="stat-value" id="lastUpdate">-</div>
              <div class="stat-label">Last Update</div>
            </div>
          </div>
        </div>

        <div class="card">
          <h2>Recent Scan History</h2>
          <table class="metrics-table" id="scanHistory">
            <thead>
              <tr>
                <th>Date</th>
                <th>Duration</th>
                <th>Beatmaps Scanned</th>
                <th>Scores Found</th>
                <th>API Errors</th>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>
      </div>

      <script>
        async function updateProgress() {
          try {
            const res = await fetch('/api/scan-progress');
            const data = await res.json();
            
            document.getElementById('processed').innerText = data.processed.toLocaleString();
            document.getElementById('total').innerText = data.total.toLocaleString();
            document.getElementById('progress').style.width = data.percentage + '%';
            document.getElementById('progress').innerText = data.percentage + '%';
            
            if (data.currentMetrics) {
              document.getElementById('scoresFound').innerText = data.currentMetrics.algerianScoresFound;
              document.getElementById('runtime').innerText = data.currentMetrics.startTime ? 
                Math.floor((Date.now() - data.currentMetrics.startTime) / 60000) : '0';
            }
          } catch (err) {
            console.error('Error fetching progress', err);
          }
        }

        async function updateHealth() {
          try {
            const res = await fetch('/health');
            const health = await res.json();
            const statusDiv = document.getElementById('healthStatus');
            
            const dbStatus = health.database === 'connected' ? 'healthy' : 'error';
            const apiStatus = health.osuApi === 'authenticated' ? 'healthy' : 'error';
            
            statusDiv.innerHTML = \`
              <p><span class="status-indicator status-\${dbStatus}"></span><strong>Database:</strong> \${health.database}</p>
              <p><span class="status-indicator status-\${apiStatus}"></span><strong>osu! API:</strong> \${health.osuApi}</p>
              <p><strong>Last scan:</strong> \${health.lastScanMinutesAgo} minutes ago</p>
            \`;
          } catch (err) {
            document.getElementById('healthStatus').innerHTML = '<p><span class="status-indicator status-error"></span>Health check failed</p>';
          }
        }

        async function updateStats() {
          try {
            const res = await fetch('/api/stats');
            const stats = await res.json();
            
            document.getElementById('totalScores').innerText = stats.totalScores.toLocaleString();
            document.getElementById('totalBeatmaps').innerText = stats.totalBeatmaps.toLocaleString();
            document.getElementById('totalPlayers').innerText = stats.totalPlayers.toLocaleString();
            document.getElementById('lastUpdate').innerText = stats.lastUpdated ? 
              new Date(stats.lastUpdated).toLocaleString() : 'Never';
          } catch (err) {
            console.error('Error fetching stats', err);
          }
        }

        async function updateScanHistory() {
          try {
            const res = await fetch('/api/scan-metrics');
            const data = await res.json();
            const tbody = document.querySelector('#scanHistory tbody');
            
            tbody.innerHTML = '';
            data.recent.forEach(scan => {
              const duration = scan.scan_end ? 
                Math.floor((scan.scan_end - scan.scan_start) / 60000) + ' min' : 
                'In progress';
              
              const row = tbody.insertRow();
              row.innerHTML = \`
                <td>\${new Date(scan.created_at).toLocaleString()}</td>
                <td>\${duration}</td>
                <td>\${scan.beatmaps_scanned.toLocaleString()}</td>
                <td>\${scan.algerian_scores_found.toLocaleString()}</td>
                <td>\${scan.api_errors}</td>
              \`;
            });
          } catch (err) {
            console.error('Error fetching scan history', err);
          }
        }

        function updateAll() {
          updateProgress();
          updateHealth();
          updateStats();
          updateScanHistory();
        }

        // Initial load and periodic updates
        updateAll();
        setInterval(updateProgress, 5000);
        setInterval(updateHealth, 30000);
        setInterval(updateStats, 60000);
        setInterval(updateScanHistory, 120000);
      </script>
    </body>
    </html>
  `);
});

// Database cleanup and maintenance
async function cleanupOldData() {
  try {
    log('INFO', '🧹 Starting database cleanup...');
    
    // Remove scores older than 6 months that haven't been updated
    const sixMonthsAgo = Date.now() - (6 * 30 * 24 * 60 * 60 * 1000);
    
    const result = await query(`
      DELETE FROM algeria_top50 
      WHERE last_updated < $1
    `, [sixMonthsAgo]);
    
    log('INFO', `🧹 Cleaned up ${result.rowCount} old scores`);
    
    // Clean up old scan metrics (keep last 100 records)
    await query(`
      DELETE FROM scan_metrics 
      WHERE id NOT IN (
        SELECT id FROM scan_metrics 
        ORDER BY created_at DESC 
        LIMIT 100
      )
    `);
    
    // Vacuum and analyze for performance
    await query('VACUUM ANALYZE algeria_top50');
    await query('VACUUM ANALYZE player_stats');
    await query('VACUUM ANALYZE scan_metrics');
    
    log('INFO', '🔧 Database maintenance completed');
  } catch (err) {
    log('ERROR', '❌ Database cleanup failed:', err.message);
  }
}

// Force manual scan endpoint (admin only)
app.post('/api/force-scan', async (req, res) => {
  try {
    // Basic auth check (you should implement proper auth)
    const authHeader = req.headers.authorization;
    if (!authHeader || authHeader !== `Bearer ${process.env.ADMIN_TOKEN}`) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
    
    log('INFO', '🔧 Manual scan triggered via API');
    
    // Don't await - run in background
    updateLeaderboards().catch(err => {
      log('ERROR', '❌ Manual scan failed:', err.message);
    });
    
    res.json({ message: 'Manual scan started' });
  } catch (err) {
    log('ERROR', '❌ Force scan error:', err.message);
    res.status(500).json({ error: 'Failed to start scan' });
  }
});

// Export data endpoint
app.get('/api/export', async (req, res) => {
  try {
    const format = req.query.format || 'json';
    const limit = Math.min(parseInt(req.query.limit || '1000'), 10000);
    
    const data = await getRows(`
      SELECT * FROM algeria_top50 
      ORDER BY score DESC 
      LIMIT $1
    `, [limit]);
    
    if (format === 'csv') {
      const csv = [
        'beatmap_id,beatmap_title,player_id,username,rank,score,accuracy,mods,pp,difficulty_rating,last_updated',
        ...data.map(row => Object.values(row).map(v => `"${v}"`).join(','))
      ].join('\n');
      
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', 'attachment; filename=algeria_scores.csv');
      res.send(csv);
    } else {
      res.json(data);
    }
  } catch (err) {
    log('ERROR', '❌ Export error:', err.message);
    res.status(500).json({ error: 'Export failed' });
  }
});

// Start server with enhanced error handling
process.on('uncaughtException', (err) => {
  log('ERROR', '💥 Uncaught exception:', err);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  log('ERROR', '💥 Unhandled rejection at:', promise, 'reason:', reason);
});

app.listen(port, async () => {
  log('INFO', `✅ Enhanced osu! Algerian leaderboard tracker running at http://localhost:${port}`);
  log('INFO', `📊 Admin dashboard available at http://localhost:${port}/scan-status`);
  
  try {
    await ensureTables();
    
    // Initial update
    await updateLeaderboards();
    
    // Schedule regular updates (every 30 minutes)
    setInterval(updateLeaderboards, 30 * 60 * 1000);
    
    // Schedule weekly cleanup
    setInterval(cleanupOldData, 7 * 24 * 60 * 60 * 1000);
    
    log('INFO', '🚀 All systems operational');
  } catch (err) {
    log('ERROR', '❌ Startup failed:', err.message);
  }
});

setInterval(updateLeaderboards, 6 * 60 * 60 * 1000); // Every 6 hours
