const express = require('express');
const WebSocket = require('ws');
const { createServer } = require('http');
const axios = require('axios');
const pool = require('./db');  // your pg pool
const redisClient = require('./redisClient'); // your redis client
const playerDiscovery = require('./playerDiscovery');
const {
  validateInput,
  asyncHandler,
  log,
  getCached,
  getRow,
  getRows,
  getAccessToken,
  getCacheKey,
  updateLeaderboards,
  calculateDailyStats,
  ensureTables,
  query,
  authenticateToken
} = require('./utils'); // hypothetical utility imports

const app = express();
const port = process.env.PORT || 3000;

app.use(express.json());

// === API ENDPOINTS ===

// Enhanced player profile
app.get('/api/players/:username',
  validateInput({
    username: { required: true, minLength: 2, maxLength: 15 }
  }),
  asyncHandler(async (req, res) => {
    const username = req.params.username.toLowerCase();
    const cacheKey = getCacheKey('player', username);

    const data = await getCached(cacheKey, async () => {
      // Use exact match or prefix search but avoid '%username%'
      const playerStats = await getRow(`SELECT * FROM player_stats WHERE LOWER(username) = $1`, [username]);
      if (!playerStats) return null;

      const [recentScores, bestScores, skills, achievements, activity] = await Promise.all([
        getRows(`SELECT * FROM algeria_top50 WHERE LOWER(username) = $1 ORDER BY last_updated DESC LIMIT 10`, [username]),
        getRows(`SELECT * FROM algeria_top50 WHERE LOWER(username) = $1 ORDER BY pp DESC LIMIT 10`, [username]),
        getRows(`SELECT skill_type, skill_value, calculated_at FROM skill_tracking WHERE LOWER(username) = $1 ORDER BY calculated_at DESC LIMIT 25`, [username]),
        getRows(`SELECT a.name, a.description, a.icon, a.points, pa.unlocked_at
                 FROM player_achievements pa
                 JOIN achievements a ON pa.achievement_id = a.id
                 WHERE LOWER(pa.username) = $1
                 ORDER BY pa.unlocked_at DESC`, [username]),
        getRows(`SELECT activity_type, activity_data, timestamp FROM player_activity WHERE LOWER(username) = $1 ORDER BY timestamp DESC LIMIT 10`, [username])
      ]);

      // Calculate skill progression
      const skillProgression = {};
      for (const skill of skills) {
        if (!skillProgression[skill.skill_type]) skillProgression[skill.skill_type] = [];
        skillProgression[skill.skill_type].push({
          value: parseFloat(skill.skill_value),
          timestamp: parseInt(skill.calculated_at)
        });
      }

      // Calculate rank among all players
      const rankResult = await getRow(
        `SELECT COUNT(*) + 1 AS rank FROM player_stats WHERE weighted_pp > $1`,
        [playerStats.weighted_pp || 0]
      );

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
  let { sort = 'weighted_pp', limit = 50, offset = 0, timeframe = 'all', minScores = 5 } = req.query;
  limit = Math.min(parseInt(limit), 100);
  offset = parseInt(offset);
  minScores = parseInt(minScores);

  const allowedSort = ['weighted_pp', 'total_pp', 'first_places', 'avg_rank', 'accuracy_avg', 'total_scores'];
  sort = allowedSort.includes(sort.toLowerCase()) ? sort.toLowerCase() : 'weighted_pp';
  const sortOrder = (sort === 'avg_rank') ? 'ASC' : 'DESC';

  let whereClause = `WHERE total_scores >= $1`;
  const params = [minScores];
  let paramCount = 1;

  if (timeframe !== 'all') {
    const timeRanges = {
      '24h': 24 * 60 * 60 * 1000,
      '7d': 7 * 24 * 60 * 60 * 1000,
      '30d': 30 * 24 * 60 * 60 * 1000
    };
    const cutoff = Date.now() - (timeRanges[timeframe] || 0);
    whereClause += ` AND last_calculated >= $${++paramCount}`;
    params.push(cutoff);
  }

  params.push(limit, offset);

  const data = await getCached(getCacheKey('rankings', sort, limit, offset, timeframe, minScores), async () => {
    return await getRows(`
      SELECT *, ROW_NUMBER() OVER (ORDER BY ${sort} ${sortOrder}) AS rank
      FROM player_stats
      ${whereClause}
      ORDER BY ${sort} ${sortOrder}
      LIMIT $${++paramCount} OFFSET $${++paramCount}
    `, params);
  }, 300);

  res.json({
    success: true,
    data,
    meta: {
      sort,
      limit,
      offset,
      hasMore: data.length === limit
    }
  });
}));

// Player comparison endpoint
app.get('/api/compare/:username1/:username2',
  validateInput({
    username1: { required: true, minLength: 2, maxLength: 15 },
    username2: { required: true, minLength: 2, maxLength: 15 }
  }),
  asyncHandler(async (req, res) => {
    const username1 = req.params.username1.toLowerCase();
    const username2 = req.params.username2.toLowerCase();
    const cacheKey = getCacheKey('compare', username1, username2);

    const data = await getCached(cacheKey, async () => {
      const [player1, player2] = await Promise.all([
        getRow(`SELECT * FROM player_stats WHERE LOWER(username) = $1`, [username1]),
        getRow(`SELECT * FROM player_stats WHERE LOWER(username) = $1`, [username2])
      ]);
      if (!player1 || !player2) return null;

      const [skills1, skills2] = await Promise.all([
        getRows(`SELECT skill_type, AVG(skill_value) as avg_skill FROM skill_tracking WHERE LOWER(username) = $1 GROUP BY skill_type`, [username1]),
        getRows(`SELECT skill_type, AVG(skill_value) as avg_skill FROM skill_tracking WHERE LOWER(username) = $1 GROUP BY skill_type`, [username2])
      ]);

      const skillComparison = {};
      ['aim', 'speed', 'accuracy', 'reading', 'consistency'].forEach(skill => {
        const s1 = skills1.find(s => s.skill_type === skill);
        const s2 = skills2.find(s => s.skill_type === skill);
        skillComparison[skill] = {
          player1: s1 ? parseFloat(s1.avg_skill) : 0,
          player2: s2 ? parseFloat(s2.avg_skill) : 0,
          difference: (s1 ? parseFloat(s1.avg_skill) : 0) - (s2 ? parseFloat(s2.avg_skill) : 0)
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
    const q = req.query.q.trim();
    const type = (req.query.type || 'all').toLowerCase();
    const limit = Math.min(parseInt(req.query.limit) || 20, 100);
    const cacheKey = getCacheKey('search', type, q, limit);

    const data = await getCached(cacheKey, async () => {
      const results = {};
      const searchPattern = `%${q}%`;

      if (type === 'all' || type === 'players') {
        results.players = await getRows(`
          SELECT username, weighted_pp, first_places, avatar_url
          FROM player_stats
          WHERE username ILIKE $1
          ORDER BY weighted_pp DESC
          LIMIT $2
        `, [searchPattern, limit]);
      }

      if (type === 'all' || type === 'beatmaps') {
        results.beatmaps = await getRows(`
          SELECT DISTINCT bm.beatmap_id, bm.artist, bm.title, bm.version, bm.difficulty_rating,
            COUNT(ats.username) as algerian_players
          FROM beatmap_metadata bm
          LEFT JOIN algeria_top50 ats ON bm.beatmap_id = ats.beatmap_id
          WHERE bm.artist ILIKE $1 OR bm.title ILIKE $1 OR bm.version ILIKE $1
          GROUP BY bm.beatmap_id, bm.artist, bm.title, bm.version, bm.difficulty_rating
          ORDER BY algerian_players DESC
          LIMIT $2
        `, [searchPattern, limit]);
      }

      return results;
    }, 600);

    res.json({ success: true, query: q, data });
  })
);

// Analytics overview endpoint
app.get('/api/analytics/overview', asyncHandler(async (req, res) => {
  const cacheKey = 'analytics:overview';
  const data = await getCached(cacheKey, async () => {
    const [
      totalStats, recentActivity, topPerformers,
      skillDistribution, modUsage, difficultyDistribution
    ] = await Promise.all([
      getRow(`
        SELECT
          COUNT(DISTINCT username) AS total_players,
          COUNT(*) AS total_scores,
          COUNT(DISTINCT beatmap_id) AS total_beatmaps,
          AVG(accuracy) AS avg_accuracy,
          MAX(score) AS highest_score,
          SUM(pp) AS total_pp
        FROM algeria_top50
      `),
      getRow(`
        SELECT COUNT(*) AS active_24h
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
        SELECT skill_type, AVG(skill_value) AS avg_value
        FROM skill_tracking
        WHERE calculated_at > $1
        GROUP BY skill_type
      `, [Date.now() - (30 * 24 * 60 * 60 * 1000)]),
      getRows(`
        SELECT mods, COUNT(*) AS usage_count, AVG(accuracy) AS avg_accuracy, AVG(pp) AS avg_pp
        FROM algeria_top50
        WHERE mods != 'None'
        GROUP BY mods
        ORDER BY usage_count DESC
        LIMIT 10
      `),
      getRows(`
        SELECT FLOOR(difficulty_rating) AS difficulty_range, COUNT(*) AS score_count, AVG(accuracy) AS avg_accuracy
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

// Admin and webhook endpoints (as you had them, using authenticateToken, validations etc.)
// ...

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

// === WEBSOCKET SERVER ===
const server = createServer(app);
const wss = new WebSocket.Server({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  log('INFO', 'New WebSocket connection from', req.socket.remoteAddress);
  ws.send(JSON.stringify({ type: 'connected', timestamp: Date.now() }));

  ws.on('close', () => log('DEBUG', 'WebSocket connection closed'));
  ws.on('error', err => log('ERROR', 'WebSocket error:', err.message));
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
    log('DEBUG', `Broadcasted to ${sentCount} clients: ${data.type}`);
  }
}
global.broadcastToClients = broadcastToClients;

// === SCHEDULED TASKS ===
const SCHEDULE_CONFIG = {
  countryRankingIntervalMs: 30 * 60 * 1000,
  discoveryIntervalMs: 4 * 60 * 60 * 1000,
  leaderboardIntervalMs: 2 * 60 * 60 * 1000,
  skillCleanupIntervalMs: 7 * 24 * 60 * 60 * 1000,
  skillCleanupCutoffDays: 90,
  dailyJobHourUTC: 0
};

function scheduleTasks() {
  setInterval(async () => {
    try {
      log('INFO', 'Scheduled: country rankings discovery');
      await playerDiscovery.discoverFromCountryRankings();
    } catch (err) {
      log('ERROR', 'Scheduled country rankings discovery failed:', err.message);
    }
  }, SCHEDULE_CONFIG.countryRankingIntervalMs);

  setInterval(async () => {
    try {
      log('INFO', 'Scheduled: comprehensive discovery run');
      await playerDiscovery.runDiscovery();
    } catch (err) {
      log('ERROR', 'Scheduled comprehensive discovery failed:', err.message);
    }
  }, SCHEDULE_CONFIG.discoveryIntervalMs);

  setInterval(async () => {
    try {
      log('INFO', 'Scheduled: leaderboard update');
      await updateLeaderboards();
    } catch (err) {
      log('ERROR', 'Scheduled leaderboard update failed:', err.message);
    }
  }, SCHEDULE_CONFIG.leaderboardIntervalMs);

  setInterval(async () => {
    try {
      const cutoff = Date.now() - (SCHEDULE_CONFIG.skillCleanupCutoffDays * 24 * 60 * 60 * 1000);
      const result = await query('DELETE FROM skill_tracking WHERE calculated_at < $1', [cutoff]);
      if (result.rowCount > 0) {
        log('INFO', `Cleaned up ${result.rowCount} old skill tracking entries`);
      }
    } catch (err) {
      log('ERROR', 'Skill tracking cleanup failed:', err.message);
    }
  }, SCHEDULE_CONFIG.skillCleanupIntervalMs);

  // Daily stats at configured UTC hour
  (function scheduleDailyStats() {
    try {
      const now = new Date();
      let nextRun = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), SCHEDULE_CONFIG.dailyJobHourUTC, 0, 0));
      if (nextRun <= now) nextRun.setUTCDate(nextRun.getUTCDate() + 1);
      const delay = nextRun - now;

      setTimeout(async function runDaily() {
        try {
          await calculateDailyStats();
        } catch (err) {
          log('ERROR', 'Daily stats calculation failed:', err.message);
        } finally {
          setTimeout(runDaily, 24 * 60 * 60 * 1000);
        }
      }, delay);
    } catch (err) {
      log('ERROR', 'Daily stats scheduler failed:', err.message);
    }
  })();

  log('INFO', 'Scheduled tasks initialized');
}

// === SERVER INITIALIZATION ===
async function initializeServer() {
  try {
    await ensureTables();
    scheduleTasks();

    // Initial startup runs
    setTimeout(async () => {
      log('INFO', 'Running initial player discovery...');
      try {
        await playerDiscovery.runDiscovery();
      } catch (err) {
        log('ERROR', 'Initial player discovery failed:', err.message);
      }
    }, 30000);

    setTimeout(async () => {
      log('INFO', 'Running initial leaderboard update...');
      try {
        await updateLeaderboards();
      } catch (err) {
        log('ERROR', 'Initial leaderboard update failed:', err.message);
      }
    }, 60000);

    log('INFO', 'Server initialization complete');
  } catch (err) {
    log('ERROR', 'Server initialization failed:', err.message);
    throw err;
  }
}

// === GRACEFUL SHUTDOWN ===
async function shutdown(code = 0) {
  try {
    log('INFO', 'Shutting down...');
    if (wss) {
      wss.clients.forEach(client => {
        try { client.terminate(); } catch (e) {}
      });
      wss.close();
    }
    if (server) {
      server.close();
    }
    try {
      await redisClient.quit();
    } catch {}
    try {
      await pool.end();
    } catch {}

    log('INFO', 'Shutdown complete');
    process.exit(code);
  } catch (err) {
    log('ERROR', 'Shutdown error:', err.message);
    process.exit(1);
  }
}

// Handle termination signals
process.on('SIGINT', () => shutdown(0));
process.on('SIGTERM', () => shutdown(0));

// Handle uncaught exceptions and unhandled rejections
process.on('uncaughtException', err => {
  log('ERROR', 'Uncaught exception:', err);
  shutdown(1);
});
process.on('unhandledRejection', (reason, promise) => {
  log('ERROR', 'Unhandled rejection:', reason);
  shutdown(1);
});

// === START SERVER ===
server.listen(port, async () => {
  log('INFO', `Server listening on port ${port}`);
  log('INFO', `WebSocket server at ws://localhost:${port}/ws`);
  log('INFO', `API available at http://localhost:${port}/api/`);
  log('INFO', `Health check at http://localhost:${port}/health`);

  await initializeServer();
});

// Export app and server for tests
module.exports = { app, server, playerDiscovery };
