const express = require('express');
const router = express.Router();
const { getRows, getRow } = require('../config/db');
const { validateInput } = require('../config/security');
const { cacheService } = require('../services/cache');
const { leaderboardService } = require('../services/leaderboard');

// Get main leaderboards with filtering
router.get('/', async (req, res) => {
  try {
    const {
      limit = 100,
      offset = 0,
      sort = 'score',
      order = 'DESC',
      minDifficulty,
      maxDifficulty,
      mods,
      player,
      beatmapId
    } = req.query;

    const allowedSort = [
      'rank', 'score', 'pp', 'accuracy', 'last_updated', 
      'difficulty_rating', 'max_combo'
    ];
    const sortColumn = allowedSort.includes(sort) ? sort : 'score';
    const sortOrder = order.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';

    let params = [];
    let whereClauses = [];

    // Add filters
    if (minDifficulty !== undefined && minDifficulty !== '') {
      params.push(parseFloat(minDifficulty));
      whereClauses.push(`difficulty_rating >= $${params.length}`);
    }
    if (maxDifficulty !== undefined && maxDifficulty !== '') {
      params.push(parseFloat(maxDifficulty));
      whereClauses.push(`difficulty_rating <= $${params.length}`);
    }
    if (mods) {
      params.push(mods);
      whereClauses.push(`mods = $${params.length}`);
    }
    if (player) {
      params.push(`%${player}%`);
      whereClauses.push(`username ILIKE $${params.length}`);
    }
    if (beatmapId) {
      params.push(beatmapId);
      whereClauses.push(`beatmap_id = $${params.length}`);
    }

    const whereClause = whereClauses.length ? `WHERE ${whereClauses.join(' AND ')}` : '';
    const paramCount = params.length;

    const sql = `
      SELECT 
        beatmap_id, beatmap_title, artist, difficulty_name, difficulty_rating,
        player_id, username, rank, score, accuracy, accuracy_text, mods, pp,
        max_combo, count_300, count_100, count_50, count_miss, 
        play_date, last_updated
      FROM algeria_top50
      ${whereClause}
      ORDER BY ${sortColumn} ${sortOrder}
      LIMIT $${paramCount + 1} OFFSET $${paramCount + 2}
    `;

    params.push(parseInt(limit), parseInt(offset));
    const data = await getRows(sql, params);

    res.json({
      success: true,
      data,
      meta: {
        sort: sortColumn,
        order: sortOrder,
        limit: parseInt(limit),
        offset: parseInt(offset),
        hasMore: data.length === parseInt(limit),
        filters: {
          minDifficulty,
          maxDifficulty,
          mods,
          player,
          beatmapId
        }
      }
    });
  } catch (error) {
    console.error('Leaderboards error:', error);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

// Get leaderboard for specific beatmap
router.get('/beatmap/:beatmapId', 
  validateInput({
    beatmapId: { required: true, type: 'number' }
  }),
  async (req, res) => {
    try {
      const { beatmapId } = req.params;
      const { limit = 50 } = req.query;
      
      // Try cache first
      let data = await cacheService.getLeaderboardCache(beatmapId);
      
      if (!data) {
        data = await leaderboardService.getBeatmapLeaderboard(beatmapId, parseInt(limit));
        
        // Cache for 5 minutes
        await cacheService.cacheLeaderboard(beatmapId, data, 300);
      }

      const beatmapInfo = await getRow(`
        SELECT artist, title, version, difficulty_rating, creator
        FROM beatmap_metadata 
        WHERE beatmap_id = $1
      `, [beatmapId]);

      res.json({
        success: true,
        data,
        beatmapInfo,
        meta: {
          beatmapId: parseInt(beatmapId),
          totalScores: data.length,
          limit: parseInt(limit)
        }
      });
    } catch (error) {
      console.error('Beatmap leaderboard error:', error);
      res.status(500).json({ success: false, error: 'Internal server error' });
    }
  }
);

// Get top scores across all beatmaps
router.get('/top-scores', async (req, res) => {
  try {
    const { 
      limit = 50, 
      timeframe = 'all',
      minPP = 0
    } = req.query;

    let whereClause = 'WHERE pp > $1';
    let params = [parseFloat(minPP)];

    // Add time filter
    if (timeframe !== 'all') {
      const timeRanges = {
        '24h': 24 * 60 * 60 * 1000,
        '7d': 7 * 24 * 60 * 60 * 1000,
        '30d': 30 * 24 * 60 * 60 * 1000
      };
      
      const cutoff = Date.now() - (timeRanges[timeframe] || 0);
      if (cutoff > 0) {
        params.push(cutoff);
        whereClause += ` AND last_updated > ${params.length}`;
      }
    }

    params.push(parseInt(limit));

    const data = await getRows(`
      SELECT 
        beatmap_id, beatmap_title, artist, difficulty_name, difficulty_rating,
        username, rank, score, accuracy, accuracy_text, mods, pp,
        max_combo, play_date, last_updated
      FROM algeria_top50
      ${whereClause}
      ORDER BY pp DESC
      LIMIT ${params.length}
    `, params);

    res.json({
      success: true,
      data,
      meta: {
        timeframe,
        minPP: parseFloat(minPP),
        limit: parseInt(limit),
        totalResults: data.length
      }
    });
  } catch (error) {
    console.error('Top scores error:', error);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

// Get recent scores
router.get('/recent', async (req, res) => {
  try {
    const { limit = 50, hours = 24 } = req.query;
    
    const cutoff = Date.now() - (parseInt(hours) * 60 * 60 * 1000);
    
    const data = await getRows(`
      SELECT 
        beatmap_id, beatmap_title, artist, difficulty_name, difficulty_rating,
        username, rank, score, accuracy, accuracy_text, mods, pp,
        max_combo, play_date, last_updated
      FROM algeria_top50
      WHERE last_updated > $1
      ORDER BY last_updated DESC
      LIMIT $2
    `, [cutoff, parseInt(limit)]);

    res.json({
      success: true,
      data,
      meta: {
        hours: parseInt(hours),
        limit: parseInt(limit),
        cutoff: new Date(cutoff).toISOString()
      }
    });
  } catch (error) {
    console.error('Recent scores error:', error);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

// Get first place scores
router.get('/first-places', async (req, res) => {
  try {
    const { limit = 100, player } = req.query;
    
    let whereClause = 'WHERE rank = 1';
    let params = [];

    if (player) {
      params.push(`%${player}%`);
      whereClause += ` AND username ILIKE ${params.length}`;
    }

    params.push(parseInt(limit));

    const data = await getRows(`
      SELECT 
        beatmap_id, beatmap_title, artist, difficulty_name, difficulty_rating,
        username, score, accuracy, accuracy_text, mods, pp,
        max_combo, play_date, last_updated
      FROM algeria_top50
      ${whereClause}
      ORDER BY pp DESC
      LIMIT ${params.length}
    `, params);

    res.json({
      success: true,
      data,
      meta: {
        player,
        limit: parseInt(limit),
        totalFirstPlaces: data.length
      }
    });
  } catch (error) {
    console.error('First places error:', error);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

// Get leaderboard statistics
router.get('/stats', async (req, res) => {
  try {
    const cacheKey = 'leaderboard_stats';
    let stats = await cacheService.get(cacheKey);
    
    if (!stats) {
      stats = await leaderboardService.getLeaderboardStats();
      await cacheService.set(cacheKey, stats, 600); // Cache for 10 minutes
    }

    res.json({
      success: true,
      data: stats
    });
  } catch (error) {
    console.error('Leaderboard stats error:', error);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

// Get mod usage statistics
router.get('/mods', async (req, res) => {
  try {
    const { limit = 20 } = req.query;
    
    const data = await getRows(`
      SELECT 
        mods,
        COUNT(*) as usage_count,
        AVG(accuracy) as avg_accuracy,
        AVG(pp) as avg_pp,
        MAX(pp) as max_pp,
        COUNT(DISTINCT username) as unique_players
      FROM algeria_top50
      WHERE mods != 'None' AND mods IS NOT NULL
      GROUP BY mods
      ORDER BY usage_count DESC
      LIMIT $1
    `, [parseInt(limit)]);

    res.json({
      success: true,
      data,
      meta: {
        limit: parseInt(limit)
      }
    });
  } catch (error) {
    console.error('Mod usage error:', error);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

// Get difficulty distribution
router.get('/difficulty-distribution', async (req, res) => {
  try {
    const data = await getRows(`
      SELECT 
        CASE 
          WHEN difficulty_rating >= 8.0 THEN '8.0+'
          WHEN difficulty_rating >= 7.0 THEN '7.0-7.99'
          WHEN difficulty_rating >= 6.0 THEN '6.0-6.99'
          WHEN difficulty_rating >= 5.0 THEN '5.0-5.99'
          WHEN difficulty_rating >= 4.0 THEN '4.0-4.99'
          WHEN difficulty_rating >= 3.0 THEN '3.0-3.99'
          WHEN difficulty_rating >= 2.0 THEN '2.0-2.99'
          ELSE '0.0-1.99'
        END as difficulty_range,
        COUNT(*) as score_count,
        AVG(accuracy) as avg_accuracy,
        AVG(pp) as avg_pp,
        COUNT(DISTINCT username) as unique_players
      FROM algeria_top50
      WHERE difficulty_rating > 0
      GROUP BY 
        CASE 
          WHEN difficulty_rating >= 8.0 THEN '8.0+'
          WHEN difficulty_rating >= 7.0 THEN '7.0-7.99'
          WHEN difficulty_rating >= 6.0 THEN '6.0-6.99'
          WHEN difficulty_rating >= 5.0 THEN '5.0-5.99'
          WHEN difficulty_rating >= 4.0 THEN '4.0-4.99'
          WHEN difficulty_rating >= 3.0 THEN '3.0-3.99'
          WHEN difficulty_rating >= 2.0 THEN '2.0-2.99'
          ELSE '0.0-1.99'
        END
      ORDER BY 
        CASE difficulty_range
          WHEN '0.0-1.99' THEN 1
          WHEN '2.0-2.99' THEN 2
          WHEN '3.0-3.99' THEN 3
          WHEN '4.0-4.99' THEN 4
          WHEN '5.0-5.99' THEN 5
          WHEN '6.0-6.99' THEN 6
          WHEN '7.0-7.99' THEN 7
          WHEN '8.0+' THEN 8
        END
    `);

    res.json({
      success: true,
      data
    });
  } catch (error) {
    console.error('Difficulty distribution error:', error);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

// Get most popular beatmaps
router.get('/popular-beatmaps', async (req, res) => {
  try {
    const { limit = 20 } = req.query;
    
    const data = await getRows(`
      SELECT 
        beatmap_id,
        beatmap_title,
        artist,
        difficulty_name,
        difficulty_rating,
        COUNT(*) as total_scores,
        AVG(accuracy) as avg_accuracy,
        MAX(score) as best_score,
        AVG(pp) as avg_pp,
        COUNT(DISTINCT username) as unique_players
      FROM algeria_top50
      GROUP BY beatmap_id, beatmap_title, artist, difficulty_name, difficulty_rating
      ORDER BY total_scores DESC
      LIMIT $1
    `, [parseInt(limit)]);

    res.json({
      success: true,
      data,
      meta: {
        limit: parseInt(limit)
      }
    });
  } catch (error) {
    console.error('Popular beatmaps error:', error);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

module.exports = router;