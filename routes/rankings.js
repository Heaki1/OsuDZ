const express = require('express');
const router = express.Router();
const { getRows } = require('../config/db');
const { cacheService } = require('../services/cache');
const { validateInput } = require('../config/security');

// Get player rankings
router.get('/', async (req, res) => {
  try {
    const { 
      sort = 'weighted_pp', 
      limit = 50, 
      offset = 0, 
      timeframe = 'all',
      minScores = 5 
    } = req.query;
    
    // Try cache first
    const cacheParams = { sort, limit, offset, timeframe, minScores };
    let data = await cacheService.getRankingsCache(cacheParams);
    
    if (!data) {
      const allowedSort = ['weighted_pp', 'total_pp', 'first_places', 'avg_rank', 'accuracy_avg', 'total_scores'];
      const sortColumn = allowedSort.includes(sort.toLowerCase()) ? sort.toLowerCase() : 'weighted_pp';
      const sortOrder = sort === 'avg_rank' ? 'ASC' : 'DESC';
      
      let whereClause = `WHERE total_scores >= $1 AND is_active = true`;
      let params = [parseInt(minScores)];
      let paramCount = 1;
      
      if (timeframe !== 'all') {
        const timeRanges = {
          '24h': 24 * 60 * 60 * 1000,
          '7d': 7 * 24 * 60 * 60 * 1000,
          '30d': 30 * 24 * 60 * 60 * 1000
        };
        
        const cutoff = Date.now() - (timeRanges[timeframe] || 0);
        if (cutoff > 0) {
          whereClause += ` AND last_calculated >= $${++paramCount}`;
          params.push(cutoff);
        }
      }
      
      params.push(parseInt(limit), parseInt(offset));
      
      data = await getRows(`
        SELECT 
          username, user_id, total_scores, avg_rank, best_score, total_pp, weighted_pp,
          first_places, top_10_places, accuracy_avg, playcount, total_playtime,
          level, global_rank, country_rank, last_seen, avatar_url,
          ROW_NUMBER() OVER (ORDER BY ${sortColumn} ${sortOrder}) as rank
        FROM player_stats 
        ${whereClause}
        ORDER BY ${sortColumn} ${sortOrder}
        LIMIT $${++paramCount} OFFSET $${++paramCount}
      `, params);

      // Cache for 5 minutes
      await cacheService.cacheRankings(cacheParams, data, 300);
    }
    
    res.json({
      success: true,
      data,
      meta: {
        sort,
        limit: parseInt(limit),
        offset: parseInt(offset),
        timeframe,
        minScores: parseInt(minScores),
        hasMore: data.length === parseInt(limit)
      }
    });
  } catch (error) {
    console.error('Rankings error:', error);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

// Get top performers by specific metric
router.get('/top/:metric', 
  validateInput({
    metric: { 
      required: true, 
      enum: ['pp', 'accuracy', 'first_places', 'total_scores', 'playtime'] 
    }
  }),
  async (req, res) => {
    try {
      const { metric } = req.params;
      const { limit = 20 } = req.query;

      const metricMapping = {
        pp: 'weighted_pp',
        accuracy: 'accuracy_avg',
        first_places: 'first_places',
        total_scores: 'total_scores',
        playtime: 'total_playtime'
      };

      const column = metricMapping[metric];
      const order = metric === 'accuracy' ? 'DESC' : 'DESC';

      const data = await getRows(`
        SELECT 
          username, user_id, weighted_pp, accuracy_avg, first_places, 
          total_scores, total_playtime, country_rank, avatar_url,
          ${column} as metric_value
        FROM player_stats
        WHERE is_active = true AND ${column} > 0
        ORDER BY ${column} ${order}
        LIMIT $1
      `, [parseInt(limit)]);

      res.json({
        success: true,
        data,
        meta: {
          metric,
          limit: parseInt(limit)
        }
      });
    } catch (error) {
      console.error('Top performers error:', error);
      res.status(500).json({ success: false, error: 'Internal server error' });
    }
  }
);

// Get rankings by skill
router.get('/skills/:skillType', 
  validateInput({
    skillType: { 
      required: true, 
      enum: ['aim', 'speed', 'accuracy', 'reading', 'consistency'] 
    }
  }),
  async (req, res) => {
    try {
      const { skillType } = req.params;
      const { limit = 20 } = req.query;

      const data = await getRows(`
        WITH recent_skills AS (
          SELECT DISTINCT ON (username) 
                 username, skill_value, calculated_at
          FROM skill_tracking 
          WHERE skill_type = $1
          ORDER BY username, calculated_at DESC
        )
        SELECT 
          rs.username, 
          rs.skill_value,
          ps.weighted_pp, 
          ps.country_rank, 
          ps.avatar_url,
          ROW_NUMBER() OVER (ORDER BY rs.skill_value DESC) as skill_rank
        FROM recent_skills rs
        JOIN player_stats ps ON rs.username = ps.username
        WHERE ps.is_active = true
        ORDER BY rs.skill_value DESC
        LIMIT $2
      `, [skillType, parseInt(limit)]);

      res.json({
        success: true,
        data,
        meta: {
          skillType,
          limit: parseInt(limit)
        }
      });
    } catch (error) {
      console.error('Skill rankings error:', error);
      res.status(500).json({ success: false, error: 'Internal server error' });
    }
  }
);

// Get country ranking comparison
router.get('/country-comparison', async (req, res) => {
  try {
    const { limit = 100 } = req.query;

    const data = await getRows(`
      SELECT 
        username, 
        country_rank, 
        global_rank, 
        weighted_pp, 
        accuracy_avg,
        first_places,
        total_scores,
        avatar_url
      FROM player_stats
      WHERE is_active = true AND country_rank > 0
      ORDER BY country_rank ASC
      LIMIT $1
    `, [parseInt(limit)]);

    res.json({
      success: true,
      data,
      meta: {
        limit: parseInt(limit),
        country: 'DZ'
      }
    });
  } catch (error) {
    console.error('Country comparison error:', error);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

// Get ranking changes/trends
router.get('/trends', async (req, res) => {
  try {
    const { days = 7, limit = 50 } = req.query;
    
    // This would require historical ranking data
    // For now, return recent activity as a proxy for trends
    const cutoff = Date.now() - (parseInt(days) * 24 * 60 * 60 * 1000);
    
    const data = await getRows(`
      SELECT 
        ps.username,
        ps.weighted_pp,
        ps.country_rank,
        ps.first_places,
        ps.last_calculated,
        COUNT(ats.username) as recent_scores
      FROM player_stats ps
      LEFT JOIN algeria_top50 ats ON ps.username = ats.username AND ats.last_updated > $1
      WHERE ps.is_active = true
      GROUP BY ps.username, ps.weighted_pp, ps.country_rank, ps.first_places, ps.last_calculated
      HAVING COUNT(ats.username) > 0
      ORDER BY recent_scores DESC, ps.weighted_pp DESC
      LIMIT $2
    `, [cutoff, parseInt(limit)]);

    res.json({
      success: true,
      data,
      meta: {
        days: parseInt(days),
        limit: parseInt(limit),
        period: `Last ${days} days`
      }
    });
  } catch (error) {
    console.error('Ranking trends error:', error);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

// Get milestone achievements in rankings
router.get('/milestones', async (req, res) => {
  try {
    const data = await getRows(`
      SELECT 
        'top_100_pp' as milestone,
        COUNT(*) as players_count
      FROM player_stats 
      WHERE weighted_pp >= 100 AND is_active = true
      
      UNION ALL
      
      SELECT 
        'top_1000_pp' as milestone,
        COUNT(*) as players_count
      FROM player_stats 
      WHERE weighted_pp >= 1000 AND is_active = true
      
      UNION ALL
      
      SELECT 
        'high_accuracy' as milestone,
        COUNT(*) as players_count
      FROM player_stats 
      WHERE accuracy_avg >= 0.95 AND is_active = true
      
      UNION ALL
      
      SELECT 
        'first_place_holders' as milestone,
        COUNT(*) as players_count
      FROM player_stats 
      WHERE first_places > 0 AND is_active = true
      
      UNION ALL
      
      SELECT 
        'prolific_players' as milestone,
        COUNT(*) as players_count
      FROM player_stats 
      WHERE total_scores >= 100 AND is_active = true
    `);

    res.json({
      success: true,
      data
    });
  } catch (error) {
    console.error('Milestones error:', error);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

module.exports = router;