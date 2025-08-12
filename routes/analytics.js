const express = require('express');
const router = express.Router();
const { getRows, getRow, getCached, getCacheKey } = require('../config/db');

// Analytics overview endpoint
router.get('/overview', async (req, res) => {
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
    }, 900);
    
    res.json({ success: true, data });
  } catch (error) {
    console.error('Analytics overview error:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch analytics data' });
  }
});

// Player activity trends
router.get('/activity-trends', async (req, res) => {
  try {
    const { days = 30 } = req.query;
    const cacheKey = getCacheKey('analytics', 'activity-trends', days);
    
    const data = await getCached(cacheKey, async () => {
      const cutoff = Date.now() - (parseInt(days) * 24 * 60 * 60 * 1000);
      
      return await getRows(`
        SELECT 
          DATE(to_timestamp(last_updated / 1000)) as date,
          COUNT(DISTINCT username) as active_players,
          COUNT(*) as scores_set,
          AVG(accuracy) as avg_accuracy,
          SUM(pp) as total_pp
        FROM algeria_top50
        WHERE last_updated > $1
        GROUP BY DATE(to_timestamp(last_updated / 1000))
        ORDER BY date ASC
      `, [cutoff]);
    }, 300);
    
    res.json({ success: true, data });
  } catch (error) {
    console.error('Activity trends error:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch activity trends' });
  }
});

// Beatmap popularity
router.get('/beatmap-popularity', async (req, res) => {
  try {
    const { limit = 20 } = req.query;
    const cacheKey = getCacheKey('analytics', 'beatmap-popularity', limit);
    
    const data = await getCached(cacheKey, async () => {
      return await getRows(`
        SELECT 
          a.beatmap_id,
          a.beatmap_title,
          a.artist,
          a.difficulty_name,
          a.difficulty_rating,
          COUNT(DISTINCT a.username) as player_count,
          AVG(a.accuracy) as avg_accuracy,
          MAX(a.pp) as highest_pp,
          MIN(a.rank) as best_rank
        FROM algeria_top50 a
        GROUP BY a.beatmap_id, a.beatmap_title, a.artist, a.difficulty_name, a.difficulty_rating
        ORDER BY player_count DESC, highest_pp DESC
        LIMIT $1
      `, [parseInt(limit)]);
    }, 600);
    
    res.json({ success: true, data });
  } catch (error) {
    console.error('Beatmap popularity error:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch beatmap popularity' });
  }
});

// Skill analysis
router.get('/skill-analysis', async (req, res) => {
  try {
    const { skill_type, days = 30 } = req.query;
    const cacheKey = getCacheKey('analytics', 'skill-analysis', skill_type, days);
    
    const data = await getCached(cacheKey, async () => {
      const cutoff = Date.now() - (parseInt(days) * 24 * 60 * 60 * 1000);
      let whereClause = 'WHERE calculated_at > $1';
      let params = [cutoff];
      
      if (skill_type) {
        whereClause += ' AND skill_type = $2';
        params.push(skill_type);
      }
      
      return await getRows(`
        SELECT 
          skill_type,
          AVG(skill_value) as avg_skill,
          MIN(skill_value) as min_skill,
          MAX(skill_value) as max_skill,
          COUNT(DISTINCT username) as player_count
        FROM skill_tracking
        ${whereClause}
        GROUP BY skill_type
        ORDER BY avg_skill DESC
      `, params);
    }, 300);
    
    res.json({ success: true, data });
  } catch (error) {
    console.error('Skill analysis error:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch skill analysis' });
  }
});

// Mod usage statistics
router.get('/mod-usage', async (req, res) => {
  try {
    const cacheKey = 'analytics:mod-usage';
    
    const data = await getCached(cacheKey, async () => {
      const modStats = await getRows(`
        SELECT 
          mods,
          COUNT(*) as usage_count,
          COUNT(DISTINCT username) as unique_players,
          AVG(accuracy) as avg_accuracy,
          AVG(pp) as avg_pp,
          AVG(difficulty_rating) as avg_difficulty
        FROM algeria_top50
        WHERE mods != 'None' AND mods IS NOT NULL
        GROUP BY mods
        ORDER BY usage_count DESC
        LIMIT 15
      `);
      
      // Calculate individual mod stats
      const individualMods = {};
      modStats.forEach(stat => {
        const mods = stat.mods.split(',');
        mods.forEach(mod => {
          if (!individualMods[mod]) {
            individualMods[mod] = {
              usage_count: 0,
              unique_players: new Set(),
              total_accuracy: 0,
              total_pp: 0,
              total_difficulty: 0,
              count: 0
            };
          }
          
          individualMods[mod].usage_count += parseInt(stat.usage_count);
          individualMods[mod].total_accuracy += parseFloat(stat.avg_accuracy) * parseInt(stat.usage_count);
          individualMods[mod].total_pp += parseFloat(stat.avg_pp) * parseInt(stat.usage_count);
          individualMods[mod].total_difficulty += parseFloat(stat.avg_difficulty) * parseInt(stat.usage_count);
          individualMods[mod].count += parseInt(stat.usage_count);
        });
      });
      
      const processedMods = Object.entries(individualMods).map(([mod, data]) => ({
        mod,
        usage_count: data.usage_count,
        avg_accuracy: data.total_accuracy / data.count,
        avg_pp: data.total_pp / data.count,
        avg_difficulty: data.total_difficulty / data.count
      })).sort((a, b) => b.usage_count - a.usage_count);
      
      return {
        combinedMods: modStats,
        individualMods: processedMods
      };
    }, 1800);
    
    res.json({ success: true, data });
  } catch (error) {
    console.error('Mod usage error:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch mod usage statistics' });
  }
});

// Performance distribution
router.get('/performance-distribution', async (req, res) => {
  try {
    const cacheKey = 'analytics:performance-distribution';
    
    const data = await getCached(cacheKey, async () => {
      const [ppDistribution, accuracyDistribution, rankDistribution] = await Promise.all([
        getRows(`
          SELECT 
            CASE 
              WHEN pp = 0 THEN '0 PP'
              WHEN pp < 50 THEN '1-49 PP'
              WHEN pp < 100 THEN '50-99 PP'
              WHEN pp < 200 THEN '100-199 PP'
              WHEN pp < 300 THEN '200-299 PP'
              WHEN pp < 400 THEN '300-399 PP'
              WHEN pp >= 400 THEN '400+ PP'
            END as pp_range,
            COUNT(*) as count
          FROM algeria_top50
          GROUP BY pp_range
          ORDER BY MIN(pp) ASC
        `),
        getRows(`
          SELECT 
            CASE 
              WHEN accuracy < 0.80 THEN '<80%'
              WHEN accuracy < 0.85 THEN '80-85%'
              WHEN accuracy < 0.90 THEN '85-90%'
              WHEN accuracy < 0.95 THEN '90-95%'
              WHEN accuracy < 0.98 THEN '95-98%'
              WHEN accuracy < 1.00 THEN '98-100%'
              WHEN accuracy = 1.00 THEN '100%'
            END as accuracy_range,
            COUNT(*) as count
          FROM algeria_top50
          WHERE accuracy > 0
          GROUP BY accuracy_range
          ORDER BY MIN(accuracy) ASC
        `),
        getRows(`
          SELECT 
            CASE 
              WHEN rank = 1 THEN '#1'
              WHEN rank <= 3 THEN '#2-3'
              WHEN rank <= 5 THEN '#4-5'
              WHEN rank <= 10 THEN '#6-10'
              WHEN rank <= 25 THEN '#11-25'
              WHEN rank > 25 THEN '#26+'
            END as rank_range,
            COUNT(*) as count
          FROM algeria_top50
          GROUP BY rank_range
          ORDER BY MIN(rank) ASC
        `)
      ]);
      
      return {
        ppDistribution,
        accuracyDistribution,
        rankDistribution
      };
    }, 1200);
    
    res.json({ success: true, data });
  } catch (error) {
    console.error('Performance distribution error:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch performance distribution' });
  }
});

module.exports = router;