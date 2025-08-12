const express = require('express');
const router = express.Router();
const { getRows, getRow } = require('../config/db');
const { cacheService } = require('../services/cache');
const { validateInput } = require('../config/security');
const { getPlayerAchievements } = require('../services/achievements');

// Get player profile
router.get('/:username', 
  validateInput({
    username: { required: true, minLength: 2, maxLength: 15 }
  }),
  async (req, res) => {
    try {
      const { username } = req.params;
      const cacheKey = cacheService.generateKey('player', username);
      
      // Try to get from cache first
      let data = await cacheService.getPlayerCache(username);
      
      if (!data) {
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
          getPlayerAchievements(username),
          getRows(`
            SELECT activity_type, activity_data, timestamp
            FROM player_activity
            WHERE username ILIKE $1
            ORDER BY timestamp DESC
            LIMIT 10
          `, [`%${username}%`])
        ]);
        
        if (!playerStats) {
          return res.status(404).json({ success: false, error: 'Player not found' });
        }
        
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
        
        data = {
          ...playerStats,
          countryRank: parseInt(rankResult.rank),
          recentScores,
          bestScores,
          skillProgression,
          achievements,
          recentActivity: activity
        };

        // Cache the result
        await cacheService.cachePlayer(username, data);
      }
      
      res.json({ success: true, data });
    } catch (error) {
      console.error('Player profile error:', error);
      res.status(500).json({ success: false, error: 'Internal server error' });
    }
  }
);

// Get player statistics summary
router.get('/:username/stats', 
  validateInput({
    username: { required: true, minLength: 2, maxLength: 15 }
  }),
  async (req, res) => {
    try {
      const { username } = req.params;
      
      const stats = await getRow(`
        SELECT 
          username, total_scores, avg_rank, best_score, total_pp, weighted_pp,
          first_places, top_10_places, accuracy_avg, playcount, total_playtime,
          level, global_rank, country_rank, last_seen
        FROM player_stats 
        WHERE username ILIKE $1
      `, [`%${username}%`]);
      
      if (!stats) {
        return res.status(404).json({ success: false, error: 'Player not found' });
      }

      res.json({ success: true, data: stats });
    } catch (error) {
      console.error('Player stats error:', error);
      res.status(500).json({ success: false, error: 'Internal server error' });
    }
  }
);

// Get player recent scores
router.get('/:username/recent', 
  validateInput({
    username: { required: true, minLength: 2, maxLength: 15 }
  }),
  async (req, res) => {
    try {
      const { username } = req.params;
      const { limit = 20 } = req.query;
      
      const scores = await getRows(`
        SELECT 
          beatmap_id, beatmap_title, artist, difficulty_name, difficulty_rating,
          rank, score, accuracy, accuracy_text, mods, pp, max_combo,
          count_300, count_100, count_50, count_miss, play_date, last_updated
        FROM algeria_top50 
        WHERE username ILIKE $1 
        ORDER BY last_updated DESC 
        LIMIT $2
      `, [`%${username}%`, parseInt(limit)]);

      res.json({ success: true, data: scores });
    } catch (error) {
      console.error('Player recent scores error:', error);
      res.status(500).json({ success: false, error: 'Internal server error' });
    }
  }
);

// Get player best scores
router.get('/:username/best', 
  validateInput({
    username: { required: true, minLength: 2, maxLength: 15 }
  }),
  async (req, res) => {
    try {
      const { username } = req.params;
      const { limit = 20 } = req.query;
      
      const scores = await getRows(`
        SELECT 
          beatmap_id, beatmap_title, artist, difficulty_name, difficulty_rating,
          rank, score, accuracy, accuracy_text, mods, pp, max_combo,
          count_300, count_100, count_50, count_miss, play_date, last_updated
        FROM algeria_top50 
        WHERE username ILIKE $1 
        ORDER BY pp DESC 
        LIMIT $2
      `, [`%${username}%`, parseInt(limit)]);

      res.json({ success: true, data: scores });
    } catch (error) {
      console.error('Player best scores error:', error);
      res.status(500).json({ success: false, error: 'Internal server error' });
    }
  }
);

// Get player first place scores
router.get('/:username/firsts', 
  validateInput({
    username: { required: true, minLength: 2, maxLength: 15 }
  }),
  async (req, res) => {
    try {
      const { username } = req.params;
      const { limit = 50 } = req.query;
      
      const scores = await getRows(`
        SELECT 
          beatmap_id, beatmap_title, artist, difficulty_name, difficulty_rating,
          score, accuracy, accuracy_text, mods, pp, max_combo,
          count_300, count_100, count_50, count_miss, play_date, last_updated
        FROM algeria_top50 
        WHERE username ILIKE $1 AND rank = 1
        ORDER BY pp DESC 
        LIMIT $2
      `, [`%${username}%`, parseInt(limit)]);

      res.json({ 
        success: true, 
        data: scores,
        total: scores.length 
      });
    } catch (error) {
      console.error('Player first places error:', error);
      res.status(500).json({ success: false, error: 'Internal server error' });
    }
  }
);

// Get player skills
router.get('/:username/skills', 
  validateInput({
    username: { required: true, minLength: 2, maxLength: 15 }
  }),
  async (req, res) => {
    try {
      const { username } = req.params;
      const { days = 30 } = req.query;
      
      const cutoff = Date.now() - (parseInt(days) * 24 * 60 * 60 * 1000);
      
      const skills = await getRows(`
        SELECT skill_type, skill_value, calculated_at
        FROM skill_tracking 
        WHERE username ILIKE $1 AND calculated_at > $2
        ORDER BY calculated_at DESC
      `, [`%${username}%`, cutoff]);

      // Group skills by type
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

      // Get latest skills
      const latestSkills = {};
      Object.keys(skillProgression).forEach(skillType => {
        const skillData = skillProgression[skillType];
        if (skillData.length > 0) {
          latestSkills[skillType] = skillData[0].value;
        }
      });

      res.json({ 
        success: true, 
        data: {
          latest: latestSkills,
          progression: skillProgression
        }
      });
    } catch (error) {
      console.error('Player skills error:', error);
      res.status(500).json({ success: false, error: 'Internal server error' });
    }
  }
);

// Get player achievements
router.get('/:username/achievements', 
  validateInput({
    username: { required: true, minLength: 2, maxLength: 15 }
  }),
  async (req, res) => {
    try {
      const { username } = req.params;
      
      const achievements = await getPlayerAchievements(username);
      
      // Calculate total points
      const totalPoints = achievements.reduce((sum, achievement) => sum + (achievement.points || 0), 0);
      
      res.json({ 
        success: true, 
        data: {
          achievements,
          totalAchievements: achievements.length,
          totalPoints
        }
      });
    } catch (error) {
      console.error('Player achievements error:', error);
      res.status(500).json({ success: false, error: 'Internal server error' });
    }
  }
);

module.exports = router;