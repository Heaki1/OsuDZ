const express = require('express');
const router = express.Router();
const { getRows, getRow, getCached, getCacheKey } = require('../config/db');
const { validateInput } = require('../middleware/validation');

// Player comparison endpoint
router.get('/:username1/:username2', 
  validateInput({
    username1: { required: true, minLength: 2 },
    username2: { required: true, minLength: 2 }
  }),
  async (req, res) => {
    try {
      const { username1, username2 } = req.params;
      const cacheKey = getCacheKey('compare', username1, username2);
      
      const data = await getCached(cacheKey, async () => {
        const [player1, player2] = await Promise.all([
          getRow(`SELECT * FROM player_stats WHERE username ILIKE $1`, [`%${username1}%`]),
          getRow(`SELECT * FROM player_stats WHERE username ILIKE $1`, [`%${username2}%`])
        ]);
        
        if (!player1 || !player2) return null;
        
        const [skills1, skills2, scores1, scores2, achievements1, achievements2] = await Promise.all([
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
          `, [`%${username2}%`]),
          getRows(`
            SELECT * FROM algeria_top50 
            WHERE username ILIKE $1 
            ORDER BY pp DESC 
            LIMIT 10
          `, [`%${username1}%`]),
          getRows(`
            SELECT * FROM algeria_top50 
            WHERE username ILIKE $1 
            ORDER BY pp DESC 
            LIMIT 10
          `, [`%${username2}%`]),
          getRows(`
            SELECT COUNT(*) as achievement_count
            FROM player_achievements
            WHERE username ILIKE $1
          `, [`%${username1}%`]),
          getRows(`
            SELECT COUNT(*) as achievement_count
            FROM player_achievements
            WHERE username ILIKE $1
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
        
        // Head-to-head comparison on same beatmaps
        const headToHead = await getRows(`
          SELECT 
            p1.beatmap_id,
            p1.beatmap_title,
            p1.score as player1_score,
            p1.rank as player1_rank,
            p1.accuracy as player1_accuracy,
            p1.pp as player1_pp,
            p2.score as player2_score,
            p2.rank as player2_rank,
            p2.accuracy as player2_accuracy,
            p2.pp as player2_pp,
            CASE WHEN p1.rank < p2.rank THEN $1 ELSE $2 END as winner
          FROM algeria_top50 p1
          INNER JOIN algeria_top50 p2 ON p1.beatmap_id = p2.beatmap_id
          WHERE p1.username ILIKE $3 AND p2.username ILIKE $4
          ORDER BY GREATEST(p1.pp, p2.pp) DESC
          LIMIT 20
        `, [username1, username2, `%${username1}%`, `%${username2}%`]);
        
        const headToHeadStats = {
          totalMaps: headToHead.length,
          player1Wins: headToHead.filter(h => h.winner === username1).length,
          player2Wins: headToHead.filter(h => h.winner === username2).length,
          maps: headToHead
        };
        
        return {
          player1: {
            ...player1,
            topScores: scores1,
            achievementCount: achievements1[0]?.achievement_count || 0
          },
          player2: {
            ...player2,
            topScores: scores2,
            achievementCount: achievements2[0]?.achievement_count || 0
          },
          skillComparison,
          headToHead: headToHeadStats,
          statComparison: {
            totalPP: {
              player1: player1.total_pp || 0,
              player2: player2.total_pp || 0,
              difference: (player1.total_pp || 0) - (player2.total_pp || 0),
              percentDiff: player2.total_pp ? (((player1.total_pp || 0) - (player2.total_pp || 0)) / (player2.total_pp || 1)) * 100 : 0
            },
            weightedPP: {
              player1: player1.weighted_pp || 0,
              player2: player2.weighted_pp || 0,
              difference: (player1.weighted_pp || 0) - (player2.weighted_pp || 0),
              percentDiff: player2.weighted_pp ? (((player1.weighted_pp || 0) - (player2.weighted_pp || 0)) / (player2.weighted_pp || 1)) * 100 : 0
            },
            accuracy: {
              player1: player1.accuracy_avg || 0,
              player2: player2.accuracy_avg || 0,
              difference: (player1.accuracy_avg || 0) - (player2.accuracy_avg || 0),
              percentDiff: player2.accuracy_avg ? (((player1.accuracy_avg || 0) - (player2.accuracy_avg || 0)) / (player2.accuracy_avg || 1)) * 100 : 0
            },
            firstPlaces: {
              player1: player1.first_places || 0,
              player2: player2.first_places || 0,
              difference: (player1.first_places || 0) - (player2.first_places || 0)
            },
            totalScores: {
              player1: player1.total_scores || 0,
              player2: player2.total_scores || 0,
              difference: (player1.total_scores || 0) - (player2.total_scores || 0)
            },
            avgRank: {
              player1: player1.avg_rank || 0,
              player2: player2.avg_rank || 0,
              difference: (player1.avg_rank || 0) - (player2.avg_rank || 0)
            }
          }
        };
      }, 600);
      
      if (!data) {
        return res.status(404).json({ success: false, error: 'One or both players not found' });
      }
      
      res.json({ success: true, data });
    } catch (error) {
      console.error('Player comparison error:', error);
      res.status(500).json({ success: false, error: 'Failed to compare players' });
    }
  }
);

// Multiple player comparison (up to 4 players)
router.post('/multiple', 
  validateInput({
    usernames: { required: true }
  }),
  async (req, res) => {
    try {
      const { usernames } = req.body;
      
      if (!Array.isArray(usernames) || usernames.length < 2 || usernames.length > 4) {
        return res.status(400).json({ 
          success: false, 
          error: 'Please provide 2-4 usernames for comparison' 
        });
      }
      
      const cacheKey = getCacheKey('compare', 'multiple', ...usernames.sort());
      
      const data = await getCached(cacheKey, async () => {
        const players = await Promise.all(
          usernames.map(username => 
            getRow(`SELECT * FROM player_stats WHERE username ILIKE $1`, [`%${username}%`])
          )
        );
        
        const missingPlayers = players.map((player, index) => 
          !player ? usernames[index] : null
        ).filter(Boolean);
        
        if (missingPlayers.length > 0) {
          throw new Error(`Players not found: ${missingPlayers.join(', ')}`);
        }
        
        // Get skills for all players
        const allSkills = await Promise.all(
          usernames.map(username =>
            getRows(`
              SELECT skill_type, AVG(skill_value) as avg_skill
              FROM skill_tracking 
              WHERE username ILIKE $1
              GROUP BY skill_type
            `, [`%${username}%`])
          )
        );
        
        // Get top scores for all players
        const allTopScores = await Promise.all(
          usernames.map(username =>
            getRows(`
              SELECT * FROM algeria_top50 
              WHERE username ILIKE $1 
              ORDER BY pp DESC 
              LIMIT 5
            `, [`%${username}%`])
          )
        );
        
        const comparisonData = {
          players: players.map((player, index) => ({
            ...player,
            topScores: allTopScores[index],
            skills: allSkills[index].reduce((acc, skill) => {
              acc[skill.skill_type] = parseFloat(skill.avg_skill);
              return acc;
            }, {})
          })),
          rankings: {
            byWeightedPP: players.map((p, i) => ({ ...p, index: i }))
              .sort((a, b) => (b.weighted_pp || 0) - (a.weighted_pp || 0)),
            byAccuracy: players.map((p, i) => ({ ...p, index: i }))
              .sort((a, b) => (b.accuracy_avg || 0) - (a.accuracy_avg || 0)),
            byFirstPlaces: players.map((p, i) => ({ ...p, index: i }))
              .sort((a, b) => (b.first_places || 0) - (a.first_places || 0))
          }
        };
        
        return comparisonData;
      }, 300);
      
      res.json({ success: true, data });
    } catch (error) {
      console.error('Multiple player comparison error:', error);
      res.status(500).json({ 
        success: false, 
        error: error.message || 'Failed to compare players' 
      });
    }
  }
);

// Compare players on specific beatmap
router.get('/beatmap/:beatmapId/:username1/:username2', async (req, res) => {
  try {
    const { beatmapId, username1, username2 } = req.params;
    const cacheKey = getCacheKey('compare', 'beatmap', beatmapId, username1, username2);
    
    const data = await getCached(cacheKey, async () => {
      const [score1, score2, allScores] = await Promise.all([
        getRow(`
          SELECT * FROM algeria_top50 
          WHERE beatmap_id = $1 AND username ILIKE $2
        `, [beatmapId, `%${username1}%`]),
        getRow(`
          SELECT * FROM algeria_top50 
          WHERE beatmap_id = $1 AND username ILIKE $2
        `, [beatmapId, `%${username2}%`]),
        getRows(`
          SELECT username, rank, score, accuracy, pp, mods
          FROM algeria_top50 
          WHERE beatmap_id = $1
          ORDER BY rank ASC
          LIMIT 50
        `, [beatmapId])
      ]);
      
      if (!score1 && !score2) {
        return { hasScores: false, message: 'Neither player has played this beatmap' };
      }
      
      return {
        hasScores: true,
        beatmapId,
        player1Score: score1,
        player2Score: score2,
        leaderboard: allScores,
        comparison: score1 && score2 ? {
          scoreDiff: score1.score - score2.score,
          rankDiff: score1.rank - score2.rank,
          accuracyDiff: score1.accuracy - score2.accuracy,
          ppDiff: (score1.pp || 0) - (score2.pp || 0),
          winner: score1.rank < score2.rank ? username1 : username2
        } : null
      };
    }, 300);
    
    res.json({ success: true, data });
  } catch (error) {
    console.error('Beatmap comparison error:', error);
    res.status(500).json({ success: false, error: 'Failed to compare on beatmap' });
  }
});

module.exports = router;