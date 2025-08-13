const { query } = require('../config/db');
const { broadcastToClients } = require('../middleware/websocket');

async function checkPlayerAchievements(username) {
  try {
    const [playerScores, playerStats] = await Promise.all([
      query(`
        SELECT * FROM algeria_top50 
        WHERE username = $1 
        ORDER BY last_updated DESC
      `, [username]),
      query(`
        SELECT * FROM player_stats 
        WHERE username = $1
      `, [username])
    ]);

    if (!playerStats.rows[0]) return [];

    const scores = playerScores.rows;
    const stats = playerStats.rows[0];
    const newAchievements = [];

    // Define achievement checks
    const achievementChecks = [
      {
        name: 'First Steps',
        condition: () => scores.length >= 1,
        description: 'Set your first score'
      },
      {
        name: 'Getting Started',
        condition: () => scores.length >= 10,
        description: 'Set 10 scores'
      },
      {
        name: 'Dedicated Player',
        condition: () => scores.length >= 50,
        description: 'Set 50 scores'
      },
      {
        name: 'Century Club',
        condition: () => scores.length >= 100,
        description: 'Set 100 scores'
      },
      {
        name: 'Score Master',
        condition: () => scores.length >= 250,
        description: 'Set 250 scores'
      },
      {
        name: 'Perfectionist',
        condition: () => scores.some(s => (s.accuracy || 0) >= 100),
        description: 'Get your first SS rank'
      },
      {
        name: 'Accuracy King',
        condition: () => scores.filter(s => (s.accuracy || 0) >= 98).length >= 10,
        description: 'Get 10 scores with 98%+ accuracy'
      },
      {
        name: 'Speed Demon',
        condition: () => scores.some(s => s.mods?.includes('DT')),
        description: 'Get a score with DT mod'
      },
      {
        name: 'Precision Master',
        condition: () => scores.some(s => s.mods?.includes('HR')),
        description: 'Get a score with HR mod'
      },
      {
        name: 'In the Dark',
        condition: () => scores.some(s => s.mods?.includes('HD')),
        description: 'Get a score with HD mod'
      },
      {
        name: 'Mod Variety',
        condition: () => {
          const mods = new Set();
          scores.forEach(s => {
            if (s.mods && s.mods !== 'None') {
              s.mods.split(',').forEach(mod => mods.add(mod.trim()));
            }
          });
          return mods.size >= 5;
        },
        description: 'Use 5 different mod combinations'
      },
      {
        name: 'Top Player',
        condition: () => scores.some(s => s.rank === 1),
        description: 'Reach #1 on any beatmap'
      },
      {
        name: 'Consistent Performer',
        condition: () => scores.filter(s => s.rank <= 10).length >= 25,
        description: 'Get 25 top 10 scores'
      },
      {
        name: 'PP Collector',
        condition: () => (stats.total_pp || 0) >= 1000,
        description: 'Reach 1000 total PP'
      },
      {
        name: 'PP Hunter',
        condition: () => (stats.weighted_pp || 0) >= 2000,
        description: 'Reach 2000 weighted PP'
      },
      {
        name: 'Elite Player',
        condition: () => (stats.weighted_pp || 0) >= 5000,
        description: 'Reach 5000 weighted PP'
      },
      {
        name: 'High Roller',
        condition: () => scores.some(s => (s.score || 0) >= 10000000),
        description: 'Get a 10M+ score'
      },
      {
        name: 'Combo King',
        condition: () => scores.some(s => (s.max_combo || 0) >= 1000),
        description: 'Achieve 1000+ combo'
      },
      {
        name: 'Difficulty Climber',
        condition: () => scores.some(s => (s.difficulty_rating || 0) >= 6.0),
        description: 'Complete a 6+ star map'
      },
      {
        name: 'Extreme Challenge',
        condition: () => scores.some(s => (s.difficulty_rating || 0) >= 8.0),
        description: 'Complete an 8+ star map'
      }
    ];

    // Check each achievement
    for (const check of achievementChecks) {
      if (check.condition()) {
        // Get achievement ID
        const achievement = await query(`
          SELECT id FROM achievements WHERE name = $1
        `, [check.name]);

        if (achievement.rows[0]) {
          // Check if player already has this achievement
          const existing = await query(`
            SELECT id FROM player_achievements 
            WHERE username = $1 AND achievement_id = $2
          `, [username, achievement.rows[0].id]);

          if (existing.rows.length === 0) {
            // Award the achievement
            await query(`
              INSERT INTO player_achievements (username, achievement_id)
              VALUES ($1, $2)
            `, [username, achievement.rows[0].id]);

            newAchievements.push({
              name: check.name,
              description: check.description,
              unlockedAt: new Date()
            });
          }
        }
      }
    }

    return newAchievements;

  } catch (err) {
    console.error(`❌ Achievement check failed for ${username}:`, err.message);
    return [];
  }
}

async function updateAchievementsJob() {
  console.log('🏆 Updating player achievements...');
  
  try {
    // Get players who need achievement checks (recently active)

const cutoffDate = Math.floor(Date.now() / 1000);
const players = await query(`
  SELECT DISTINCT username 
  FROM player_stats 
  WHERE is_active = true 
  AND (
    last_seen > $1 OR 
    last_calculated > $1 OR
    last_seen IS NULL
  )
  ORDER BY last_seen DESC
  LIMIT 50
`, [cutoffDate]);

    let totalNewAchievements = 0;
    let processedPlayers = 0;

    for (const player of players.rows) {
      try {
        const newAchievements = await checkPlayerAchievements(player.username);
        
        if (newAchievements.length > 0) {
          console.log(`🏆 ${player.username} unlocked ${newAchievements.length} achievements:`, 
                     newAchievements.map(a => a.name).join(', '));
          
          // Broadcast new achievements
          broadcastToClients({
            type: 'new_achievements',
            username: player.username,
            achievements: newAchievements,
            timestamp: Date.now()
          });

          totalNewAchievements += newAchievements.length;
        }

        processedPlayers++;

      } catch (err) {
        console.error(`❌ Failed to check achievements for ${player.username}:`, err.message);
      }
    }

    console.log(`✅ Achievement update completed: ${totalNewAchievements} new achievements for ${processedPlayers} players`);

    return {
      processedPlayers,
      totalNewAchievements
    };

  } catch (err) {
    console.error('❌ Achievement update failed:', err.message);
    throw err;
  }
}

module.exports = updateAchievementsJob;