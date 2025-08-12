const { query, getRows, getRow } = require('../config/db');

// Default achievements data
const defaultAchievements = [
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
  { name: 'Combo Master', description: 'Get a 1000+ combo score', category: 'performance', icon: '🔥', points: 75 },
  { name: 'Mod Master', description: 'Use 5 different mods', category: 'mods', icon: '🎮', points: 50 },
  { name: 'Rising Star', description: 'Reach top 100 in country ranking', category: 'ranking', icon: '⭐', points: 100 },
  { name: 'No Fail', description: 'Complete a 5+ star map without failing', category: 'performance', icon: '💪', points: 60 },
  { name: 'Accuracy Elite', description: 'Maintain 95%+ average accuracy', category: 'accuracy', icon: '🎪', points: 80 }
];

// Insert default achievements into database
async function insertDefaultAchievements() {
  try {
    for (const achievement of defaultAchievements) {
      await query(`
        INSERT INTO achievements (name, description, category, icon, points)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (name) DO NOTHING
      `, [achievement.name, achievement.description, achievement.category, achievement.icon, achievement.points]);
    }
    console.log('✅ Default achievements initialized');
  } catch (err) {
    console.error('❌ Failed to insert default achievements:', err.message);
  }
}

// Check achievements for a player
async function checkAchievements(username) {
  try {
    const playerScores = await getRows(`
      SELECT * FROM algeria_top50 WHERE username = $1 ORDER BY last_updated DESC
    `, [username]);
    
    const playerStats = await getRow(`
      SELECT * FROM player_stats WHERE username = $1
    `, [username]);
    
    if (!playerStats) return [];

    const unlockedAchievements = [];

    // Get unique mods used by player
    const uniqueMods = [...new Set(playerScores.map(s => s.mods).filter(m => m && m !== 'None'))];

    const achievementChecks = [
      { 
        name: 'First Steps', 
        condition: () => playerScores.length >= 1 
      },
      { 
        name: 'Century Club', 
        condition: () => playerScores.length >= 100 
      },
      { 
        name: 'PP Collector', 
        condition: () => (playerStats.total_pp || 0) >= 1000 
      },
      { 
        name: 'Perfectionist', 
        condition: () => playerScores.some(s => (s.accuracy || 0) >= 1.0) 
      },
      { 
        name: 'Speed Demon', 
        condition: () => playerScores.some(s => s.mods?.includes('DT')) 
      },
      { 
        name: 'Precision Master', 
        condition: () => playerScores.some(s => s.mods?.includes('HR')) 
      },
      { 
        name: 'In the Dark', 
        condition: () => playerScores.some(s => s.mods?.includes('HD')) 
      },
      { 
        name: 'Top Player', 
        condition: () => playerScores.some(s => s.rank === 1) 
      },
      { 
        name: 'Dedication', 
        condition: () => (playerStats.total_playtime || 0) >= 360000 
      },
      {
        name: 'Combo Master',
        condition: () => playerScores.some(s => (s.max_combo || 0) >= 1000)
      },
      {
        name: 'Mod Master',
        condition: () => uniqueMods.length >= 5
      },
      {
        name: 'Rising Star',
        condition: () => (playerStats.country_rank || 999999) <= 100
      },
      {
        name: 'No Fail',
        condition: () => playerScores.some(s => (s.difficulty_rating || 0) >= 5.0 && (s.count_miss || 0) === 0)
      },
      {
        name: 'Accuracy Elite',
        condition: () => (playerStats.accuracy_avg || 0) >= 0.95
      },
      {
        name: 'Consistency King',
        condition: () => checkConsecutiveDays(playerScores)
      }
    ];

    for (const check of achievementChecks) {
      if (check.condition()) {
        const achievement = await getRow(`SELECT id FROM achievements WHERE name = $1`, [check.name]);
        if (achievement) {
          // Check if player already has this achievement
          const existingAchievement = await getRow(`
            SELECT id FROM player_achievements 
            WHERE username = $1 AND achievement_id = $2
          `, [username, achievement.id]);

          if (!existingAchievement) {
            await query(`
              INSERT INTO player_achievements (username, achievement_id)
              VALUES ($1, $2)
            `, [username, achievement.id]);

            unlockedAchievements.push(check.name);
            console.log(`🏆 ${username} unlocked achievement: ${check.name}`);
          }
        }
      }
    }

    return unlockedAchievements;
  } catch (err) {
    console.error('Achievement check failed:', err.message);
    return [];
  }
}

// Helper function to check consecutive days
function checkConsecutiveDays(playerScores, requiredDays = 5) {
  if (playerScores.length < requiredDays) return false;

  const scoreDates = playerScores
    .map(s => new Date(s.last_updated).toDateString())
    .filter((date, index, array) => array.indexOf(date) === index)
    .sort();

  let consecutiveDays = 1;
  let maxConsecutiveDays = 1;

  for (let i = 1; i < scoreDates.length; i++) {
    const currentDate = new Date(scoreDates[i]);
    const previousDate = new Date(scoreDates[i - 1]);
    const daysDiff = (currentDate - previousDate) / (1000 * 60 * 60 * 24);

    if (daysDiff === 1) {
      consecutiveDays++;
      maxConsecutiveDays = Math.max(maxConsecutiveDays, consecutiveDays);
    } else {
      consecutiveDays = 1;
    }
  }

  return maxConsecutiveDays >= requiredDays;
}

// Get player achievements
async function getPlayerAchievements(username) {
  try {
    return await getRows(`
      SELECT a.name, a.description, a.icon, a.points, a.category, pa.unlocked_at
      FROM player_achievements pa
      JOIN achievements a ON pa.achievement_id = a.id
      WHERE pa.username = $1
      ORDER BY pa.unlocked_at DESC
    `, [username]);
  } catch (err) {
    console.error('Failed to get player achievements:', err.message);
    return [];
  }
}

// Get achievement statistics
async function getAchievementStatistics() {
  try {
    const stats = await getRows(`
      SELECT 
        a.name,
        a.description,
        a.category,
        a.points,
        COUNT(pa.id) as unlock_count,
        (COUNT(pa.id)::float / (SELECT COUNT(*) FROM player_stats WHERE is_active = true)) * 100 as unlock_percentage
      FROM achievements a
      LEFT JOIN player_achievements pa ON a.id = pa.achievement_id
      GROUP BY a.id, a.name, a.description, a.category, a.points
      ORDER BY unlock_count DESC
    `);

    return stats.map(stat => ({
      ...stat,
      unlockCount: parseInt(stat.unlock_count),
      unlockPercentage: parseFloat(stat.unlock_percentage) || 0
    }));
  } catch (err) {
    console.error('Failed to get achievement statistics:', err.message);
    return [];
  }
}

// Get rarest achievements
async function getRarestAchievements(limit = 5) {
  try {
    return await getRows(`
      SELECT 
        a.name,
        a.description,
        a.icon,
        a.points,
        COUNT(pa.id) as unlock_count
      FROM achievements a
      LEFT JOIN player_achievements pa ON a.id = pa.achievement_id
      GROUP BY a.id, a.name, a.description, a.icon, a.points
      ORDER BY unlock_count ASC, a.points DESC
      LIMIT $1
    `, [limit]);
  } catch (err) {
    console.error('Failed to get rarest achievements:', err.message);
    return [];
  }
}

// Get achievement leaderboard (most points)
async function getAchievementLeaderboard(limit = 10) {
  try {
    return await getRows(`
      SELECT 
        pa.username,
        SUM(a.points) as total_points,
        COUNT(pa.id) as total_achievements,
        ps.weighted_pp,
        ps.country_rank
      FROM player_achievements pa
      JOIN achievements a ON pa.achievement_id = a.id
      JOIN player_stats ps ON pa.username = ps.username
      WHERE ps.is_active = true
      GROUP BY pa.username, ps.weighted_pp, ps.country_rank
      ORDER BY total_points DESC, total_achievements DESC
      LIMIT $1
    `, [limit]);
  } catch (err) {
    console.error('Failed to get achievement leaderboard:', err.message);
    return [];
  }
}

// Add custom achievement (admin function)
async function addAchievement(name, description, category, icon, points) {
  try {
    const result = await query(`
      INSERT INTO achievements (name, description, category, icon, points)
      VALUES ($1, $2, $3, $4, $5)
      RETURNING id
    `, [name, description, category, icon, points]);

    console.log(`✅ Added new achievement: ${name}`);
    return result.rows[0].id;
  } catch (err) {
    console.error('Failed to add achievement:', err.message);
    return null;
  }
}

// Remove achievement (admin function)
async function removeAchievement(achievementId) {
  try {
    // First remove all player achievements
    await query(`DELETE FROM player_achievements WHERE achievement_id = $1`, [achievementId]);
    
    // Then remove the achievement itself
    const result = await query(`DELETE FROM achievements WHERE id = $1`, [achievementId]);
    
    if (result.rowCount > 0) {
      console.log(`✅ Removed achievement with ID: ${achievementId}`);
      return true;
    }
    return false;
  } catch (err) {
    console.error('Failed to remove achievement:', err.message);
    return false;
  }
}

// Manual award achievement to player (admin function)
async function awardAchievement(username, achievementName) {
  try {
    const achievement = await getRow(`SELECT id FROM achievements WHERE name = $1`, [achievementName]);
    if (!achievement) {
      throw new Error('Achievement not found');
    }

    const existingAward = await getRow(`
      SELECT id FROM player_achievements 
      WHERE username = $1 AND achievement_id = $2
    `, [username, achievement.id]);

    if (existingAward) {
      return { success: false, message: 'Player already has this achievement' };
    }

    await query(`
      INSERT INTO player_achievements (username, achievement_id)
      VALUES ($1, $2)
    `, [username, achievement.id]);

    console.log(`🏆 Manually awarded "${achievementName}" to ${username}`);
    return { success: true, message: 'Achievement awarded successfully' };
  } catch (err) {
    console.error('Failed to award achievement:', err.message);
    return { success: false, message: err.message };
  }
}

// Get recent achievement unlocks
async function getRecentAchievementUnlocks(limit = 10) {
  try {
    return await getRows(`
      SELECT 
        pa.username,
        a.name,
        a.description,
        a.icon,
        a.points,
        pa.unlocked_at
      FROM player_achievements pa
      JOIN achievements a ON pa.achievement_id = a.id
      ORDER BY pa.unlocked_at DESC
      LIMIT $1
    `, [limit]);
  } catch (err) {
    console.error('Failed to get recent achievement unlocks:', err.message);
    return [];
  }
}

module.exports = {
  insertDefaultAchievements,
  checkAchievements,
  getPlayerAchievements,
  getAchievementStatistics,
  getRarestAchievements,
  getAchievementLeaderboard,
  addAchievement,
  removeAchievement,
  awardAchievement,
  getRecentAchievementUnlocks
};