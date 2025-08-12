const { query, getRows } = require('../config/db');

class SkillCalculator {
  static calculateAimSkill(scores) {
    const aimScores = scores.filter(s => s.mods?.includes('HR') || s.difficulty_rating > 5.0);
    if (aimScores.length === 0) return 0;
    
    const avgPP = aimScores.reduce((sum, s) => sum + (s.pp || 0), 0) / aimScores.length;
    const accuracyFactor = aimScores.reduce((sum, s) => sum + (s.accuracy || 0), 0) / aimScores.length;
    
    return Math.min(10, (avgPP / 100) * accuracyFactor * 1.2);
  }

  static calculateSpeedSkill(scores) {
    const speedScores = scores.filter(s => s.mods?.includes('DT') || s.difficulty_rating > 4.5);
    if (speedScores.length === 0) return 0;
    
    const dtScores = speedScores.filter(s => s.mods?.includes('DT')).length;
    const dtRatio = dtScores / scores.length;
    const avgPP = speedScores.reduce((sum, s) => sum + (s.pp || 0), 0) / speedScores.length;
    
    return Math.min(10, (avgPP / 80) * (1 + dtRatio));
  }

  static calculateAccuracySkill(scores) {
    if (scores.length === 0) return 0;
    
    const avgAccuracy = scores.reduce((sum, s) => sum + (s.accuracy || 0), 0) / scores.length;
    const highAccuracyScores = scores.filter(s => (s.accuracy || 0) > 0.98).length;
    const consistencyBonus = highAccuracyScores / scores.length;
    
    return Math.min(10, avgAccuracy * 10 * (1 + consistencyBonus));
  }

  static calculateReadingSkill(scores) {
    const readingScores = scores.filter(s => s.mods?.includes('HD') || s.mods?.includes('HR'));
    if (readingScores.length === 0) return Math.min(10, scores.length * 0.1);
    
    const hdScores = readingScores.filter(s => s.mods?.includes('HD')).length;
    const hrScores = readingScores.filter(s => s.mods?.includes('HR')).length;
    const modVariety = (hdScores + hrScores) / scores.length;
    
    return Math.min(10, 3 + (modVariety * 7));
  }

  static calculateConsistencySkill(scores) {
    if (scores.length < 5) return 0;
    
    const missRates = scores.map(s => (s.count_miss || 0) / Math.max(1, s.max_combo || 100));
    const avgMissRate = missRates.reduce((sum, rate) => sum + rate, 0) / missRates.length;
    const consistency = Math.max(0, 1 - avgMissRate * 2);
    
    return Math.min(10, consistency * 10);
  }

  static calculateOverallSkill(scores) {
    const skills = {
      aim: this.calculateAimSkill(scores),
      speed: this.calculateSpeedSkill(scores),
      accuracy: this.calculateAccuracySkill(scores),
      reading: this.calculateReadingSkill(scores),
      consistency: this.calculateConsistencySkill(scores)
    };

    // Weighted average with emphasis on different aspects
    const weights = {
      aim: 0.25,
      speed: 0.25,
      accuracy: 0.20,
      reading: 0.15,
      consistency: 0.15
    };

    const overallSkill = Object.entries(skills).reduce((sum, [skill, value]) => {
      return sum + (value * weights[skill]);
    }, 0);

    return {
      ...skills,
      overall: Math.min(10, overallSkill)
    };
  }

  static calculateSkillProgression(username, days = 30) {
    const cutoff = Date.now() - (days * 24 * 60 * 60 * 1000);
    
    return getRows(`
      SELECT skill_type, skill_value, calculated_at
      FROM skill_tracking 
      WHERE username = $1 AND calculated_at > $2
      ORDER BY calculated_at ASC
    `, [username, cutoff]);
  }

  static async calculateSkillTrends(username) {
    const recentSkills = await getRows(`
      SELECT skill_type, skill_value, calculated_at
      FROM skill_tracking 
      WHERE username = $1
      ORDER BY calculated_at DESC
      LIMIT 50
    `, [username]);

    const trends = {};
    const skillTypes = ['aim', 'speed', 'accuracy', 'reading', 'consistency'];

    skillTypes.forEach(skillType => {
      const skillData = recentSkills
        .filter(s => s.skill_type === skillType)
        .slice(0, 10);

      if (skillData.length >= 3) {
        const recent = skillData.slice(0, 3).reduce((sum, s) => sum + s.skill_value, 0) / 3;
        const older = skillData.slice(-3).reduce((sum, s) => sum + s.skill_value, 0) / 3;
        const trend = recent - older;

        trends[skillType] = {
          current: recent,
          trend: trend,
          direction: trend > 0.1 ? 'improving' : trend < -0.1 ? 'declining' : 'stable'
        };
      } else {
        trends[skillType] = {
          current: skillData[0]?.skill_value || 0,
          trend: 0,
          direction: 'insufficient_data'
        };
      }
    });

    return trends;
  }

  static calculateSkillRanking(username, skillType) {
    return query(`
      WITH recent_skills AS (
        SELECT DISTINCT ON (username) username, skill_value
        FROM skill_tracking 
        WHERE skill_type = $2
        ORDER BY username, calculated_at DESC
      )
      SELECT COUNT(*) + 1 as rank
      FROM recent_skills
      WHERE skill_value > (
        SELECT skill_value 
        FROM recent_skills 
        WHERE username = $1
      )
    `, [username, skillType]);
  }

  static async getTopPlayersBySkill(skillType, limit = 10) {
    return await getRows(`
      WITH recent_skills AS (
        SELECT DISTINCT ON (username) username, skill_value, calculated_at
        FROM skill_tracking 
        WHERE skill_type = $1
        ORDER BY username, calculated_at DESC
      )
      SELECT rs.username, rs.skill_value, ps.weighted_pp, ps.country_rank
      FROM recent_skills rs
      JOIN player_stats ps ON rs.username = ps.username
      WHERE ps.is_active = true
      ORDER BY rs.skill_value DESC
      LIMIT $2
    `, [skillType, limit]);
  }

  static calculateSkillBalance(skills) {
    const skillValues = Object.values(skills).filter(v => typeof v === 'number');
    if (skillValues.length === 0) return 0;

    const mean = skillValues.reduce((sum, val) => sum + val, 0) / skillValues.length;
    const variance = skillValues.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / skillValues.length;
    const balance = Math.max(0, 10 - Math.sqrt(variance));

    return balance;
  }

  static categorizePlayer(skills) {
    const { aim, speed, accuracy, reading, consistency } = skills;
    
    // Determine dominant skills
    const maxSkill = Math.max(aim, speed, accuracy, reading, consistency);
    const dominantSkills = [];
    
    if (aim >= maxSkill * 0.9) dominantSkills.push('aim');
    if (speed >= maxSkill * 0.9) dominantSkills.push('speed');
    if (accuracy >= maxSkill * 0.9) dominantSkills.push('accuracy');
    if (reading >= maxSkill * 0.9) dominantSkills.push('reading');
    if (consistency >= maxSkill * 0.9) dominantSkills.push('consistency');

    // Categorize based on dominant skills
    if (dominantSkills.length === 1) {
      const categories = {
        aim: 'Aim Specialist',
        speed: 'Speed Player',
        accuracy: 'Accuracy Player',
        reading: 'Technical Player',
        consistency: 'Consistent Player'
      };
      return categories[dominantSkills[0]] || 'Specialized Player';
    }

    if (dominantSkills.includes('aim') && dominantSkills.includes('speed')) {
      return 'All-Rounder';
    }

    if (dominantSkills.includes('accuracy') && dominantSkills.includes('consistency')) {
      return 'Precision Player';
    }

    if (dominantSkills.length >= 3) {
      return 'Versatile Player';
    }

    return 'Developing Player';
  }
}

// Update player skills
async function updatePlayerSkills(username, playerScores) {
  try {
    const skills = SkillCalculator.calculateOverallSkill(playerScores);
    const now = Date.now();
    
    for (const [skillType, skillValue] of Object.entries(skills)) {
      if (skillType === 'overall') continue; // Skip overall in individual tracking
      
      await query(`
        INSERT INTO skill_tracking (username, skill_type, skill_value, calculated_at)
        VALUES ($1, $2, $3, $4)
      `, [username, skillType, skillValue, now]);
    }
    
    // Keep only last 30 entries per skill type to prevent table bloat
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
    console.error('Skill tracking update failed:', err.message);
  }
}

// Get skill statistics for all players
async function getSkillStatistics() {
  try {
    const stats = await getRows(`
      WITH recent_skills AS (
        SELECT DISTINCT ON (username, skill_type) 
               username, skill_type, skill_value
        FROM skill_tracking 
        ORDER BY username, skill_type, calculated_at DESC
      )
      SELECT 
        skill_type,
        COUNT(*) as player_count,
        AVG(skill_value) as avg_skill,
        MAX(skill_value) as max_skill,
        MIN(skill_value) as min_skill,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY skill_value) as median_skill
      FROM recent_skills
      GROUP BY skill_type
    `);

    return stats.reduce((acc, stat) => {
      acc[stat.skill_type] = {
        playerCount: parseInt(stat.player_count),
        average: parseFloat(stat.avg_skill),
        maximum: parseFloat(stat.max_skill),
        minimum: parseFloat(stat.min_skill),
        median: parseFloat(stat.median_skill)
      };
      return acc;
    }, {});
  } catch (err) {
    console.error('Failed to get skill statistics:', err.message);
    return {};
  }
}

// Clean up old skill tracking data
async function cleanupOldSkillData(cutoffDays = 90) {
  try {
    const cutoff = Date.now() - (cutoffDays * 24 * 60 * 60 * 1000);
    const result = await query(`
      DELETE FROM skill_tracking 
      WHERE calculated_at < $1
    `, [cutoff]);
    
    if (result.rowCount > 0) {
      console.log(`🧹 Cleaned up ${result.rowCount} old skill tracking entries`);
    }
    
    return result.rowCount;
  } catch (err) {
    console.error('Skill cleanup failed:', err.message);
    return 0;
  }
}

module.exports = {
  SkillCalculator,
  updatePlayerSkills,
  getSkillStatistics,
  cleanupOldSkillData
};