const { query } = require('../config/db');
const { SkillCalculator } = require('../services/skillCalculator');
const { broadcastToClients } = require('../middleware/websocket');

async function updatePlayerSkillsJob() {
  console.log('🧮 Updating player skills...');
  
  try {
    // Get all active players
const result = await query(`
  SELECT username 
  FROM player_stats 
  WHERE is_active = true 
  AND total_scores > 0
  ORDER BY last_calculated ASC
  LIMIT 100
`);
const players = result.rows || [];

for (const player of players) {
      try {
        // Get player's scores for skill calculation
        const playerScores = await query(`
          SELECT * FROM algeria_top50 
          WHERE username = $1 
          ORDER BY pp DESC 
          LIMIT 100
        `, [player.username]);

        if (playerScores.rows.length === 0) {
          console.log(`⚠️ No scores found for ${player.username}, skipping...`);
          continue;
        }

        // Calculate skills
        const skills = {
          aim: SkillCalculator.calculateAimSkill(playerScores.rows),
          speed: SkillCalculator.calculateSpeedSkill(playerScores.rows),
          accuracy: SkillCalculator.calculateAccuracySkill(playerScores.rows),
          reading: SkillCalculator.calculateReadingSkill(playerScores.rows),
          consistency: SkillCalculator.calculateConsistencySkill(playerScores.rows)
        };

        const now = Date.now();

        // Store skill tracking data
        for (const [skillType, skillValue] of Object.entries(skills)) {
          await query(`
            INSERT INTO skill_tracking (username, skill_type, skill_value, calculated_at)
            VALUES ($1, $2, $3, $4)
          `, [player.username, skillType, skillValue, now]);
        }

        // Clean up old skill data (keep last 30 entries per skill type)
        await query(`
          DELETE FROM skill_tracking 
          WHERE username = $1 
          AND id NOT IN (
            SELECT id FROM skill_tracking 
            WHERE username = $1 
            ORDER BY calculated_at DESC 
            LIMIT 150
          )
        `, [player.username]);

        // Update player stats with last calculation time
        await query(`
          UPDATE player_stats 
          SET last_calculated = $1 
          WHERE username = $2
        `, [now, player.username]);

        updatedCount++;
        console.log(`✅ Updated skills for ${player.username}:`, skills);

        // Broadcast progress every 10 players
        if (updatedCount % 10 === 0) {
          broadcastToClients({
            type: 'skill_update_progress',
            progress: {
              updated: updatedCount,
              total: players.length,
              currentPlayer: player.username
            }
          });
        }

      } catch (err) {
        console.error(`❌ Failed to update skills for ${player.username}:`, err.message);
        errorCount++;
      }
    }

    console.log(`✅ Skill calculation completed: ${updatedCount} players updated (${errorCount} errors)`);

    // Broadcast completion
    broadcastToClients({
      type: 'skill_update_complete',
      summary: {
        updated: updatedCount,
        errors: errorCount,
        timestamp: Date.now()
      }
    });

    return { updated: updatedCount, errors: errorCount };

  } catch (err) {
    console.error('❌ Player skills update failed:', err.message);
    throw err;
  }
}

module.exports = updatePlayerSkillsJob;