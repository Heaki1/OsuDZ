const { query } = require('../config/db');
const { getAccessToken } = require('../services/osuApi');
const axios = require('axios');

async function fetchGeneralStatsJob() {
  console.log('🔄 Updating general player stats...');
  const token = await getAccessToken();

  const players = await query(`SELECT user_id FROM players WHERE country = 'DZ'`);
  let updatedCount = 0;

  for (const player of players) {
    try {
      const { data } = await axios.get(`https://osu.ppy.sh/api/v2/users/${player.user_id}/osu`, {
        headers: { Authorization: `Bearer ${token}` }
      });

      await query(`
        UPDATE players
        SET accuracy = $1, playcount = $2, total_score = $3, ranked_score = $4
        WHERE user_id = $5
      `, [
        data.statistics.hit_accuracy,
        data.statistics.play_count,
        data.statistics.total_score,
        data.statistics.ranked_score,
        player.user_id
      ]);

      updatedCount++;
    } catch (err) {
      console.warn(`⚠ Failed to update stats for user ${player.user_id}:`, err.message);
    }
  }

  console.log(`✅ Updated stats for ${updatedCount} players`);
}

module.exports = fetchGeneralStatsJob;
