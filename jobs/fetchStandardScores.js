const { getAccessToken } = require('../services/osuApi');
const { query } = require('../config/db');
const axios = require('axios');

async function fetchStandardScoresJob() {
  console.log('🔄 Fetching standard scores...');
  const token = await getAccessToken();

  const players = await query(`SELECT user_id, username FROM players WHERE country = 'DZ'`);
  let totalScores = 0;

  for (const player of players) {
    try {
      // Fetch top plays
      const topRes = await axios.get(`https://osu.ppy.sh/api/v2/users/${player.user_id}/scores/best`, {
        headers: { Authorization: `Bearer ${token}` },
        params: { mode: 'osu', limit: 50 }
      });

      // Fetch recent plays
      const recentRes = await axios.get(`https://osu.ppy.sh/api/v2/users/${player.user_id}/scores/recent`, {
        headers: { Authorization: `Bearer ${token}` },
        params: { mode: 'osu', limit: 50 }
      });

      // Store/update them in DB
      const allScores = [...topRes.data, ...recentRes.data];
      totalScores += allScores.length;

      for (const score of allScores) {
        await query(`
          INSERT INTO scores (score_id, user_id, beatmap_id, pp, accuracy, mods, date_played)
          VALUES ($1, $2, $3, $4, $5, $6, $7)
          ON CONFLICT (score_id) DO NOTHING
        `, [
          score.id,
          player.user_id,
          score.beatmap.id,
          score.pp,
          score.accuracy * 100,
          score.mods.join(','),
          new Date(score.ended_at).getTime()
        ]);
      }
    } catch (err) {
      console.warn(`⚠ Failed to fetch scores for ${player.username}:`, err.message);
    }
  }

  console.log(`✅ Stored ${totalScores} scores`);
}

module.exports = fetchStandardScoresJob;
