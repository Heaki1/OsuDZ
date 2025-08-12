const { query } = require('../config/db');

async function fetchDailyStatsJob() {
  console.log('📊 Calculating daily statistics...');
  
  try {
    const today = new Date().toISOString().split('T')[0];
    const yesterday = Date.now() - (24 * 60 * 60 * 1000);
    
    const [activePlayersResult, newScoresResult, totalPPResult, avgAccuracyResult, topScoreResult] = await Promise.all([
      query(`
        SELECT COUNT(DISTINCT username) as count 
        FROM algeria_top50 
        WHERE last_updated > $1
      `, [yesterday]),
      
      query(`
        SELECT COUNT(*) as count 
        FROM algeria_top50 
        WHERE last_updated > $1
      `, [yesterday]),
      
      query(`
        SELECT SUM(pp) as total 
        FROM algeria_top50 
        WHERE last_updated > $1 AND pp > 0
      `, [yesterday]),
      
      query(`
        SELECT AVG(accuracy) as avg 
        FROM algeria_top50 
        WHERE last_updated > $1 AND accuracy::numeric > 0
      `, [yesterday]),
      
      query(`
        SELECT MAX(score) as max 
        FROM algeria_top50 
        WHERE last_updated > $1
      `, [yesterday])
    ]);
    
    await query(`
      INSERT INTO daily_stats (date, active_players, new_scores, total_pp_gained, average_accuracy, top_score)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (date) DO UPDATE SET
        active_players = EXCLUDED.active_players,
        new_scores = EXCLUDED.new_scores,
        total_pp_gained = EXCLUDED.total_pp_gained,
        average_accuracy = EXCLUDED.average_accuracy,
        top_score = EXCLUDED.top_score
    `, [
      today,
      parseInt(activePlayersResult.rows[0].count) || 0,
      parseInt(newScoresResult.rows[0].count) || 0,
      parseFloat(totalPPResult.rows[0].total) || 0,
      parseFloat(avgAccuracyResult.rows[0].avg) || 0,
      parseInt(topScoreResult.rows[0].max) || 0
    ]);
    
    console.log(`✅ Daily stats calculated for ${today}`);
  } catch (err) {
    console.error('❌ Daily stats calculation failed:', err.message);
    throw err;
  }
}

module.exports = fetchDailyStatsJob;