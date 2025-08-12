const { query } = require('../config/db');

async function cleanupJob() {
  console.log('🧹 Running database cleanup...');
  
  try {
    const SKILL_CLEANUP_CUTOFF_DAYS = 90;
    const cutoff = Date.now() - SKILL_CLEANUP_CUTOFF_DAYS * 24 * 60 * 60 * 1000;
    
    // Clean up old skill tracking data
    const skillResult = await query(`
      DELETE FROM skill_tracking 
      WHERE calculated_at < $1
    `, [cutoff]);
    
    // Clean up old player activity data (keep last 3 months)
    const activityResult = await query(`
      DELETE FROM player_activity 
      WHERE timestamp < $1
    `, [cutoff]);
    
    // Clean up old discovery logs (keep last 6 months)
    const discoveryResult = await query(`
      DELETE FROM player_discovery_log 
      WHERE discovery_timestamp < $1
    `, [Date.now() - (180 * 24 * 60 * 60 * 1000)]);
    
    const totalCleaned = skillResult.rowCount + activityResult.rowCount + discoveryResult.rowCount;
    
    if (totalCleaned > 0) {
      console.log(`✅ Cleaned up ${totalCleaned} old records:`, {
        skillTracking: skillResult.rowCount,
        playerActivity: activityResult.rowCount,
        discoveryLogs: discoveryResult.rowCount
      });
    } else {
      console.log('✅ No old records to clean up');
    }
    
  } catch (err) {
    console.error('❌ Database cleanup failed:', err.message);
    throw err;
  }
}

module.exports = cleanupJob;