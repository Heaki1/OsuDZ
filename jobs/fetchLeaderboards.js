const { leaderboardService } = require('../services/leaderboard');

async function fetchLeaderboardsJob() {
  console.log('🔄 Starting leaderboards update...');
  
  try {
    await leaderboardService.updateLeaderboards();
    console.log('✅ Leaderboards update completed');
  } catch (err) {
    console.error('❌ Leaderboards update failed:', err.message);
    throw err;
  }
}

module.exports = fetchLeaderboardsJob;
