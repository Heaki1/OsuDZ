const { playerDiscovery } = require('../services/playerDiscovery');

async function fetchAlgerianPlayersJob() {
  console.log('🔄 Starting comprehensive player discovery...');
  
  try {
    const results = await playerDiscovery.runDiscovery();
    const total = Object.values(results).reduce((sum, count) => sum + count, 0);
    
    console.log(`✅ Discovery completed - Found ${total} new players:`, {
      countryRankings: results.countryRankings,
      recentScores: results.recentScores,
      userSearch: results.userSearch,
      multiplayerMatches: results.multiplayerMatches
    });
    
    return results;
  } catch (err) {
    console.error('❌ Player discovery failed:', err.message);
    throw err;
  }
}

module.exports = fetchAlgerianPlayersJob;