const { PlayerDiscoveryService } = require('../services/playerDiscovery');
const playerDiscovery = new PlayerDiscoveryService();

async function fetchAlgerianPlayersJob() {
  console.log('🔄 Starting comprehensive player discovery...');
  
  try {
    const results = await playerDiscovery.runDiscovery();
    const total = Object.values(results).reduce((sum, count) => sum + count, 0);
    console.log(`✅ Discovery completed - Found ${total} new players`, results);
    return results;
  } catch (err) {
    console.error('❌ Player discovery failed:', err.message);
    throw err;
  }
}

module.exports = fetchAlgerianPlayersJob;
