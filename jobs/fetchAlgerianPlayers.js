const { discoverPlayers } = require('../services/playerDiscovery');

async function fetchAlgerianPlayersJob() {
  console.log('🔄 Fetching Algerian players...');
  const players = await discoverPlayers('DZ', 50);
  console.log(`✅ Found/Updated ${players.length} Algerian players`);
}

module.exports = fetchAlgerianPlayersJob;
