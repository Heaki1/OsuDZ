const cron = require('node-cron');
const fetchAlgerianPlayersJob = require('./fetchAlgerianPlayers');
const fetchStandardScoresJob = require('./fetchStandardScores');
const fetchGeneralStatsJob = require('./fetchGeneralStats');

// Every 12 hours → Refresh player list
cron.schedule('0 */12 * * *', fetchAlgerianPlayersJob);

// Every hour → Fetch top & recent scores
cron.schedule('0 * * * *', fetchStandardScoresJob);

// Every 6 hours → Update general stats
cron.schedule('0 */6 * * *', fetchGeneralStatsJob);

console.log('📅 Jobs scheduled');
