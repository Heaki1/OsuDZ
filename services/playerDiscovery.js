const axios = require('axios');
const { query, getRows, getRow } = require('../config/db');
const { getAccessToken, getCountryRankings, searchUsers, limiter } = require('./osuApi');
const { checkAchievements } = require('./achievements');

class PlayerDiscoveryService {
  constructor() {
    this.discoveryMethods = [
      'country_rankings',
      'recent_activity', 
      'user_search',
      'multiplayer_matches'
    ];
  }

  // Method 1: Monitor Algeria country rankings
  async discoverFromCountryRankings() {
    try {
      const token = await getAccessToken();
      let cursor = null;
      let page = 1;
      const maxPages = 10;
      let totalFound = 0;

      while (page <= maxPages) {
        const params = {
          country: 'DZ',
          mode: 'osu',
          type: 'performance'
        };
        
        if (cursor) params.cursor_string = cursor;

        const response = await axios.get('https://osu.ppy.sh/api/v2/rankings/osu/performance', {
          headers: { Authorization: `Bearer ${token}` },
          params
        });

        const rankings = response.data.ranking || [];
        if (rankings.length === 0) break;

        for (const playerRanking of rankings) {
          const registered = await this.registerPlayer(playerRanking.user, 'country_rankings');
          if (registered) totalFound++;
        }

        cursor = response.data.cursor?.page;
        if (!cursor) break;
        
        page++;
        await new Promise(resolve => setTimeout(resolve, 1000));
      }

      console.log(`🇩🇿 Country rankings: found ${totalFound} players (${page-1} pages)`);
      return totalFound;
    } catch (err) {
      console.error('❌ Country rankings discovery failed:', err.message);
      return 0;
    }
  }

  // Method 2: Monitor recent scores from popular beatmaps
  async discoverFromRecentScores() {
    try {
      const popularBeatmaps = await getRows(`
        SELECT beatmap_id, COUNT(DISTINCT username) as player_count
        FROM algeria_top50 
        GROUP BY beatmap_id 
        ORDER BY player_count DESC 
        LIMIT 30
      `);

      const token = await getAccessToken();
      let totalFound = 0;

      for (const beatmap of popularBeatmaps) {
        try {
          const response = await axios.get(
            `https://osu.ppy.sh/api/v2/beatmaps/${beatmap.beatmap_id}/scores`,
            {
              headers: { Authorization: `Bearer ${token}` },
              params: { limit: 50 }
            }
          );

          const scores = response.data.scores || [];
          const algerianScores = scores.filter(score => 
            score.user?.country?.code === 'DZ'
          );

          for (const score of algerianScores) {
            const registered = await this.registerPlayer(score.user, 'recent_scores');
            if (registered) totalFound++;
          }

          await new Promise(resolve => setTimeout(resolve, 800));
        } catch (err) {
          console.warn(`Failed to check beatmap ${beatmap.beatmap_id}:`, err.message);
        }
      }

      console.log(`📊 Recent scores: found ${totalFound} new players`);
      return totalFound;
    } catch (err) {
      console.error('❌ Recent scores discovery failed:', err.message);
      return 0;
    }
  }

  // Method 3: Search for Algerian players
  async discoverFromUserSearch() {
    try {
      const token = await getAccessToken();
      let totalFound = 0;
      
      const searchTerms = ['algeria', 'dz', 'algerie'];

      for (const term of searchTerms) {
        try {
          const response = await axios.get('https://osu.ppy.sh/api/v2/search', {
            headers: { Authorization: `Bearer ${token}` },
            params: {
              mode: 'user',
              query: term
            }
          });

          const users = response.data.user?.data || [];
          const algerianUsers = users.filter(user => 
            user.country?.code === 'DZ'
          );

          for (const user of algerianUsers) {
            const registered = await this.registerPlayer(user, 'user_search');
            if (registered) totalFound++;
          }

          await new Promise(resolve => setTimeout(resolve, 2000));
        } catch (err) {
          console.warn(`Search term '${term}' failed:`, err.message);
        }
      }

      console.log(`🔍 User search: found ${totalFound} new players`);
      return totalFound;
    } catch (err) {
      console.error('❌ User search discovery failed:', err.message);
      return 0;
    }
  }

  // Method 4: Discover players from recent multiplayer matches
  async discoverFromMultiplayerMatches() {
    try {
      const token = await getAccessToken();
      let totalFound = 0;
      let matchIds = [];
      
      try {
        const res = await axios.get('https://osu.ppy.sh/api/v2/multiplayer/matches', {
          headers: { Authorization: `Bearer ${token}` },
          params: { limit: 30 }
        });
        if (Array.isArray(res.data.matches)) {
          matchIds = res.data.matches.map(m => m.id).slice(0, 30);
        }
      } catch (e) {
        console.debug('Multiplayer match list endpoint failed, will attempt alternative scanning');
      }

      if (matchIds.length === 0) {
        const recentBeatmaps = await getRows(`
          SELECT beatmap_id FROM beatmap_metadata
          ORDER BY last_updated DESC
          LIMIT 20
        `);
        for (const bm of recentBeatmaps) {
          try {
            const res = await axios.get(`https://osu.ppy.sh/api/v2/beatmaps/${bm.beatmap_id}/multiplayer`, {
              headers: { Authorization: `Bearer ${token}` },
              params: { limit: 10 }
            });
            if (res.data.matches) {
              for (const m of res.data.matches) {
                if (m.id) matchIds.push(m.id);
              }
            }
            await new Promise(r => setTimeout(r, 400));
          } catch (err) {
            // ignore per-beatmap errors
          }
        }
      }

      for (const matchId of [...new Set(matchIds)].slice(0, 60)) {
        try {
          const res = await axios.get(`https://osu.ppy.sh/api/v2/multiplayer/matches/${matchId}`, {
            headers: { Authorization: `Bearer ${token}` }
          });
          const match = res.data;
          const participants = match?.matches ? (match.matches.flatMap(x => x.scores || [])) : (match?.scores || []);
          if (Array.isArray(participants)) {
            for (const p of participants) {
              const u = p.user || p;
              if (u && u.country?.code === 'DZ') {
                const registered = await this.registerPlayer(u, 'multiplayer_matches');
                if (registered) totalFound++;
              }
            }
          }
          await new Promise(r => setTimeout(r, 500));
        } catch (err) {
          console.debug(`Multiplayer match ${matchId} fetch failed: ${err.message}`);
        }
      }

      console.log(`🎮 Multiplayer discovery: found ${totalFound} players`);
      return totalFound;
    } catch (err) {
      console.error('Multiplayer discovery failed:', err.message);
      return 0;
    }
  }

  // Core player registration
  async registerPlayer(userData, discoveryMethod = 'unknown') {
    try {
      if (!userData || !userData.id || userData.country?.code !== 'DZ') {
        return false;
      }

      // Check if player already exists
      const existingPlayer = await getRow(`
        SELECT username, last_seen FROM player_stats 
        WHERE user_id = $1 OR username ILIKE $2
      `, [userData.id, userData.username]);

      const now = new Date().toISOString();
      const isNewPlayer = !existingPlayer;

      if (isNewPlayer) {
        console.log(`🆕 New Algerian player: ${userData.username} (${discoveryMethod})`);
        
        // Send Discord notification
        if (process.env.DISCORD_WEBHOOK_URL) {
          await this.sendNewPlayerNotification(userData, discoveryMethod);
        }

        // Broadcast to clients if available
        if (global.broadcastToClients) {
          global.broadcastToClients({
            type: 'new_player_discovered',
            player: {
              username: userData.username,
              userId: userData.id,
              discoveryMethod,
              timestamp: now
            }
          });
        }
      }

      // Insert/update player
      await query(`
        INSERT INTO player_stats (
          username, user_id, join_date, last_seen, avatar_url, cover_url,
          global_rank, country_rank, level, playcount, total_playtime, is_active
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, true)
        ON CONFLICT (username) DO UPDATE SET
          user_id = COALESCE(EXCLUDED.user_id, player_stats.user_id),
          last_seen = EXCLUDED.last_seen,
          avatar_url = COALESCE(EXCLUDED.avatar_url, player_stats.avatar_url),
          cover_url = COALESCE(EXCLUDED.cover_url, player_stats.cover_url),
          global_rank = COALESCE(EXCLUDED.global_rank, player_stats.global_rank),
          country_rank = COALESCE(EXCLUDED.country_rank, player_stats.country_rank),
          level = COALESCE(EXCLUDED.level, player_stats.level),
          playcount = COALESCE(EXCLUDED.playcount, player_stats.playcount),
          total_playtime = COALESCE(EXCLUDED.total_playtime, player_stats.total_playtime),
          is_active = true
      `, [
        userData.username,
        userData.id,
        userData.join_date ? new Date(userData.join_date).toISOString() : now,
        now,
        userData.avatar_url,
        userData.cover_url || userData.cover?.url,
        userData.statistics?.global_rank,
        userData.statistics?.country_rank,
        userData.statistics?.level?.current,
        userData.statistics?.play_count,
        userData.statistics?.play_time
      ]);

      // Log discovery
      await query(`
        INSERT INTO player_discovery_log (username, user_id, discovery_method, is_new_player, player_data)
        VALUES ($1, $2, $3, $4, $5)
      `, [
        userData.username,
        userData.id,
        discoveryMethod,
        isNewPlayer,
        JSON.stringify(userData)
      ]);

      // If new player, fetch their history
      if (isNewPlayer) {
        setTimeout(() => {
          this.fetchPlayerHistory(userData.username, userData.id);
        }, 5000);
      }

      return isNewPlayer;
    } catch (err) {
      console.error(`❌ Failed to register ${userData?.username}:`, err.message);
      return false;
    }
  }

  // Fetch comprehensive player history
  async fetchPlayerHistory(username, userId) {
    try {
      console.log(`📥 Fetching history for ${username}`);
      
      const token = await getAccessToken();
      
      // Get best scores
      const bestResponse = await axios.get(
        `https://osu.ppy.sh/api/v2/users/${userId}/scores/best`,
        {
          headers: { Authorization: `Bearer ${token}` },
          params: { limit: 100, mode: 'osu' }
        }
      );

      const bestScores = bestResponse.data || [];
      
      // Process scores for leaderboard positions
      let processedScores = 0;
      for (const score of bestScores.slice(0, 50)) { // Limit to avoid rate limits
        try {
          await limiter.schedule(() => 
            this.checkScoreOnLeaderboard(score, username)
          );
          processedScores++;
        } catch (err) {
          console.warn(`Failed to process score on beatmap ${score.beatmap.id}:`, err.message);
        }
      }

      await this.updatePlayerStats(username, bestScores);
      await checkAchievements(username);
      
      console.log(`✅ Completed history fetch for ${username} (${processedScores} scores)`);
      
    } catch (err) {
      console.error(`❌ Failed to fetch history for ${username}:`, err.message);
    }
  }

  // Check if score appears on leaderboard
  async checkScoreOnLeaderboard(score, expectedUsername) {
    try {
      const token = await getAccessToken();
      
      const response = await axios.get(
        `https://osu.ppy.sh/api/v2/beatmaps/${score.beatmap.id}/scores`,
        {
          headers: { Authorization: `Bearer ${token}` },
          params: { limit: 50 }
        }
      );

      const leaderboardScores = response.data.scores || [];
      const algerianScores = leaderboardScores.filter(s => 
        s.user?.country?.code === 'DZ'
      );

      if (algerianScores.length > 0) {
        const beatmapTitle = `${score.beatmapset.artist} - ${score.beatmapset.title} [${score.beatmap.version}]`;
        // This would need to import from leaderboard service
        // await saveBeatmapScores(score.beatmap.id, beatmapTitle, algerianScores, score.beatmap);
      }

    } catch (err) {
      // Silently handle - this is expected for many beatmaps
    }
  }

  // Update player statistics
  async updatePlayerStats(username, scores) {
    try {
      if (scores.length === 0) return;

      const totalPP = scores.reduce((sum, s) => sum + (s.pp || 0), 0);
      const avgAccuracy = scores.reduce((sum, s) => sum + (s.accuracy || 0), 0) / scores.length;
      const bestScore = Math.max(...scores.map(s => s.score || 0));

      const weightedPP = scores.reduce((sum, score, index) => {
        return sum + (score.pp || 0) * Math.pow(0.95, index);
      }, 0);

      await query(`
        UPDATE player_stats SET
          total_pp = $2,
          weighted_pp = $3,
          accuracy_avg = $4,
          best_score = $5,
          playcount = $6,
          last_calculated = $7
        WHERE username = $1
      `, [
        username,
        totalPP,
        weightedPP,
        avgAccuracy,
        bestScore,
        scores.length,
        new Date().toISOString()
      ]);

    } catch (err) {
      console.error(`Failed to update stats for ${username}:`, err.message);
    }
  }

  // Discord notification (disabled temporarily)
  async sendNewPlayerNotification(userData, discoveryMethod) {
    return; // skip sending anything
  }

  // Run complete discovery
  async runDiscovery() {
    console.log('🔍 Starting player discovery...');
    
    const startTime = Date.now();
    const results = {
      countryRankings: await this.discoverFromCountryRankings(),
      recentScores: await this.discoverFromRecentScores(),
      userSearch: await this.discoverFromUserSearch(),
      multiplayerMatches: await this.discoverFromMultiplayerMatches()
    };

    const total = Object.values(results).reduce((sum, count) => sum + count, 0);
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    
    console.log(`✅ Discovery completed in ${duration}s - Found ${total} new players`);
    
    if (global.broadcastToClients) {
      global.broadcastToClients({
        type: 'discovery_complete',
        results,
        total,
        duration,
        timestamp: Date.now()
      });
    }

    return results;
  }
}

// Helper function to discover players by country
async function discoverPlayers(country = 'DZ', limit = 50) {
  const discovery = new PlayerDiscoveryService();
  return await discovery.discoverFromCountryRankings();
}

module.exports = {
  PlayerDiscoveryService,
  discoverPlayers
};