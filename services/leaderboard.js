const axios = require('axios');
const { query, getRows, pool } = require('../config/db');
const { getAccessToken, getBeatmapScores, getBeatmap, limiter } = require('./osuApi');
const { saveProgress, getProgress } = require('../config/redis');
const { updatePlayerSkills } = require('./skillCalculator');
const { checkAchievements } = require('./achievements');

// Leaderboard fetching and management
class LeaderboardService {
  constructor() {
    this.maxRetries = 3;
    this.batchSize = 50;
  }

  // Fetch leaderboard for a specific beatmap
  async fetchLeaderboard(beatmapId, beatmapTitle) {
    let attempt = 0;
    
    while (attempt < this.maxRetries) {
      try {
        const token = await getAccessToken();
        
        const [scoresRes, beatmapRes] = await Promise.all([
          axios.get(`https://osu.ppy.sh/api/v2/beatmaps/${beatmapId}/scores`, {
            headers: { Authorization: `Bearer ${token}` }
          }),
          axios.get(`https://osu.ppy.sh/api/v2/beatmaps/${beatmapId}`, {
            headers: { Authorization: `Bearer ${token}` }
          }).catch(() => ({ data: null }))
        ]);
        
        const scores = scoresRes.data.scores || [];
        const beatmapInfo = beatmapRes.data;
        const algerianScores = scores.filter(s => s.user?.country?.code === 'DZ');
        
        if (algerianScores.length > 0) {
          await this.saveBeatmapScores(beatmapId, beatmapTitle, algerianScores, beatmapInfo);
          
          // Broadcast to clients if available
          if (global.broadcastToClients) {
            global.broadcastToClients({
              type: 'new_scores',
              beatmapId,
              beatmapTitle,
              scoresCount: algerianScores.length,
              topScore: algerianScores[0]
            });
          }
        }
        
        if (beatmapInfo) {
          await this.saveBeatmapMetadata(beatmapInfo);
        }
        
        return {
          success: true,
          algerianScores: algerianScores.length,
          totalScores: scores.length
        };
        
      } catch (err) {
        attempt++;
        if (attempt >= this.maxRetries) {
          console.warn(`Failed to fetch ${beatmapId} after ${this.maxRetries} attempts:`, err.message);
          return { success: false, error: err.message };
        }
        
        const backoffTime = Math.min(1000 * Math.pow(2, attempt), 10000);
        await new Promise(resolve => setTimeout(resolve, backoffTime));
      }
    }
  }

  // Save beatmap scores to database
  async saveBeatmapScores(beatmapId, beatmapTitle, algerianScores, beatmapInfo) {
    const now = Date.now();
    const client = await pool.connect();
    
    try {
      await client.query('BEGIN');
      
      for (let i = 0; i < algerianScores.length; i++) {
        const s = algerianScores[i];
        const mods = s.mods?.length ? s.mods.join(',') : 'None';
        
        // Check for new #1 score
        const existingTop = await client.query(
          'SELECT username, rank FROM algeria_top50 WHERE beatmap_id = $1 ORDER BY rank ASC LIMIT 1',
          [beatmapId]
        );
        
        const isNewFirst = i === 0 && (!existingTop.rows[0] || existingTop.rows[0].username !== s.user.username);
        
        await client.query(`
          INSERT INTO algeria_top50
            (beatmap_id, beatmap_title, artist, difficulty_name, player_id, username, rank, score, 
             accuracy, accuracy_text, mods, pp, difficulty_rating, max_combo, count_300, count_100, 
             count_50, count_miss, play_date, last_updated)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
          ON CONFLICT (beatmap_id, player_id) DO UPDATE SET
            beatmap_title = EXCLUDED.beatmap_title,
            artist = EXCLUDED.artist,
            difficulty_name = EXCLUDED.difficulty_name,
            username = EXCLUDED.username,
            rank = EXCLUDED.rank,
            score = EXCLUDED.score,
            accuracy = EXCLUDED.accuracy,
            accuracy_text = EXCLUDED.accuracy_text,
            mods = EXCLUDED.mods,
            pp = EXCLUDED.pp,
            difficulty_rating = EXCLUDED.difficulty_rating,
            max_combo = EXCLUDED.max_combo,
            count_300 = EXCLUDED.count_300,
            count_100 = EXCLUDED.count_100,
            count_50 = EXCLUDED.count_50,
            count_miss = EXCLUDED.count_miss,
            play_date = EXCLUDED.play_date,
            last_updated = EXCLUDED.last_updated
        `, [
          beatmapId, beatmapTitle,
          beatmapInfo?.beatmapset?.artist || 'Unknown',
          beatmapInfo?.version || 'Unknown',
          s.user.id, s.user.username, i + 1, s.score,
          s.accuracy, `${(s.accuracy * 100).toFixed(2)}%`,
          mods, s.pp || 0, beatmapInfo?.difficulty_rating || 0,
          s.max_combo || 0, s.statistics?.count_300 || 0,
          s.statistics?.count_100 || 0, s.statistics?.count_50 || 0,
          s.statistics?.count_miss || 0,
          new Date(s.created_at).getTime(), now
        ]);
        
        if (isNewFirst) {
          await client.query(`
            INSERT INTO player_activity (username, activity_type, activity_data)
            VALUES ($1, 'new_first_place', $2)
          `, [s.user.username, JSON.stringify({
            beatmapId, beatmapTitle, score: s.score, pp: s.pp, mods
          })]);
          
          // Send notification if configured
          if (process.env.DISCORD_WEBHOOK_URL) {
            await this.sendDiscordNotification(s, beatmapTitle, beatmapId, 'new_first');
          }
        }
      }
      
      await client.query('COMMIT');
      
      // Update player stats for all affected players
      for (const score of algerianScores) {
        await this.updatePlayerStats(score.user.username);
        await checkAchievements(score.user.username);
      }
      
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  }

  // Save beatmap metadata
  async saveBeatmapMetadata(beatmapInfo) {
    try {
      const beatmapset = beatmapInfo.beatmapset || {};
      
      await query(`
        INSERT INTO beatmap_metadata (
          beatmap_id, beatmapset_id, artist, title, version, creator,
          difficulty_rating, cs, ar, od, hp, length, bpm, max_combo,
          play_count, favorite_count, ranked_date, last_updated
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
        ON CONFLICT (beatmap_id) DO UPDATE SET
          artist = EXCLUDED.artist,
          title = EXCLUDED.title,
          version = EXCLUDED.version,
          creator = EXCLUDED.creator,
          difficulty_rating = EXCLUDED.difficulty_rating,
          cs = EXCLUDED.cs,
          ar = EXCLUDED.ar,
          od = EXCLUDED.od,
          hp = EXCLUDED.hp,
          length = EXCLUDED.length,
          bpm = EXCLUDED.bpm,
          max_combo = EXCLUDED.max_combo,
          play_count = EXCLUDED.play_count,
          favorite_count = EXCLUDED.favorite_count,
          last_updated = EXCLUDED.last_updated
      `, [
        beatmapInfo.id, beatmapset.id, beatmapset.artist, beatmapset.title,
        beatmapInfo.version, beatmapset.creator, beatmapInfo.difficulty_rating,
        beatmapInfo.cs, beatmapInfo.ar, beatmapInfo.accuracy, beatmapInfo.drain,
        beatmapInfo.total_length, beatmapInfo.bpm, beatmapInfo.max_combo,
        beatmapInfo.playcount, beatmapset.favourite_count,
        beatmapset.ranked_date ? new Date(beatmapset.ranked_date).getTime() : null,
        Date.now()
      ]);
    } catch (err) {
      console.error('Beatmap metadata save failed:', err.message);
    }
  }

  // Update player statistics based on their scores
  async updatePlayerStats(username) {
    try {
      const playerScores = await getRows(`
        SELECT * FROM algeria_top50 WHERE username = $1
      `, [username]);
      
      if (playerScores.length === 0) return;
      
      const now = Date.now();
      const totalScores = playerScores.length;
      const avgRank = playerScores.reduce((sum, s) => sum + s.rank, 0) / totalScores;
      const bestScore = Math.max(...playerScores.map(s => s.score));
      const totalPP = playerScores.reduce((sum, s) => sum + (s.pp || 0), 0);
      const firstPlaces = playerScores.filter(s => s.rank === 1).length;
      const top10Places = playerScores.filter(s => s.rank <= 10).length;
      const avgAccuracy = playerScores.reduce((sum, s) => sum + (s.accuracy || 0), 0) / totalScores;
      
      const sortedByPP = playerScores.sort((a, b) => (b.pp || 0) - (a.pp || 0));
      const weightedPP = sortedByPP.reduce((sum, score, index) => {
        return sum + (score.pp || 0) * Math.pow(0.95, index);
      }, 0);
      
      await query(`
        INSERT INTO player_stats (
          username, total_scores, avg_rank, best_score, total_pp, weighted_pp,
          first_places, top_10_places, accuracy_avg, last_calculated
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (username) DO UPDATE SET
          total_scores = EXCLUDED.total_scores,
          avg_rank = EXCLUDED.avg_rank,
          best_score = EXCLUDED.best_score,
          total_pp = EXCLUDED.total_pp,
          weighted_pp = EXCLUDED.weighted_pp,
          first_places = EXCLUDED.first_places,
          top_10_places = EXCLUDED.top_10_places,
          accuracy_avg = EXCLUDED.accuracy_avg,
          last_calculated = EXCLUDED.last_calculated
      `, [username, totalScores, avgRank, bestScore, totalPP, weightedPP, 
          firstPlaces, top10Places, avgAccuracy, now]);
      
      // Update skills
      await updatePlayerSkills(username, playerScores);
      
    } catch (err) {
      console.error('Player stats update failed:', err.message);
    }
  }

  // Get all beatmaps for scanning
  async getAllBeatmaps() {
    let allBeatmaps = [];
    let page = 1;
    const maxPages = 30; // Reduced for faster scanning
    
    while (page <= maxPages) {
      try {
        const token = await getAccessToken();
        const res = await axios.get('https://osu.ppy.sh/api/v2/beatmapsets/search', {
          headers: { Authorization: `Bearer ${token}` },
          params: { mode: 'osu', nsfw: false, sort: 'ranked_desc', page, 's': 'ranked' }
        });
        
        const sets = res.data.beatmapsets || [];
        if (sets.length === 0) break;
        
        const beatmaps = sets.flatMap(set =>
          (set.beatmaps || [])
            .filter(bm => bm.difficulty_rating >= 2.0)
            .map(bm => ({ 
              id: bm.id, 
              title: `${set.artist} - ${set.title} [${bm.version}]`, 
              difficulty: bm.difficulty_rating 
            }))
        );
        
        allBeatmaps.push(...beatmaps);
        page++;
        await new Promise(resolve => setTimeout(resolve, 1000));
      } catch (err) {
        console.error(`❌ Failed to fetch beatmap page ${page}:`, err.message);
        break;
      }
    }
    
    return allBeatmaps;
  }

  // Main update function
  async updateLeaderboards() {
    console.log("🔄 Starting leaderboards update...");
    try {
      const beatmaps = await this.getAllBeatmaps();
      await saveProgress("total_beatmaps", beatmaps.length);
      
      // Priority: Update known beatmaps first
      const priorityBeatmaps = await getRows(`
        SELECT beatmap_id, MIN(beatmap_title) AS beatmap_title
        FROM algeria_top50
        GROUP BY beatmap_id
        ORDER BY MIN(last_updated) ASC
        LIMIT 100
      `);
      
      if (priorityBeatmaps.length > 0) {
        console.log(`⚡ Priority scanning ${priorityBeatmaps.length} known beatmaps`);
        for (const bm of priorityBeatmaps) {
          await limiter.schedule(() => this.fetchLeaderboard(bm.beatmap_id, bm.beatmap_title));
        }
      }
      
      // Regular scanning
      let startIndex = parseInt(await getProgress("last_index") || "0", 10);
      if (startIndex >= beatmaps.length) {
        startIndex = 0;
        await saveProgress("last_index", "0");
      }
      
      console.log(`📌 Regular scanning from index ${startIndex}/${beatmaps.length}`);
      
      for (let i = startIndex; i < Math.min(startIndex + this.batchSize, beatmaps.length); i++) {
        const bm = beatmaps[i];
        await limiter.schedule(() => this.fetchLeaderboard(bm.id, bm.title));
        await saveProgress("last_index", i + 1);
        
        if (i % 10 === 0 && global.broadcastToClients) {
          global.broadcastToClients({
            type: 'scan_progress',
            progress: {
              current: i,
              total: beatmaps.length,
              percentage: ((i / beatmaps.length) * 100).toFixed(2)
            }
          });
        }
      }
      
      console.log("✅ Leaderboard update completed");
      
      if (global.broadcastToClients) {
        global.broadcastToClients({
          type: 'scan_complete',
          timestamp: Date.now()
        });
      }
      
      return { success: true, beatmapsScanned: beatmaps.length };
    } catch (err) {
      console.error('❌ Leaderboard update failed:', err.message);
      
      if (global.broadcastToClients) {
        global.broadcastToClients({
          type: 'scan_error',
          error: err.message,
          timestamp: Date.now()
        });
      }
      
      throw err;
    }
  }

  // Send Discord notification
  async sendDiscordNotification(score, beatmapTitle, beatmapId, type) {
    // Placeholder for Discord notification
    // Implementation would depend on specific webhook setup
    return;
  }

  // Get leaderboard statistics
  async getLeaderboardStats() {
    try {
      const stats = await getRows(`
        SELECT 
          COUNT(DISTINCT beatmap_id) as total_beatmaps,
          COUNT(DISTINCT username) as total_players,
          COUNT(*) as total_scores,
          AVG(accuracy) as avg_accuracy,
          MAX(score) as highest_score,
          SUM(pp) as total_pp,
          COUNT(CASE WHEN rank = 1 THEN 1 END) as first_places
        FROM algeria_top50
      `);

      const recentActivity = await getRows(`
        SELECT COUNT(*) as recent_scores
        FROM algeria_top50
        WHERE last_updated > $1
      `, [Date.now() - (24 * 60 * 60 * 1000)]);

      return {
        ...stats[0],
        recentScores: parseInt(recentActivity[0].recent_scores)
      };
    } catch (err) {
      console.error('Failed to get leaderboard stats:', err.message);
      return {};
    }
  }

  // Get top performers on a specific beatmap
  async getBeatmapLeaderboard(beatmapId, limit = 50) {
    try {
      return await getRows(`
        SELECT 
          username, rank, score, accuracy, accuracy_text, mods, pp,
          max_combo, count_300, count_100, count_50, count_miss,
          play_date, last_updated
        FROM algeria_top50
        WHERE beatmap_id = $1
        ORDER BY rank ASC
        LIMIT $2
      `, [beatmapId, limit]);
    } catch (err) {
      console.error('Failed to get beatmap leaderboard:', err.message);
      return [];
    }
  }
}

// Create singleton instance
const leaderboardService = new LeaderboardService();

module.exports = {
  LeaderboardService,
  leaderboardService
};