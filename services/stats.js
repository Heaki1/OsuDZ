const { query, getRows, getRow } = require('../config/db');

// Daily statistics calculation and management
class StatsService {
  constructor() {
    this.dailyStatsTable = 'daily_stats';
  }

  // Calculate daily statistics
  async calculateDailyStats() {
    try {
      const today = new Date().toISOString().split('T')[0];
      const yesterday = Date.now() - (24 * 60 * 60 * 1000);
      
      const [activePlayersResult, newScoresResult, totalPPResult, avgAccuracyResult, topScoreResult] = await Promise.all([
        getRow(`
          SELECT COUNT(DISTINCT username) as count 
          FROM algeria_top50 
          WHERE last_updated > $1
        `, [yesterday]),
        
        getRow(`
          SELECT COUNT(*) as count 
          FROM algeria_top50 
          WHERE last_updated > $1
        `, [yesterday]),
        
        getRow(`
          SELECT SUM(pp) as total 
          FROM algeria_top50 
          WHERE last_updated > $1 AND pp > 0
        `, [yesterday]),
        
        getRow(`
          SELECT AVG(accuracy) as avg 
          FROM algeria_top50 
          WHERE last_updated > $1 AND accuracy::numeric > 0
        `, [yesterday]),
        
        getRow(`
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
        parseInt(activePlayersResult.count) || 0,
        parseInt(newScoresResult.count) || 0,
        parseFloat(totalPPResult.total) || 0,
        parseFloat(avgAccuracyResult.avg) || 0,
        parseInt(topScoreResult.max) || 0
      ]);
      
      console.log(`📊 Daily stats calculated for ${today}`);
      return { success: true, date: today };
    } catch (err) {
      console.error('Daily stats calculation failed:', err.message);
      throw err;
    }
  }

  // Get daily stats for a specific date
  async getDailyStats(date) {
    try {
      return await getRow(`
        SELECT * FROM daily_stats WHERE date = $1
      `, [date]);
    } catch (err) {
      console.error('Failed to get daily stats:', err.message);
      return null;
    }
  }

  // Get daily stats for a date range
  async getDailyStatsRange(startDate, endDate) {
    try {
      return await getRows(`
        SELECT * FROM daily_stats 
        WHERE date BETWEEN $1 AND $2
        ORDER BY date ASC
      `, [startDate, endDate]);
    } catch (err) {
      console.error('Failed to get daily stats range:', err.message);
      return [];
    }
  }

  // Get weekly statistics
  async getWeeklyStats() {
    try {
      const weekAgo = new Date();
      weekAgo.setDate(weekAgo.getDate() - 7);
      const weekAgoStr = weekAgo.toISOString().split('T')[0];
      const today = new Date().toISOString().split('T')[0];

      return await getRows(`
        SELECT 
          date,
          active_players,
          new_scores,
          total_pp_gained,
          average_accuracy,
          top_score
        FROM daily_stats
        WHERE date BETWEEN $1 AND $2
        ORDER BY date ASC
      `, [weekAgoStr, today]);
    } catch (err) {
      console.error('Failed to get weekly stats:', err.message);
      return [];
    }
  }

  // Get monthly statistics
  async getMonthlyStats() {
    try {
      const monthAgo = new Date();
      monthAgo.setMonth(monthAgo.getMonth() - 1);
      const monthAgoStr = monthAgo.toISOString().split('T')[0];
      const today = new Date().toISOString().split('T')[0];

      return await getRows(`
        SELECT 
          date,
          active_players,
          new_scores,
          total_pp_gained,
          average_accuracy,
          top_score
        FROM daily_stats
        WHERE date BETWEEN $1 AND $2
        ORDER BY date ASC
      `, [monthAgoStr, today]);
    } catch (err) {
      console.error('Failed to get monthly stats:', err.message);
      return [];
    }
  }

  // Get aggregated statistics
  async getAggregatedStats(period = 'week') {
    try {
      let dateFilter;
      const now = new Date();

      switch (period) {
        case 'day':
          dateFilter = now.toISOString().split('T')[0];
          break;
        case 'week':
          const weekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
          dateFilter = weekAgo.toISOString().split('T')[0];
          break;
        case 'month':
          const monthAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
          dateFilter = monthAgo.toISOString().split('T')[0];
          break;
        default:
          dateFilter = '1970-01-01'; // All time
      }

      const stats = await getRow(`
        SELECT 
          SUM(active_players) as total_active_players,
          SUM(new_scores) as total_new_scores,
          SUM(total_pp_gained) as total_pp_gained,
          AVG(average_accuracy) as overall_avg_accuracy,
          MAX(top_score) as highest_score,
          COUNT(*) as days_recorded
        FROM daily_stats
        WHERE date >= $1
      `, [dateFilter]);

      return stats;
    } catch (err) {
      console.error('Failed to get aggregated stats:', err.message);
      return null;
    }
  }

  // Get player activity trends
  async getPlayerActivityTrends(days = 30) {
    try {
      const cutoff = new Date();
      cutoff.setDate(cutoff.getDate() - days);
      const cutoffStr = cutoff.toISOString().split('T')[0];

      return await getRows(`
        SELECT 
          date,
          active_players,
          new_scores,
          CASE 
            WHEN LAG(active_players) OVER (ORDER BY date) IS NOT NULL 
            THEN active_players - LAG(active_players) OVER (ORDER BY date)
            ELSE 0
          END as player_change,
          CASE 
            WHEN LAG(new_scores) OVER (ORDER BY date) IS NOT NULL 
            THEN new_scores - LAG(new_scores) OVER (ORDER BY date)
            ELSE 0
          END as score_change
        FROM daily_stats
        WHERE date >= $1
        ORDER BY date ASC
      `, [cutoffStr]);
    } catch (err) {
      console.error('Failed to get player activity trends:', err.message);
      return [];
    }
  }

  // Get top performing days
  async getTopPerformingDays(metric = 'new_scores', limit = 10) {
    try {
      const validMetrics = ['active_players', 'new_scores', 'total_pp_gained', 'top_score'];
      if (!validMetrics.includes(metric)) {
        metric = 'new_scores';
      }

      return await getRows(`
        SELECT 
          date,
          active_players,
          new_scores,
          total_pp_gained,
          average_accuracy,
          top_score
        FROM daily_stats
        WHERE ${metric} IS NOT NULL AND ${metric} > 0
        ORDER BY ${metric} DESC
        LIMIT $1
      `, [limit]);
    } catch (err) {
      console.error('Failed to get top performing days:', err.message);
      return [];
    }
  }

  // Get overall statistics
  async getOverallStats() {
    try {
      const [
        totalStats, 
        recentActivity, 
        topPerformers, 
        modUsage, 
        difficultyDistribution
      ] = await Promise.all([
        getRow(`
          SELECT 
            COUNT(DISTINCT username) as total_players,
            COUNT(*) as total_scores,
            COUNT(DISTINCT beatmap_id) as total_beatmaps,
            AVG(accuracy) as avg_accuracy,
            MAX(score) as highest_score,
            SUM(pp) as total_pp
          FROM algeria_top50
        `),
        
        getRow(`
          SELECT COUNT(*) as active_24h
          FROM algeria_top50
          WHERE last_updated > $1
        `, [Date.now() - (24 * 60 * 60 * 1000)]),
        
        getRows(`
          SELECT username, weighted_pp, first_places
          FROM player_stats
          ORDER BY weighted_pp DESC
          LIMIT 5
        `),
        
        getRows(`
          SELECT 
            mods, 
            COUNT(*) as usage_count,
            AVG(accuracy) as avg_accuracy,
            AVG(pp) as avg_pp
          FROM algeria_top50
          WHERE mods != 'None'
          GROUP BY mods
          ORDER BY usage_count DESC
          LIMIT 10
        `),
        
        getRows(`
          SELECT 
            FLOOR(difficulty_rating) as difficulty_range,
            COUNT(*) as score_count,
            AVG(accuracy) as avg_accuracy
          FROM algeria_top50
          GROUP BY FLOOR(difficulty_rating)
          ORDER BY difficulty_range ASC
        `)
      ]);
      
      return {
        totalStats: {
          ...totalStats,
          active24h: parseInt(recentActivity.active_24h)
        },
        topPerformers,
        modUsage,
        difficultyDistribution
      };
    } catch (err) {
      console.error('Failed to get overall stats:', err.message);
      return null;
    }
  }

  // Get beatmap popularity statistics
  async getBeatmapPopularityStats(limit = 20) {
    try {
      return await getRows(`
        SELECT 
          beatmap_id,
          beatmap_title,
          artist,
          difficulty_name,
          difficulty_rating,
          COUNT(*) as total_scores,
          AVG(accuracy) as avg_accuracy,
          MAX(score) as best_score,
          AVG(pp) as avg_pp
        FROM algeria_top50
        GROUP BY beatmap_id, beatmap_title, artist, difficulty_name, difficulty_rating
        ORDER BY total_scores DESC
        LIMIT $1
      `, [limit]);
    } catch (err) {
      console.error('Failed to get beatmap popularity stats:', err.message);
      return [];
    }
  }

  // Get player growth statistics
  async getPlayerGrowthStats() {
    try {
      const stats = await getRows(`
        SELECT 
          DATE_TRUNC('week', date) as week,
          AVG(new_players) as avg_new_players_per_week,
          SUM(new_players) as total_new_players
        FROM daily_stats
        WHERE date >= CURRENT_DATE - INTERVAL '12 weeks'
        GROUP BY DATE_TRUNC('week', date)
        ORDER BY week ASC
      `);

      const totalPlayers = await getRow(`
        SELECT COUNT(*) as total FROM player_stats WHERE is_active = true
      `);

      return {
        weeklyGrowth: stats,
        totalActivePlayers: parseInt(totalPlayers.total)
      };
    } catch (err) {
      console.error('Failed to get player growth stats:', err.message);
      return { weeklyGrowth: [], totalActivePlayers: 0 };
    }
  }

  // Get performance distribution
  async getPerformanceDistribution() {
    try {
      return await getRows(`
        SELECT 
          CASE 
            WHEN weighted_pp >= 5000 THEN '5000+'
            WHEN weighted_pp >= 3000 THEN '3000-4999'
            WHEN weighted_pp >= 1000 THEN '1000-2999'
            WHEN weighted_pp >= 500 THEN '500-999'
            WHEN weighted_pp >= 100 THEN '100-499'
            ELSE '0-99'
          END as pp_range,
          COUNT(*) as player_count
        FROM player_stats
        WHERE is_active = true AND weighted_pp > 0
        GROUP BY 
          CASE 
            WHEN weighted_pp >= 5000 THEN '5000+'
            WHEN weighted_pp >= 3000 THEN '3000-4999'
            WHEN weighted_pp >= 1000 THEN '1000-2999'
            WHEN weighted_pp >= 500 THEN '500-999'
            WHEN weighted_pp >= 100 THEN '100-499'
            ELSE '0-99'
          END
        ORDER BY 
          CASE 
            WHEN pp_range = '0-99' THEN 1
            WHEN pp_range = '100-499' THEN 2
            WHEN pp_range = '500-999' THEN 3
            WHEN pp_range = '1000-2999' THEN 4
            WHEN pp_range = '3000-4999' THEN 5
            WHEN pp_range = '5000+' THEN 6
          END
      `);
    } catch (err) {
      console.error('Failed to get performance distribution:', err.message);
      return [];
    }
  }

  // Clean up old daily stats
  async cleanupOldStats(keepDays = 365) {
    try {
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - keepDays);
      const cutoffStr = cutoffDate.toISOString().split('T')[0];

      const result = await query(`
        DELETE FROM daily_stats WHERE date < $1
      `, [cutoffStr]);

      if (result.rowCount > 0) {
        console.log(`🧹 Cleaned up ${result.rowCount} old daily stats entries`);
      }

      return result.rowCount;
    } catch (err) {
      console.error('Failed to cleanup old stats:', err.message);
      return 0;
    }
  }

  // Export stats to CSV format
  async exportStatsToCSV(startDate, endDate) {
    try {
      const stats = await this.getDailyStatsRange(startDate, endDate);
      
      const headers = ['date', 'active_players', 'new_scores', 'total_pp_gained', 'average_accuracy', 'top_score'];
      const csvContent = [
        headers.join(','),
        ...stats.map(row => headers.map(header => row[header] || '').join(','))
      ].join('\n');

      return csvContent;
    } catch (err) {
      console.error('Failed to export stats to CSV:', err.message);
      return '';
    }
  }
}

// Create singleton instance
const statsService = new StatsService();

module.exports = {
  StatsService,
  statsService
};