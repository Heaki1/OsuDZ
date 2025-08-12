const express = require('express');
const router = express.Router();
const { getRows, getCached, getCacheKey } = require('../config/db');
const { validateInput } = require('../middleware/validation');

// Main search endpoint
router.get('/', 
  validateInput({
    q: { required: true, minLength: 2, maxLength: 50 }
  }),
  async (req, res) => {
    try {
      const { q, type = 'all', limit = 20 } = req.query;
      const searchTerm = q.trim();
      const cacheKey = getCacheKey('search', type, searchTerm, limit);
      
      const data = await getCached(cacheKey, async () => {
        const results = {};
        
        if (type === 'all' || type === 'players') {
          results.players = await getRows(`
            SELECT 
              username, 
              user_id,
              weighted_pp, 
              total_pp,
              first_places, 
              total_scores,
              accuracy_avg,
              avatar_url,
              last_seen,
              country_rank,
              global_rank
            FROM player_stats
            WHERE username ILIKE $1 AND is_active = true
            ORDER BY weighted_pp DESC
            LIMIT $2
          `, [`%${searchTerm}%`, parseInt(limit)]);
        }
        
        if (type === 'all' || type === 'beatmaps') {
          results.beatmaps = await getRows(`
            SELECT DISTINCT 
              bm.beatmap_id, 
              bm.artist, 
              bm.title, 
              bm.version,
              bm.difficulty_rating,
              bm.length,
              bm.bpm,
              bm.cs,
              bm.ar,
              bm.od,
              bm.hp,
              COUNT(DISTINCT ats.username) as algerian_players,
              MAX(ats.score) as top_score,
              MAX(ats.pp) as top_pp
            FROM beatmap_metadata bm
            LEFT JOIN algeria_top50 ats ON bm.beatmap_id = ats.beatmap_id
            WHERE bm.artist ILIKE $1 OR bm.title ILIKE $1 OR bm.version ILIKE $1
            GROUP BY bm.beatmap_id, bm.artist, bm.title, bm.version, bm.difficulty_rating, bm.length, bm.bpm, bm.cs, bm.ar, bm.od, bm.hp
            ORDER BY algerian_players DESC, top_pp DESC
            LIMIT $2
          `, [`%${searchTerm}%`, parseInt(limit)]);
        }
        
        if (type === 'all' || type === 'scores') {
          results.scores = await getRows(`
            SELECT 
              ats.*,
              bm.length,
              bm.bpm
            FROM algeria_top50 ats
            LEFT JOIN beatmap_metadata bm ON ats.beatmap_id = bm.beatmap_id
            WHERE ats.beatmap_title ILIKE $1 
               OR ats.artist ILIKE $1 
               OR ats.username ILIKE $1
            ORDER BY ats.pp DESC, ats.score DESC
            LIMIT $2
          `, [`%${searchTerm}%`, parseInt(limit)]);
        }
        
        return results;
      }, 600);
      
      res.json({ success: true, query: searchTerm, data });
    } catch (error) {
      console.error('Search error:', error);
      res.status(500).json({ success: false, error: 'Search failed' });
    }
  }
);

// Advanced player search
router.get('/players/advanced', async (req, res) => {
  try {
    const { 
      minPP, maxPP, minAccuracy, maxAccuracy, 
      minScores, maxScores, hasFirstPlace, 
      sortBy = 'weighted_pp', order = 'DESC', 
      limit = 50, offset = 0 
    } = req.query;
    
    let whereClauses = ['is_active = true'];
    let params = [];
    let paramCount = 0;
    
    if (minPP !== undefined && minPP !== '') {
      whereClauses.push(`weighted_pp >= $${++paramCount}`);
      params.push(parseFloat(minPP));
    }
    if (maxPP !== undefined && maxPP !== '') {
      whereClauses.push(`weighted_pp <= $${++paramCount}`);
      params.push(parseFloat(maxPP));
    }
    if (minAccuracy !== undefined && minAccuracy !== '') {
      whereClauses.push(`accuracy_avg >= $${++paramCount}`);
      params.push(parseFloat(minAccuracy));
    }
    if (maxAccuracy !== undefined && maxAccuracy !== '') {
      whereClauses.push(`accuracy_avg <= ${++paramCount}`);
      params.push(parseFloat(maxAccuracy));
    }
    if (minScores !== undefined && minScores !== '') {
      whereClauses.push(`total_scores >= ${++paramCount}`);
      params.push(parseInt(minScores));
    }
    if (maxScores !== undefined && maxScores !== '') {
      whereClauses.push(`total_scores <= ${++paramCount}`);
      params.push(parseInt(maxScores));
    }
    if (hasFirstPlace === 'true') {
      whereClauses.push(`first_places > 0`);
    }
    
    const allowedSort = ['weighted_pp', 'total_pp', 'accuracy_avg', 'first_places', 'total_scores', 'avg_rank'];
    const sortColumn = allowedSort.includes(sortBy) ? sortBy : 'weighted_pp';
    const sortOrder = order.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
    
    params.push(parseInt(limit), parseInt(offset));
    
    const data = await getRows(`
      SELECT *,
             ROW_NUMBER() OVER (ORDER BY ${sortColumn} ${sortOrder}) as search_rank
      FROM player_stats 
      WHERE ${whereClauses.join(' AND ')}
      ORDER BY ${sortColumn} ${sortOrder}
      LIMIT ${++paramCount} OFFSET ${++paramCount}
    `, params);
    
    res.json({ 
      success: true, 
      data,
      meta: {
        filters: req.query,
        sort: sortColumn,
        order: sortOrder,
        count: data.length,
        hasMore: data.length === parseInt(limit)
      }
    });
  } catch (error) {
    console.error('Advanced player search error:', error);
    res.status(500).json({ success: false, error: 'Advanced search failed' });
  }
});

// Beatmap search with filters
router.get('/beatmaps/advanced', async (req, res) => {
  try {
    const { 
      minDiff, maxDiff, minLength, maxLength,
      minBPM, maxBPM, minCS, maxCS, minAR, maxAR,
      genre, hasAlgerianScores,
      sortBy = 'difficulty_rating', order = 'DESC',
      limit = 50, offset = 0 
    } = req.query;
    
    let whereClauses = [];
    let params = [];
    let paramCount = 0;
    
    if (minDiff !== undefined && minDiff !== '') {
      whereClauses.push(`bm.difficulty_rating >= ${++paramCount}`);
      params.push(parseFloat(minDiff));
    }
    if (maxDiff !== undefined && maxDiff !== '') {
      whereClauses.push(`bm.difficulty_rating <= ${++paramCount}`);
      params.push(parseFloat(maxDiff));
    }
    if (minLength !== undefined && minLength !== '') {
      whereClauses.push(`bm.length >= ${++paramCount}`);
      params.push(parseInt(minLength));
    }
    if (maxLength !== undefined && maxLength !== '') {
      whereClauses.push(`bm.length <= ${++paramCount}`);
      params.push(parseInt(maxLength));
    }
    if (minBPM !== undefined && minBPM !== '') {
      whereClauses.push(`bm.bpm >= ${++paramCount}`);
      params.push(parseFloat(minBPM));
    }
    if (maxBPM !== undefined && maxBPM !== '') {
      whereClauses.push(`bm.bpm <= ${++paramCount}`);
      params.push(parseFloat(maxBPM));
    }
    if (minCS !== undefined && minCS !== '') {
      whereClauses.push(`bm.cs >= ${++paramCount}`);
      params.push(parseFloat(minCS));
    }
    if (maxCS !== undefined && maxCS !== '') {
      whereClauses.push(`bm.cs <= ${++paramCount}`);
      params.push(parseFloat(maxCS));
    }
    if (minAR !== undefined && minAR !== '') {
      whereClauses.push(`bm.ar >= ${++paramCount}`);
      params.push(parseFloat(minAR));
    }
    if (maxAR !== undefined && maxAR !== '') {
      whereClauses.push(`bm.ar <= ${++paramCount}`);
      params.push(parseFloat(maxAR));
    }
    if (hasAlgerianScores === 'true') {
      whereClauses.push(`algerian_players > 0`);
    }
    
    const whereClause = whereClauses.length ? `WHERE ${whereClauses.join(' AND ')}` : '';
    
    const allowedSort = ['difficulty_rating', 'length', 'bpm', 'algerian_players', 'top_pp'];
    const sortColumn = allowedSort.includes(sortBy) ? sortBy : 'difficulty_rating';
    const sortOrder = order.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
    
    params.push(parseInt(limit), parseInt(offset));
    
    const data = await getRows(`
      SELECT 
        bm.*,
        COUNT(DISTINCT ats.username) as algerian_players,
        MAX(ats.score) as top_score,
        MAX(ats.pp) as top_pp,
        AVG(ats.accuracy) as avg_accuracy
      FROM beatmap_metadata bm
      LEFT JOIN algeria_top50 ats ON bm.beatmap_id = ats.beatmap_id
      ${whereClause}
      GROUP BY bm.beatmap_id, bm.beatmapset_id, bm.artist, bm.title, bm.version, 
               bm.creator, bm.difficulty_rating, bm.cs, bm.ar, bm.od, bm.hp, 
               bm.length, bm.bpm, bm.max_combo, bm.play_count, bm.favorite_count,
               bm.ranked_date, bm.last_updated
      ORDER BY ${sortColumn} ${sortOrder}
      LIMIT ${++paramCount} OFFSET ${++paramCount}
    `, params);
    
    res.json({ 
      success: true, 
      data,
      meta: {
        filters: req.query,
        sort: sortColumn,
        order: sortOrder,
        count: data.length,
        hasMore: data.length === parseInt(limit)
      }
    });
  } catch (error) {
    console.error('Advanced beatmap search error:', error);
    res.status(500).json({ success: false, error: 'Advanced beatmap search failed' });
  }
});

// Search suggestions/autocomplete
router.get('/suggestions', async (req, res) => {
  try {
    const { q, type = 'players', limit = 10 } = req.query;
    
    if (!q || q.length < 2) {
      return res.json({ success: true, data: [] });
    }
    
    const searchTerm = q.trim();
    const cacheKey = getCacheKey('search', 'suggestions', type, searchTerm, limit);
    
    const data = await getCached(cacheKey, async () => {
      let results = [];
      
      if (type === 'players') {
        results = await getRows(`
          SELECT username, weighted_pp, avatar_url
          FROM player_stats
          WHERE username ILIKE $1 AND is_active = true
          ORDER BY weighted_pp DESC
          LIMIT $2
        `, [`%${searchTerm}%`, parseInt(limit)]);
      } else if (type === 'beatmaps') {
        results = await getRows(`
          SELECT DISTINCT 
            bm.beatmap_id,
            CONCAT(bm.artist, ' - ', bm.title, ' [', bm.version, ']') as full_title,
            bm.difficulty_rating
          FROM beatmap_metadata bm
          WHERE bm.artist ILIKE $1 OR bm.title ILIKE $1 OR bm.version ILIKE $1
          ORDER BY bm.difficulty_rating DESC
          LIMIT $2
        `, [`%${searchTerm}%`, parseInt(limit)]);
      }
      
      return results;
    }, 300);
    
    res.json({ success: true, data });
  } catch (error) {
    console.error('Search suggestions error:', error);
    res.status(500).json({ success: false, error: 'Failed to get suggestions' });
  }
});

// Search history and trending searches (if you want to implement this)
router.get('/trending', async (req, res) => {
  try {
    const { limit = 10 } = req.query;
    const cacheKey = getCacheKey('search', 'trending', limit);
    
    const data = await getCached(cacheKey, async () => {
      // Get most active players recently
      const trendingPlayers = await getRows(`
        SELECT 
          ps.username,
          ps.weighted_pp,
          ps.avatar_url,
          COUNT(ats.username) as recent_scores
        FROM player_stats ps
        LEFT JOIN algeria_top50 ats ON ps.username = ats.username 
          AND ats.last_updated > $1
        WHERE ps.is_active = true
        GROUP BY ps.username, ps.weighted_pp, ps.avatar_url
        HAVING COUNT(ats.username) > 0
        ORDER BY recent_scores DESC, ps.weighted_pp DESC
        LIMIT $2
      `, [Date.now() - (7 * 24 * 60 * 60 * 1000), parseInt(limit)]);
      
      // Get most popular beatmaps recently
      const trendingBeatmaps = await getRows(`
        SELECT 
          bm.beatmap_id,
          CONCAT(bm.artist, ' - ', bm.title, ' [', bm.version, ']') as full_title,
          bm.difficulty_rating,
          COUNT(ats.username) as recent_plays
        FROM beatmap_metadata bm
        INNER JOIN algeria_top50 ats ON bm.beatmap_id = ats.beatmap_id
        WHERE ats.last_updated > $1
        GROUP BY bm.beatmap_id, bm.artist, bm.title, bm.version, bm.difficulty_rating
        ORDER BY recent_plays DESC, bm.difficulty_rating DESC
        LIMIT $2
      `, [Date.now() - (7 * 24 * 60 * 60 * 1000), parseInt(limit)]);
      
      return {
        trendingPlayers,
        trendingBeatmaps
      };
    }, 1800); // Cache for 30 minutes
    
    res.json({ success: true, data });
  } catch (error) {
    console.error('Trending search error:', error);
    res.status(500).json({ success: false, error: 'Failed to get trending searches' });
  }
});

// Search within specific player's scores
router.get('/player/:username/scores', async (req, res) => {
  try {
    const { username } = req.params;
    const { q, minPP, maxPP, mods, sortBy = 'pp', order = 'DESC', limit = 50 } = req.query;
    
    let whereClauses = ['username ILIKE $1'];
    let params = [`%${username}%`];
    let paramCount = 1;
    
    if (q) {
      whereClauses.push(`(beatmap_title ILIKE ${++paramCount} OR artist ILIKE ${paramCount})`);
      params.push(`%${q}%`);
    }
    if (minPP !== undefined && minPP !== '') {
      whereClauses.push(`pp >= ${++paramCount}`);
      params.push(parseFloat(minPP));
    }
    if (maxPP !== undefined && maxPP !== '') {
      whereClauses.push(`pp <= ${++paramCount}`);
      params.push(parseFloat(maxPP));
    }
    if (mods) {
      whereClauses.push(`mods = ${++paramCount}`);
      params.push(mods);
    }
    
    const allowedSort = ['pp', 'score', 'accuracy', 'rank', 'last_updated'];
    const sortColumn = allowedSort.includes(sortBy) ? sortBy : 'pp';
    const sortOrder = order.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
    
    params.push(parseInt(limit));
    
    const data = await getRows(`
      SELECT * FROM algeria_top50
      WHERE ${whereClauses.join(' AND ')}
      ORDER BY ${sortColumn} ${sortOrder}
      LIMIT ${++paramCount}
    `, params);
    
    res.json({ 
      success: true, 
      data,
      meta: {
        username,
        query: q,
        filters: req.query,
        count: data.length
      }
    });
  } catch (error) {
    console.error('Player scores search error:', error);
    res.status(500).json({ success: false, error: 'Failed to search player scores' });
  }
});

module.exports = router;