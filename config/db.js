const { Pool } = require('pg');

// Database connection pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

// Database helper functions
async function query(sql, params = []) { 
  const client = await pool.connect();
  try {
    return await client.query(sql, params);
  } finally {
    client.release();
  }
}

async function getRows(sql, params = []) { 
  return (await query(sql, params)).rows; 
}

async function getRow(sql, params = []) { 
  return (await query(sql, params)).rows[0]; 
}

// Database schema initialization
async function ensureTables() {
  try {
    // Core leaderboard table
    await query(`
      CREATE TABLE IF NOT EXISTS algeria_top50 (
        beatmap_id BIGINT,
        beatmap_title TEXT,
        artist TEXT,
        difficulty_name TEXT,
        player_id BIGINT,
        username TEXT,
        rank INTEGER,
        score BIGINT,
        accuracy REAL,
        accuracy_text TEXT,
        mods TEXT,
        pp REAL DEFAULT 0,
        difficulty_rating REAL DEFAULT 0,
        max_combo INTEGER DEFAULT 0,
        count_300 INTEGER DEFAULT 0,
        count_100 INTEGER DEFAULT 0,
        count_50 INTEGER DEFAULT 0,
        count_miss INTEGER DEFAULT 0,
        play_date BIGINT,
        last_updated BIGINT,
        PRIMARY KEY (beatmap_id, player_id)
      );
    `);

    // Enhanced player statistics
    await query(`
      CREATE TABLE IF NOT EXISTS player_stats (
        username TEXT PRIMARY KEY,
        user_id BIGINT UNIQUE,
        total_scores INTEGER DEFAULT 0,
        avg_rank REAL DEFAULT 0,
        best_score BIGINT DEFAULT 0,
        total_pp REAL DEFAULT 0,
        weighted_pp REAL DEFAULT 0,
        first_places INTEGER DEFAULT 0,
        top_10_places INTEGER DEFAULT 0,
        accuracy_avg REAL DEFAULT 0,
        playcount INTEGER DEFAULT 0,
        total_playtime INTEGER DEFAULT 0,
        level REAL DEFAULT 1,
        global_rank INTEGER DEFAULT 0,
        country_rank INTEGER DEFAULT 0,
        join_date BIGINT,
        last_seen BIGINT,
        last_calculated BIGINT DEFAULT 0,
        is_active BOOLEAN DEFAULT true,
        avatar_url TEXT,
        cover_url TEXT
      );
    `);

    // Skill tracking system
    await query(`
      CREATE TABLE IF NOT EXISTS skill_tracking (
        id SERIAL PRIMARY KEY,
        username TEXT,
        skill_type TEXT,
        skill_value REAL,
        confidence REAL DEFAULT 0.5,
        calculated_at BIGINT,
        FOREIGN KEY (username) REFERENCES player_stats(username) ON DELETE CASCADE
      );
    `);

    // Achievements system
    await query(`
      CREATE TABLE IF NOT EXISTS achievements (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE,
        description TEXT,
        category TEXT,
        icon TEXT,
        points INTEGER DEFAULT 0,
        created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000
      );
    `);

    await query(`
      CREATE TABLE IF NOT EXISTS player_achievements (
        id SERIAL PRIMARY KEY,
        username TEXT,
        achievement_id INTEGER,
        unlocked_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000,
        progress REAL DEFAULT 1.0,
        FOREIGN KEY (username) REFERENCES player_stats(username) ON DELETE CASCADE,
        FOREIGN KEY (achievement_id) REFERENCES achievements(id),
        UNIQUE(username, achievement_id)
      );
    `);

    // Activity tracking
    await query(`
      CREATE TABLE IF NOT EXISTS player_activity (
        id SERIAL PRIMARY KEY,
        username TEXT,
        activity_type TEXT,
        activity_data JSONB,
        timestamp BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000,
        FOREIGN KEY (username) REFERENCES player_stats(username) ON DELETE CASCADE
      );
    `);

    // Daily statistics
    await query(`
      CREATE TABLE IF NOT EXISTS daily_stats (
        date DATE PRIMARY KEY,
        active_players INTEGER DEFAULT 0,
        new_scores INTEGER DEFAULT 0,
        new_players INTEGER DEFAULT 0,
        total_pp_gained REAL DEFAULT 0,
        average_accuracy REAL DEFAULT 0,
        top_score BIGINT DEFAULT 0
      );
    `);

    // Beatmap metadata
    await query(`
      CREATE TABLE IF NOT EXISTS beatmap_metadata (
        beatmap_id BIGINT PRIMARY KEY,
        beatmapset_id BIGINT,
        artist TEXT,
        title TEXT,
        version TEXT,
        creator TEXT,
        difficulty_rating REAL,
        cs REAL,
        ar REAL,
        od REAL,
        hp REAL,
        length INTEGER,
        bpm REAL,
        max_combo INTEGER,
        tags TEXT[],
        genre_id INTEGER,
        language_id INTEGER,
        play_count INTEGER DEFAULT 0,
        favorite_count INTEGER DEFAULT 0,
        ranked_date BIGINT,
        last_updated BIGINT
      );
    `);

    // Player discovery tables
    await query(`
      CREATE TABLE IF NOT EXISTS player_discovery_log (
        id SERIAL PRIMARY KEY,
        username TEXT,
        user_id BIGINT,
        discovery_method TEXT,
        discovery_timestamp BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000,
        is_new_player BOOLEAN DEFAULT false,
        player_data JSONB
      );
    `);

    // Social features
    await query(`
      CREATE TABLE IF NOT EXISTS player_relationships (
        id SERIAL PRIMARY KEY,
        follower_username TEXT,
        following_username TEXT,
        relationship_type TEXT DEFAULT 'follow',
        created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000,
        FOREIGN KEY (follower_username) REFERENCES player_stats(username) ON DELETE CASCADE,
        FOREIGN KEY (following_username) REFERENCES player_stats(username) ON DELETE CASCADE,
        UNIQUE(follower_username, following_username)
      );
    `);

    await query(`
      CREATE TABLE IF NOT EXISTS player_comments (
        id SERIAL PRIMARY KEY,
        target_username TEXT,
        commenter_username TEXT,
        comment_text TEXT,
        created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000,
        is_deleted BOOLEAN DEFAULT false,
        FOREIGN KEY (target_username) REFERENCES player_stats(username) ON DELETE CASCADE,
        FOREIGN KEY (commenter_username) REFERENCES player_stats(username) ON DELETE CASCADE
      );
    `);

    // Create indexes for performance
    const indexes = [
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_score ON algeria_top50(score DESC)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_rank ON algeria_top50(rank ASC)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_pp ON algeria_top50(pp DESC)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_updated ON algeria_top50(last_updated DESC)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_username ON algeria_top50(username)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_beatmap ON algeria_top50(beatmap_id)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_player_stats_pp ON player_stats(weighted_pp DESC)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_player_activity_time ON player_activity(timestamp DESC)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_skill_tracking_username ON skill_tracking(username)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_discovery_log_timestamp ON player_discovery_log(discovery_timestamp DESC)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_beatmap_metadata_difficulty ON beatmap_metadata(difficulty_rating)'
    ];

    for (const indexSql of indexes) {
      try {
        await query(indexSql);
      } catch (err) {
        if (!err.message.includes('already exists')) {
          console.warn('Index creation failed:', err.message);
        }
      }
    }

    console.log('✅ Database schema ensured');
  } catch (err) {
    console.error('❌ Database setup failed:', err.message);
    throw err;
  }
}

// Test database connection
async function testConnection() {
  try {
    const res = await pool.query('SELECT current_database(), current_schema()');
    console.log(`Connected to DB: ${res.rows[0].current_database}, schema: ${res.rows[0].current_schema}`);
    return true;
  } catch (err) {
    console.error('DB connection test failed:', err.message);
    return false;
  }
}

// Graceful shutdown
async function closePool() {
  try {
    await pool.end();
    console.log('🗄️ Database pool closed');
  } catch (err) {
    console.error('Database pool close error:', err.message);
  }
}

module.exports = {
  pool,
  query,
  getRows,
  getRow,
  ensureTables,
  testConnection,
  closePool
};