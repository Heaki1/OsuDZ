// middleware/initialization.js
const { query } = require('../config/db');

// Environment validation
function validateEnvironment() {
  const requiredEnvVars = [
    'OSU_CLIENT_ID', 
    'OSU_CLIENT_SECRET', 
    'DATABASE_URL', 
    'REDIS_URL', 
    'JWT_SECRET'
  ];

  const missing = requiredEnvVars.filter(envVar => !process.env[envVar]);
  
  if (missing.length > 0) {
    console.error('❌ Missing required environment variables:', missing.join(', '));
    process.exit(1);
  }
  
  console.log('✅ Environment variables validated');
}

// Database schema initialization
async function ensureDatabaseSchema() {
  try {
    console.log('🔧 Ensuring database schema...');

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
        join_date TIMESTAMP,
        last_seen TIMESTAMP DEFAULT now(),
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

    console.log('✅ Database schema ensured');
  } catch (err) {
    console.error('❌ Database schema setup failed:', err.message);
    throw err;
  }
}

// Create database indexes for performance
async function createIndexes() {
  try {
    console.log('🔧 Creating database indexes...');

    const indexes = [
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_score ON algeria_top50(score DESC)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_rank ON algeria_top50(rank ASC)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_pp ON algeria_top50(pp DESC)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_updated ON algeria_top50(last_updated DESC)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_username ON algeria_top50(username)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_algeria_beatmap ON algeria_top50(beatmap_id)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_player_stats_pp ON player_stats(weighted_pp DESC)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_player_stats_active ON player_stats(is_active, last_seen DESC)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_player_activity_time ON player_activity(timestamp DESC)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_skill_tracking_username ON skill_tracking(username)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_skill_tracking_time ON skill_tracking(calculated_at DESC)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_discovery_log_timestamp ON player_discovery_log(discovery_timestamp DESC)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_beatmap_metadata_difficulty ON beatmap_metadata(difficulty_rating)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_achievements_category ON achievements(category)',
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_player_achievements_username ON player_achievements(username)'
    ];

    for (const indexSql of indexes) {
      try {
        await query(indexSql);
      } catch (err) {
        if (!err.message.includes('already exists')) {
          console.warn('⚠️ Index creation failed:', err.message);
        }
      }
    }

    console.log('✅ Database indexes created');
  } catch (err) {
    console.error('❌ Index creation failed:', err.message);
    // Don't throw - indexes are nice to have but not critical
  }
}

// Insert default achievements
async function insertDefaultAchievements() {
  try {
    console.log('🏆 Setting up default achievements...');

    const achievements = [
      { name: 'First Steps', description: 'Set your first score', category: 'milestone', icon: '🎯', points: 10 },
      { name: 'Getting Started', description: 'Set 10 scores', category: 'milestone', icon: '📈', points: 25 },
      { name: 'Dedicated Player', description: 'Set 50 scores', category: 'milestone', icon: '💪', points: 75 },
      { name: 'Century Club', description: 'Set 100 scores', category: 'milestone', icon: '💯', points: 150 },
      { name: 'Score Master', description: 'Set 250 scores', category: 'milestone', icon: '🏆', points: 300 },
      { name: 'Perfectionist', description: 'Get your first SS rank', category: 'accuracy', icon: '✨', points: 100 },
      { name: 'Accuracy King', description: 'Get 10 scores with 98%+ accuracy', category: 'accuracy', icon: '🎯', points: 200 },
      { name: 'Speed Demon', description: 'Get a score with DT mod', category: 'mods', icon: '⚡', points: 25 },
      { name: 'Precision Master', description: 'Get a score with HR mod', category: 'mods', icon: '🎯', points: 25 },
      { name: 'In the Dark', description: 'Get a score with HD mod', category: 'mods', icon: '🌑', points: 25 },
      { name: 'Mod Variety', description: 'Use 5 different mod combinations', category: 'mods', icon: '🎮', points: 50 },
      { name: 'Top Player', description: 'Reach #1 on any beatmap', category: 'ranking', icon: '👑', points: 200 },
      { name: 'Consistent Performer', description: 'Get 25 top 10 scores', category: 'ranking', icon: '📊', points: 150 },
      { name: 'PP Collector', description: 'Reach 1000 total PP', category: 'performance', icon: '💎', points: 150 },
      { name: 'PP Hunter', description: 'Reach 2000 weighted PP', category: 'performance', icon: '🔥', points: 250 },
      { name: 'Elite Player', description: 'Reach 5000 weighted PP', category: 'performance', icon: '⭐', points: 500 },
      { name: 'High Roller', description: 'Get a 10M+ score', category: 'score', icon: '💰', points: 100 },
      { name: 'Combo King', description: 'Achieve 1000+ combo', category: 'combo', icon: '🔗', points: 75 },
      { name: 'Difficulty Climber', description: 'Complete a 6+ star map', category: 'difficulty', icon: '🧗', points: 150 },
      { name: 'Extreme Challenge', description: 'Complete an 8+ star map', category: 'difficulty', icon: '🔥', points: 300 }
    ];

    for (const achievement of achievements) {
      await query(`
        INSERT INTO achievements (name, description, category, icon, points)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (name) DO NOTHING
      `, [achievement.name, achievement.description, achievement.category, achievement.icon, achievement.points]);
    }

    console.log('✅ Default achievements created');
  } catch (err) {
    console.error('❌ Achievement setup failed:', err.message);
    // Don't throw - achievements are not critical for basic functionality
  }
}

// Auto-add missing columns (for database migrations)
async function addMissingColumns() {
  try {
    console.log('🔧 Checking for missing columns...');

    // Add missing columns to existing tables
    const columnUpdates = [
      'ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS last_seen TIMESTAMP DEFAULT now()',
      'ALTER TABLE algeria_top50 ADD COLUMN IF NOT EXISTS pp REAL DEFAULT 0',
      'ALTER TABLE algeria_top50 ADD COLUMN IF NOT EXISTS artist TEXT',
      'ALTER TABLE algeria_top50 ADD COLUMN IF NOT EXISTS difficulty_name TEXT',
      'ALTER TABLE algeria_top50 ADD COLUMN IF NOT EXISTS difficulty_rating REAL DEFAULT 0',
      'ALTER TABLE algeria_top50 ADD COLUMN IF NOT EXISTS max_combo INTEGER DEFAULT 0',
      'ALTER TABLE algeria_top50 ADD COLUMN IF NOT EXISTS count_300 INTEGER DEFAULT 0',
      'ALTER TABLE algeria_top50 ADD COLUMN IF NOT EXISTS count_100 INTEGER DEFAULT 0',
      'ALTER TABLE algeria_top50 ADD COLUMN IF NOT EXISTS count_50 INTEGER DEFAULT 0',
      'ALTER TABLE algeria_top50 ADD COLUMN IF NOT EXISTS count_miss INTEGER DEFAULT 0',
      'ALTER TABLE algeria_top50 ADD COLUMN IF NOT EXISTS accuracy REAL',
      'ALTER TABLE algeria_top50 ADD COLUMN IF NOT EXISTS accuracy_text TEXT',
      'ALTER TABLE algeria_top50 ADD COLUMN IF NOT EXISTS mods TEXT',
      'ALTER TABLE algeria_top50 ADD COLUMN IF NOT EXISTS play_date BIGINT',
      'ALTER TABLE algeria_top50 ADD COLUMN IF NOT EXISTS last_updated BIGINT',
      'ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS user_id BIGINT',
      'ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS join_date TIMESTAMP'
    ];

    for (const sql of columnUpdates) {
      try {
        await query(sql);
      } catch (err) {
        // Ignore errors for columns that already exist
        if (!err.message.includes('already exists')) {
          console.warn('⚠️ Column update failed:', err.message);
        }
      }
    }

    console.log('✅ Missing columns check completed');
  } catch (err) {
    console.error('❌ Column update failed:', err.message);
    // Don't throw - this is for backward compatibility
  }
}

// Complete initialization
async function initializeDatabase() {
  await ensureDatabaseSchema();
  await addMissingColumns();
  await createIndexes();
  await insertDefaultAchievements();
}

// Graceful shutdown handler
function setupGracefulShutdown(shutdownCallback) {
  const shutdown = async (signal) => {
    console.log(`\n🛑 Received ${signal}, shutting down gracefully...`);
    
    try {
      if (shutdownCallback) {
        await shutdownCallback();
      }
      console.log('✅ Graceful shutdown completed');
      process.exit(0);
    } catch (err) {
      console.error('❌ Shutdown error:', err.message);
      process.exit(1);
    }
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  process.on('uncaughtException', (err) => {
    console.error('💥 Uncaught exception:', err);
    shutdown('uncaughtException');
  });

  process.on('unhandledRejection', (reason, promise) => {
    console.error('💥 Unhandled rejection at:', promise, 'reason:', reason);
    shutdown('unhandledRejection');
  });
}

module.exports = {
  validateEnvironment,
  initializeDatabase,
  ensureDatabaseSchema,
  createIndexes,
  insertDefaultAchievements,
  addMissingColumns,
  setupGracefulShutdown
};