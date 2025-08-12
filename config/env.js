// Environment validation and configuration
require('dotenv').config();

// Required environment variables
const requiredEnvVars = [
  'OSU_CLIENT_ID', 
  'OSU_CLIENT_SECRET', 
  'DATABASE_URL', 
  'REDIS_URL', 
  'JWT_SECRET'
];

// Validate required environment variables
function validateEnvironment() {
  const missing = [];
  
  requiredEnvVars.forEach(envVar => {
    if (!process.env[envVar]) {
      missing.push(envVar);
    }
  });
  
  if (missing.length > 0) {
    console.error(`❌ Missing required environment variables: ${missing.join(', ')}`);
    process.exit(1);
  }
  
  console.log('✅ Environment variables validated');
}

// Environment configuration with defaults
const config = {
  // Server configuration
  PORT: process.env.PORT || 3000,
  NODE_ENV: process.env.NODE_ENV || 'development',
  
  // Database configuration
  DATABASE_URL: process.env.DATABASE_URL,
  
  // Redis configuration
  REDIS_URL: process.env.REDIS_URL,
  
  // osu! API configuration
  OSU_CLIENT_ID: process.env.OSU_CLIENT_ID,
  OSU_CLIENT_SECRET: process.env.OSU_CLIENT_SECRET,
  
  // Security configuration
  JWT_SECRET: process.env.JWT_SECRET,
  API_KEY: process.env.API_KEY,
  
  // External services
  DISCORD_WEBHOOK_URL: process.env.DISCORD_WEBHOOK_URL,
  
  // Job scheduling (with defaults)
  DISCOVERY_INTERVAL_MS: parseInt(process.env.DISCOVERY_INTERVAL_MS) || 4 * 60 * 60 * 1000, // 4 hours
  RANKINGS_INTERVAL_MS: parseInt(process.env.RANKINGS_INTERVAL_MS) || 30 * 60 * 1000, // 30 minutes
  LEADERBOARD_INTERVAL_MS: parseInt(process.env.LEADERBOARD_INTERVAL_MS) || 2 * 60 * 60 * 1000, // 2 hours
  DAILY_JOB_HOUR_UTC: parseInt(process.env.DAILY_JOB_HOUR_UTC) || 0, // Midnight UTC
  
  // Feature flags
  ENABLE_PLAYER_DISCOVERY: process.env.ENABLE_PLAYER_DISCOVERY !== 'false',
  ENABLE_DISCORD_NOTIFICATIONS: process.env.ENABLE_DISCORD_NOTIFICATIONS === 'true',
  ENABLE_WEBSOCKETS: process.env.ENABLE_WEBSOCKETS !== 'false',
  
  // Rate limiting
  API_RATE_LIMIT_WINDOW_MS: parseInt(process.env.API_RATE_LIMIT_WINDOW_MS) || 15 * 60 * 1000,
  API_RATE_LIMIT_MAX: parseInt(process.env.API_RATE_LIMIT_MAX) || 100,
  ADMIN_RATE_LIMIT_MAX: parseInt(process.env.ADMIN_RATE_LIMIT_MAX) || 20,
  
  // osu! API limits
  OSU_API_MAX_CONCURRENT: parseInt(process.env.OSU_API_MAX_CONCURRENT) || 3,
  OSU_API_MIN_TIME: parseInt(process.env.OSU_API_MIN_TIME) || 600,
  
  // Cache settings
  DEFAULT_CACHE_TTL: parseInt(process.env.DEFAULT_CACHE_TTL) || 300, // 5 minutes
  PLAYER_CACHE_TTL: parseInt(process.env.PLAYER_CACHE_TTL) || 300,
  RANKINGS_CACHE_TTL: parseInt(process.env.RANKINGS_CACHE_TTL) || 300,
  SEARCH_CACHE_TTL: parseInt(process.env.SEARCH_CACHE_TTL) || 600,
  ANALYTICS_CACHE_TTL: parseInt(process.env.ANALYTICS_CACHE_TTL) || 900,
  
  // Cleanup settings
  SKILL_CLEANUP_CUTOFF_DAYS: parseInt(process.env.SKILL_CLEANUP_CUTOFF_DAYS) || 90,
  
  // Logging
  LOG_LEVEL: process.env.LOG_LEVEL || 'INFO'
};

// Development vs Production settings
const isDevelopment = config.NODE_ENV === 'development';
const isProduction = config.NODE_ENV === 'production';

// Export configuration
module.exports = {
  config,
  validateEnvironment,
  isDevelopment,
  isProduction,
  
  // Convenience getters
  get PORT() { return config.PORT; },
  get DATABASE_URL() { return config.DATABASE_URL; },
  get REDIS_URL() { return config.REDIS_URL; },
  get OSU_CLIENT_ID() { return config.OSU_CLIENT_ID; },
  get OSU_CLIENT_SECRET() { return config.OSU_CLIENT_SECRET; },
  get JWT_SECRET() { return config.JWT_SECRET; }
};
