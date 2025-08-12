const express = require('express');
const router = express.Router();
const { testConnection } = require('../config/db');
const { testRedisConnection } = require('../config/redis');
const { testApiConnection } = require('../services/osuApi');

// Health check endpoint
router.get('/', async (req, res) => {
  try {
    const [dbHealthy, redisHealthy, apiHealthy] = await Promise.all([
      testConnection(),
      testRedisConnection(),
      testApiConnection()
    ]);

    const status = dbHealthy && redisHealthy && apiHealthy ? 'healthy' : 'unhealthy';
    const statusCode = status === 'healthy' ? 200 : 503;

    res.status(statusCode).json({
      status,
      timestamp: new Date().toISOString(),
      services: {
        database: dbHealthy ? 'connected' : 'disconnected',
        redis: redisHealthy ? 'connected' : 'disconnected',
        osuApi: apiHealthy ? 'connected' : 'disconnected'
      },
      uptime: process.uptime(),
      memory: process.memoryUsage(),
      version: process.env.npm_package_version || '1.0.0'
    });
  } catch (error) {
    res.status(503).json({
      status: 'unhealthy',
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Detailed health check
router.get('/detailed', async (req, res) => {
  try {
    const healthChecks = {};

    // Database health
    try {
      await testConnection();
      healthChecks.database = { status: 'healthy', responseTime: 0 };
    } catch (err) {
      healthChecks.database = { status: 'unhealthy', error: err.message };
    }

    // Redis health
    try {
      const start = Date.now();
      await testRedisConnection();
      healthChecks.redis = { status: 'healthy', responseTime: Date.now() - start };
    } catch (err) {
      healthChecks.redis = { status: 'unhealthy', error: err.message };
    }

    // osu! API health
    try {
      const start = Date.now();
      await testApiConnection();
      healthChecks.osuApi = { status: 'healthy', responseTime: Date.now() - start };
    } catch (err) {
      healthChecks.osuApi = { status: 'unhealthy', error: err.message };
    }

    const overallHealthy = Object.values(healthChecks).every(check => check.status === 'healthy');

    res.status(overallHealthy ? 200 : 503).json({
      status: overallHealthy ? 'healthy' : 'unhealthy',
      checks: healthChecks,
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
      memory: process.memoryUsage(),
      environment: process.env.NODE_ENV || 'development'
    });
  } catch (error) {
    res.status(500).json({
      status: 'error',
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

module.exports = router;