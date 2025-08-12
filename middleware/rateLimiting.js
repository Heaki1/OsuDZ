const rateLimit = require('express-rate-limit');

// Enhanced rate limiting
const createRateLimit = (windowMs, max, message) => rateLimit({
  windowMs,
  max: (req) => req.user ? max * 2 : max, // Higher limits for authenticated users
  message: { success: false, error: message },
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => {
    // Use user ID if authenticated, otherwise IP
    return req.user ? `user:${req.user.id}` : `ip:${req.ip}`;
  },
  skip: (req) => {
    // Skip rate limiting for health checks
    return req.path === '/health';
  }
});

// Different rate limits for different endpoints
const apiRateLimit = createRateLimit(
  15 * 60 * 1000, // 15 minutes
  100, // requests per window
  'Too many API requests'
);

const adminRateLimit = createRateLimit(
  15 * 60 * 1000, // 15 minutes
  20, // requests per window
  'Too many admin requests'
);

const authRateLimit = createRateLimit(
  15 * 60 * 1000, // 15 minutes
  5, // requests per window
  'Too many authentication attempts'
);

const searchRateLimit = createRateLimit(
  1 * 60 * 1000, // 1 minute
  30, // requests per window
  'Too many search requests'
);

const heavyRateLimit = createRateLimit(
  5 * 60 * 1000, // 5 minutes
  10, // requests per window
  'Too many resource-intensive requests'
);

module.exports = {
  apiRateLimit,
  adminRateLimit,
  authRateLimit,
  searchRateLimit,
  heavyRateLimit,
  createRateLimit
};