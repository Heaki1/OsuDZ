const rateLimit = require('express-rate-limit');
const helmet = require('helmet');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcrypt');
const { config } = require('./env');

// Enhanced security middleware
const securityMiddleware = helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'", "'unsafe-inline'"],
      styleSrc: ["'self'", "'unsafe-inline'", "https://cdnjs.cloudflare.com"],
      fontSrc: ["'self'", "https://cdnjs.cloudflare.com"]
    }
  }
});

// Enhanced rate limiting
const createRateLimit = (windowMs, max, message) => rateLimit({
  windowMs,
  max: (req) => req.user ? max * 2 : max, // Higher limits for authenticated users
  message: { success: false, error: message },
  standardHeaders: true,
  legacyHeaders: false,
});

// Rate limiters for different endpoints
const apiRateLimit = createRateLimit(
  config.API_RATE_LIMIT_WINDOW_MS,
  config.API_RATE_LIMIT_MAX,
  'Too many API requests'
);

const adminRateLimit = createRateLimit(
  config.API_RATE_LIMIT_WINDOW_MS,
  config.ADMIN_RATE_LIMIT_MAX,
  'Too many admin requests'
);

// Authentication middleware
const authenticateToken = (req, res, next) => {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];
  
  if (!token) {
    return res.status(401).json({ success: false, error: 'Access token required' });
  }
  
  jwt.verify(token, config.JWT_SECRET, (err, user) => {
    if (err) {
      return res.status(403).json({ success: false, error: 'Invalid or expired token' });
    }
    req.user = user;
    next();
  });
};

// Admin authentication middleware
const requireAdmin = (req, res, next) => {
  if (!req.user || req.user.role !== 'admin') {
    return res.status(403).json({ success: false, error: 'Admin access required' });
  }
  next();
};

// Input validation helper
const validateInput = (rules) => {
  return (req, res, next) => {
    const errors = [];
    
    Object.entries(rules).forEach(([field, rule]) => {
      const value = req.params[field] || req.query[field] || req.body[field];
      
      if (rule.required && (!value || value.toString().trim() === '')) {
        errors.push(`${field} is required`);
      }
      
      if (value && rule.type === 'number' && isNaN(Number(value))) {
        errors.push(`${field} must be a number`);
      }
      
      if (value && rule.type === 'integer' && (!Number.isInteger(Number(value)) || Number(value) < 0)) {
        errors.push(`${field} must be a positive integer`);
      }
      
      if (value && rule.min !== undefined && Number(value) < rule.min) {
        errors.push(`${field} must be at least ${rule.min}`);
      }
      
      if (value && rule.max !== undefined && Number(value) > rule.max) {
        errors.push(`${field} must be at most ${rule.max}`);
      }
      
      if (value && rule.minLength && value.toString().length < rule.minLength) {
        errors.push(`${field} must be at least ${rule.minLength} characters`);
      }
      
      if (value && rule.maxLength && value.toString().length > rule.maxLength) {
        errors.push(`${field} must be at most ${rule.maxLength} characters`);
      }
      
      if (value && rule.pattern && !rule.pattern.test(value.toString())) {
        errors.push(`${field} has invalid format`);
      }
      
      if (value && rule.enum && !rule.enum.includes(value)) {
        errors.push(`${field} must be one of: ${rule.enum.join(', ')}`);
      }
    });
    
    if (errors.length > 0) {
      return res.status(400).json({ success: false, errors });
    }
    
    next();
  };
};

// Sanitize input data
const sanitizeInput = (data) => {
  if (typeof data === 'string') {
    return data.trim().replace(/[<>]/g, '');
  }
  if (typeof data === 'object' && data !== null) {
    const sanitized = {};
    for (const [key, value] of Object.entries(data)) {
      sanitized[key] = sanitizeInput(value);
    }
    return sanitized;
  }
  return data;
};

// Input sanitization middleware
const sanitizeInputMiddleware = (req, res, next) => {
  req.body = sanitizeInput(req.body);
  req.query = sanitizeInput(req.query);
  req.params = sanitizeInput(req.params);
  next();
};

// JWT token utilities
const generateToken = (payload, expiresIn = '24h') => {
  return jwt.sign(payload, config.JWT_SECRET, { expiresIn });
};

const verifyToken = (token) => {
  try {
    return jwt.verify(token, config.JWT_SECRET);
  } catch (err) {
    return null;
  }
};

// Password utilities
const hashPassword = async (password, rounds = 10) => {
  return await bcrypt.hash(password, rounds);
};

const comparePassword = async (password, hash) => {
  return await bcrypt.compare(password, hash);
};

// CORS configuration
const corsOptions = {
  origin: (origin, callback) => {
    // Allow all origins in development
    if (config.NODE_ENV === 'development') {
      return callback(null, true);
    }
    
    // In production, check allowed origins
    const allowedOrigins = (process.env.ALLOWED_ORIGINS || '').split(',').filter(Boolean);
    
    if (!origin || allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error('Not allowed by CORS'));
    }
  },
  credentials: true,
  optionsSuccessStatus: 200
};

// Security headers middleware
const securityHeaders = (req, res, next) => {
  // Security headers
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  
  // Remove server fingerprinting
  res.removeHeader('X-Powered-By');
  
  next();
};

// Request logging middleware
const requestLogger = (req, res, next) => {
  const start = Date.now();
  const { method, originalUrl, ip } = req;
  const userAgent = req.get('User-Agent') || 'unknown';
  
  res.on('finish', () => {
    const duration = Date.now() - start;
    const { statusCode } = res;
    
    console.log(`[${new Date().toISOString()}] ${method} ${originalUrl} ${statusCode} ${duration}ms - ${ip} - ${userAgent}`);
  });
  
  next();
};

// Error handling middleware
const errorHandler = (err, req, res, next) => {
  console.error('❌ Server Error:', err.stack || err.message);
  
  // Don't expose internal errors in production
  const isDev = config.NODE_ENV === 'development';
  
  if (err.name === 'ValidationError') {
    return res.status(400).json({ 
      success: false, 
      error: 'Validation failed', 
      details: isDev ? err.message : undefined 
    });
  }
  
  if (err.code === 'ECONNREFUSED') {
    return res.status(503).json({ 
      success: false, 
      error: 'Database connection failed' 
    });
  }
  
  if (err.name === 'JsonWebTokenError') {
    return res.status(401).json({ 
      success: false, 
      error: 'Invalid token' 
    });
  }
  
  res.status(500).json({ 
    success: false, 
    error: isDev ? err.message : 'Internal server error' 
  });
};

// 404 handler
const notFoundHandler = (req, res) => {
  res.status(404).json({ 
    success: false, 
    error: 'Endpoint not found' 
  });
};

module.exports = {
  securityMiddleware,
  apiRateLimit,
  adminRateLimit,
  authenticateToken,
  requireAdmin,
  validateInput,
  sanitizeInput,
  sanitizeInputMiddleware,
  generateToken,
  verifyToken,
  hashPassword,
  comparePassword,
  corsOptions,
  securityHeaders,
  requestLogger,
  errorHandler,
  notFoundHandler
};