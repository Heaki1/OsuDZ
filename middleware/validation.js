// middleware/validation.js

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
      
      if (value && rule.type === 'email' && !isValidEmail(value)) {
        errors.push(`${field} must be a valid email`);
      }
      
      if (value && rule.minLength && value.toString().length < rule.minLength) {
        errors.push(`${field} must be at least ${rule.minLength} characters`);
      }
      
      if (value && rule.maxLength && value.toString().length > rule.maxLength) {
        errors.push(`${field} must be at most ${rule.maxLength} characters`);
      }
      
      if (value && rule.min && Number(value) < rule.min) {
        errors.push(`${field} must be at least ${rule.min}`);
      }
      
      if (value && rule.max && Number(value) > rule.max) {
        errors.push(`${field} must be at most ${rule.max}`);
      }
      
      if (value && rule.pattern && !rule.pattern.test(value)) {
        errors.push(`${field} format is invalid`);
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

// Helper functions
function isValidEmail(email) {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return emailRegex.test(email);
}

// Sanitize input to prevent XSS
const sanitizeInput = (req, res, next) => {
  const sanitize = (obj) => {
    if (typeof obj === 'string') {
      return obj.replace(/<script[^>]*>.*?<\/script>/gi, '')
                .replace(/<[^>]*>/g, '')
                .trim();
    }
    if (typeof obj === 'object' && obj !== null) {
      Object.keys(obj).forEach(key => {
        obj[key] = sanitize(obj[key]);
      });
    }
    return obj;
  };
  
  req.body = sanitize(req.body);
  req.query = sanitize(req.query);
  req.params = sanitize(req.params);
  
  next();
};

// Rate limiting validation
const validateRateLimit = (windowMs, maxRequests, keyGenerator = null) => {
  const requests = new Map();
  
  return (req, res, next) => {
    const key = keyGenerator ? keyGenerator(req) : req.ip;
    const now = Date.now();
    const windowStart = now - windowMs;
    
    if (!requests.has(key)) {
      requests.set(key, []);
    }
    
    const userRequests = requests.get(key);
    const validRequests = userRequests.filter(timestamp => timestamp > windowStart);
    
    if (validRequests.length >= maxRequests) {
      return res.status(429).json({
        success: false,
        error: 'Too many requests',
        retryAfter: Math.ceil(windowMs / 1000)
      });
    }
    
    validRequests.push(now);
    requests.set(key, validRequests);
    
    // Cleanup old entries periodically
    if (Math.random() < 0.1) {
      for (const [k, timestamps] of requests.entries()) {
        const valid = timestamps.filter(t => t > windowStart);
        if (valid.length === 0) {
          requests.delete(k);
        } else {
          requests.set(k, valid);
        }
      }
    }
    
    next();
  };
};

// Validate pagination parameters
const validatePagination = (req, res, next) => {
  const { limit, offset } = req.query;
  
  if (limit !== undefined) {
    const limitNum = parseInt(limit);
    if (isNaN(limitNum) || limitNum < 1 || limitNum > 1000) {
      return res.status(400).json({
        success: false,
        error: 'Limit must be between 1 and 1000'
      });
    }
    req.query.limit = limitNum;
  }
  
  if (offset !== undefined) {
    const offsetNum = parseInt(offset);
    if (isNaN(offsetNum) || offsetNum < 0) {
      return res.status(400).json({
        success: false,
        error: 'Offset must be 0 or greater'
      });
    }
    req.query.offset = offsetNum;
  }
  
  next();
};

// Validate sort parameters
const validateSort = (allowedFields, defaultField = null, defaultOrder = 'DESC') => {
  return (req, res, next) => {
    const { sort, order } = req.query;
    
    if (sort && !allowedFields.includes(sort)) {
      return res.status(400).json({
        success: false,
        error: `Sort field must be one of: ${allowedFields.join(', ')}`
      });
    }
    
    if (order && !['ASC', 'DESC', 'asc', 'desc'].includes(order)) {
      return res.status(400).json({
        success: false,
        error: 'Order must be ASC or DESC'
      });
    }
    
    req.query.sort = sort || defaultField;
    req.query.order = order ? order.toUpperCase() : defaultOrder;
    
    next();
  };
};

// Validate username format
const validateUsername = (field = 'username') => {
  return validateInput({
    [field]: {
      required: true,
      minLength: 2,
      maxLength: 15,
      pattern: /^[a-zA-Z0-9_\-\[\]]+$/
    }
  });
};

// Validate numeric range
const validateRange = (field, min = null, max = null) => {
  const rule = { type: 'number' };
  if (min !== null) rule.min = min;
  if (max !== null) rule.max = max;
  
  return validateInput({ [field]: rule });
};

// Validate difficulty range for beatmaps
const validateDifficultyRange = (req, res, next) => {
  const { minDifficulty, maxDifficulty } = req.query;
  
  if (minDifficulty !== undefined) {
    const min = parseFloat(minDifficulty);
    if (isNaN(min) || min < 0 || min > 12) {
      return res.status(400).json({
        success: false,
        error: 'Minimum difficulty must be between 0 and 12'
      });
    }
  }
  
  if (maxDifficulty !== undefined) {
    const max = parseFloat(maxDifficulty);
    if (isNaN(max) || max < 0 || max > 12) {
      return res.status(400).json({
        success: false,
        error: 'Maximum difficulty must be between 0 and 12'
      });
    }
  }
  
  if (minDifficulty !== undefined && maxDifficulty !== undefined) {
    const min = parseFloat(minDifficulty);
    const max = parseFloat(maxDifficulty);
    if (min > max) {
      return res.status(400).json({
        success: false,
        error: 'Minimum difficulty cannot be greater than maximum difficulty'
      });
    }
  }
  
  next();
};

// Validate mods format
const validateMods = (req, res, next) => {
  const { mods } = req.query;
  
  if (mods) {
    const validMods = ['HD', 'HR', 'DT', 'NC', 'FL', 'EZ', 'NF', 'HT', 'SO', 'SD', 'PF', 'RX', 'AP', 'None'];
    const modList = mods.split(',').map(mod => mod.trim());
    
    const invalidMods = modList.filter(mod => !validMods.includes(mod));
    if (invalidMods.length > 0) {
      return res.status(400).json({
        success: false,
        error: `Invalid mods: ${invalidMods.join(', ')}. Valid mods: ${validMods.join(', ')}`
      });
    }
  }
  
  next();
};

module.exports = {
  validateInput,
  sanitizeInput,
  validateRateLimit,
  validatePagination,
  validateSort,
  validateUsername,
  validateRange,
  validateDifficultyRange,
  validateMods
};