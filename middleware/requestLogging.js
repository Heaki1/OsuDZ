// Advanced request logging middleware

const colors = {
  reset: '\x1b[0m', 
  bright: '\x1b[1m', 
  red: '\x1b[31m', 
  green: '\x1b[32m',
  yellow: '\x1b[33m', 
  blue: '\x1b[34m', 
  magenta: '\x1b[35m', 
  cyan: '\x1b[36m'
};

function getMethodColor(method) {
  switch (method) {
    case 'GET': return colors.green;
    case 'POST': return colors.yellow;
    case 'PUT': return colors.blue;
    case 'DELETE': return colors.red;
    case 'PATCH': return colors.magenta;
    default: return colors.cyan;
  }
}

function getStatusColor(status) {
  if (status >= 200 && status < 300) return colors.green;
  if (status >= 300 && status < 400) return colors.cyan;
  if (status >= 400 && status < 500) return colors.yellow;
  if (status >= 500) return colors.red;
  return colors.reset;
}

const requestLogger = (req, res, next) => {
  const startTime = Date.now();
  const timestamp = new Date().toISOString().replace('T', ' ').split('.')[0];
  
  // Store original end function
  const originalEnd = res.end;
  
  // Override end function to log response
  res.end = function(chunk, encoding) {
    // Call original end function
    originalEnd.call(this, chunk, encoding);
    
    const duration = Date.now() - startTime;
    const methodColor = getMethodColor(req.method);
    const statusColor = getStatusColor(res.statusCode);
    
    // Build log message
    const logParts = [
      `${colors.bright}[${timestamp}]${colors.reset}`,
      `${methodColor}${req.method}${colors.reset}`,
      `${statusColor}${res.statusCode}${colors.reset}`,
      `${req.originalUrl}`,
      `${colors.bright}${duration}ms${colors.reset}`
    ];
    
    // Add user info if authenticated
    if (req.user) {
      logParts.push(`${colors.blue}user:${req.user.username || req.user.id}${colors.reset}`);
    }
    
    // Add IP address for important endpoints
    if (req.originalUrl.includes('/admin/') || res.statusCode >= 400) {
      logParts.push(`${colors.cyan}${req.ip}${colors.reset}`);
    }
    
    // Add response size if significant
    const contentLength = res.get('Content-Length');
    if (contentLength && parseInt(contentLength) > 1024) {
      logParts.push(`${colors.magenta}${(parseInt(contentLength) / 1024).toFixed(1)}KB${colors.reset}`);
    }
    
    // Add warning for slow requests
    if (duration > 5000) {
      logParts.push(`${colors.red}SLOW${colors.reset}`);
    }
    
    console.log(logParts.join(' '));
    
    // Log errors with more detail
    if (res.statusCode >= 400) {
      const userAgent = req.get('User-Agent') || 'Unknown';
      const referer = req.get('Referer') || 'Direct';
      console.log(`  ${colors.yellow}→ UA: ${userAgent}${colors.reset}`);
      console.log(`  ${colors.yellow}→ Referer: ${referer}${colors.reset}`);
      
      if (req.body && Object.keys(req.body).length > 0) {
        console.log(`  ${colors.yellow}→ Body: ${JSON.stringify(req.body, null, 2)}${colors.reset}`);
      }
    }
  };
  
  next();
};

// Simple logger for basic use cases
const simpleLogger = (req, res, next) => {
  const timestamp = new Date().toISOString().replace('T', ' ').split('.')[0];
  console.log(`[${timestamp}] ${req.method} ${req.originalUrl}`);
  next();
};

// Performance logger for API endpoints
const performanceLogger = (req, res, next) => {
  const startTime = process.hrtime.bigint();
  
  res.on('finish', () => {
    const endTime = process.hrtime.bigint();
    const duration = Number(endTime - startTime) / 1000000; // Convert to milliseconds
    
    // Log slow API requests
    if (duration > 1000 && req.originalUrl.startsWith('/api/')) {
      console.warn(`🐌 Slow API request: ${req.method} ${req.originalUrl} took ${duration.toFixed(2)}ms`);
    }
    
    // Log performance metrics for monitoring
    if (process.env.NODE_ENV === 'production' && duration > 500) {
      console.log(`📊 Performance: ${req.method} ${req.originalUrl} - ${duration.toFixed(2)}ms`);
    }
  });
  
  next();
};

module.exports = requestLogger;
module.exports.requestLogger = requestLogger;
module.exports.simpleLogger = simpleLogger;
module.exports.performanceLogger = performanceLogger;
