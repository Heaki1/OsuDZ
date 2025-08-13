const helmet = require('helmet');

// Enhanced security middleware
const securityMiddleware = helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: [
        "'self'", 
        "'unsafe-inline'",
        "https://cdnjs.cloudflare.com",
        "https://cdn.jsdelivr.net"
      ],
      styleSrc: [
        "'self'", 
        "'unsafe-inline'", 
        "https://cdnjs.cloudflare.com",
        "https://fonts.googleapis.com"
      ],
      fontSrc: [
        "'self'", 
        "https://cdnjs.cloudflare.com",
        "https://fonts.gstatic.com"
      ],
      imgSrc: [
        "'self'", 
        "data:", 
        "https:",
        "https://a.ppy.sh", // osu! avatars
        "https://assets.ppy.sh" // osu! assets
      ],
      connectSrc: [
        "'self'",
        "https://osu.ppy.sh", // osu! API
        "wss:", "ws:" // WebSocket connections
      ],
      objectSrc: ["'none'"],
      mediaSrc: ["'self'"],
      frameSrc: ["'none'"]
    }
  },
  crossOriginEmbedderPolicy: false, // Disable for osu! API compatibility
  hsts: {
    maxAge: 31536000, // 1 year
    includeSubDomains: true,
    preload: true
  },
  noSniff: true,
  frameguard: { action: 'deny' },
  xssFilter: true,
  referrerPolicy: { policy: 'strict-origin-when-cross-origin' }
});

// Trust proxy setting for accurate IP addresses
const trustProxy = (app) => {
  app.set('trust proxy', 1);
};

module.exports = securityMiddleware;