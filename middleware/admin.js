// middleware/admin.js
const { authenticateToken } = require('./validation');

// Admin access middleware
const requireAdmin = (req, res, next) => {
  if (!req.user) {
    return res.status(401).json({ 
      success: false, 
      error: 'Authentication required' 
    });
  }
  
  if (req.user.role !== 'admin') {
    return res.status(403).json({ 
      success: false, 
      error: 'Admin access required' 
    });
  }
  
  next();
};

// Combined auth + admin middleware
const adminAuth = [authenticateToken, requireAdmin];

// Super admin middleware (for destructive operations)
const requireSuperAdmin = (req, res, next) => {
  if (!req.user) {
    return res.status(401).json({ 
      success: false, 
      error: 'Authentication required' 
    });
  }
  
  if (req.user.role !== 'super_admin') {
    return res.status(403).json({ 
      success: false, 
      error: 'Super admin access required' 
    });
  }
  
  next();
};

// Log admin actions
const logAdminAction = (req, res, next) => {
  const originalSend = res.send;
  
  res.send = function(data) {
    // Log successful admin actions
    if (res.statusCode < 400) {
      console.log(`🔧 Admin Action: ${req.user?.username || req.user?.id} - ${req.method} ${req.originalUrl}`);
      if (req.body && Object.keys(req.body).length > 0) {
        console.log(`   Data:`, JSON.stringify(req.body, null, 2));
      }
    }
    
    originalSend.call(this, data);
  };
  
  next();
};

module.exports = {
  requireAdmin,
  adminAuth,
  requireSuperAdmin,
  logAdminAction
};