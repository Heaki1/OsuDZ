const rateLimit = require('express-rate-limit');

function createRateLimit(windowMs, max, message) {
  return rateLimit({
    windowMs,
    max,
    message: { success: false, error: message },
    standardHeaders: true,
    legacyHeaders: false
  });
}

const apiRateLimit = createRateLimit(15 * 60 * 1000, 100, 'Too many API requests');
const adminRateLimit = createRateLimit(15 * 60 * 1000, 20, 'Too many admin requests');
const authRateLimit = createRateLimit(15 * 60 * 1000, 5, 'Too many authentication attempts');
const searchRateLimit = createRateLimit(1 * 60 * 1000, 30, 'Too many search requests');
const heavyRateLimit = createRateLimit(5 * 60 * 1000, 10, 'Too many resource-intensive requests');

module.exports = apiRateLimit; 
module.exports.apiRateLimit = apiRateLimit;
module.exports.adminRateLimit = adminRateLimit;
module.exports.authRateLimit = authRateLimit;
module.exports.searchRateLimit = searchRateLimit;
module.exports.heavyRateLimit = heavyRateLimit;
module.exports.createRateLimit = createRateLimit;

