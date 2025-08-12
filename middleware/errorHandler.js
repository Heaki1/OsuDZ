// middleware/errorHandler.js
module.exports = function errorHandler(err, req, res, next) {
  console.error('❌ Server Error:', err.stack || err.message);
  res.status(500).json({ success: false, error: 'Internal Server Error' });
};
