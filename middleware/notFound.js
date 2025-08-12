// middleware/notFound.js
module.exports = function notFound(req, res, next) {
  res.status(404).json({ success: false, error: 'Endpoint not found' });
};
