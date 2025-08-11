const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL, // Make sure DATABASE_URL is set in your environment
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

async function query(text, params) {
  const start = Date.now();
  const res = await pool.query(text, params);
  const duration = Date.now() - start;
  console.log('Executed query', { text, duration, rows: res.rowCount });
  return res;
}

async function getRow(text, params) {
  const res = await query(text, params);
  return res.rows[0] || null;
}

async function getRows(text, params) {
  const res = await query(text, params);
  return res.rows;
}

module.exports = {
  pool,
  query,
  getRow,
  getRows,
};
