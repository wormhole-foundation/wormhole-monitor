// Load module
var mysql = require('mysql');
// Initialize pool
var pool = mysql.createPool({
  connectionLimit: 10,
  host: 'localhost',
  user: process.env.WORMHOLE_USER,
  password: process.env.WORMHOLE_PW,
  database: process.env.WORMHOLE_DB,
  debug: false,
});
module.exports = pool;
