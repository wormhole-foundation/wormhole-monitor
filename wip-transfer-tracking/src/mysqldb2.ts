// import { sleep } from "../transaction_helper";

require('dotenv').config();

const mysql = require('mysql');
const util = require('util');
var connection;

connection = mysql.createConnection({
  host: 'localhost',
  user: process.env.WORMHOLE_USER,
  password: process.env.WORMHOLE_PW,
  database: process.env.WORMHOLE_DB,
});
// promise wrapper to enable async await with MYSQL
connection.query = util.promisify(connection.query).bind(connection);

// connect to the database
connection.connect(function (err) {
  if (err) {
    console.log('error connecting: ' + err.stack);
    return;
  }
  console.log('connected as... ' + connection.threadId);
});

export default connection;
