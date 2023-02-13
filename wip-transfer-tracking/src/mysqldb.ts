// var mysql = require("mysql");
require('dotenv').config();

function loadSavedData(prevDataFile) {
  var prevData;
  const fs = require('fs'); // Or `import fs from "fs";` with ESM
  if (fs.existsSync(prevDataFile)) {
    prevData = require(prevDataFile);
    console.log('I got your file');
  } else {
    prevData = [];
    console.log("the file doesn't exists");
  }
  return prevData;
}

function build_col_str(columns) {
  let columns_str = '';
  columns.forEach((col) => (columns_str += `${col},`));

  return columns_str.slice(0, -1);
}

function build_values_str(values) {
  let values_str = '';
  values.forEach(function (val) {
    let val_str;
    if (typeof val == 'string') {
      val_str = `\"${val}\"`;
    } else {
      val_str = val;
    }
    values_str += `${val_str},`;
  });

  return values_str.slice(0, -1);
}

function build_on_duplicate_key_str(entries) {
  let keypair_str = '';
  entries.forEach(function ([col, val]) {
    let val_str;
    if (typeof val == 'string') {
      val_str = `\"${val}\"`;
    } else {
      val_str = val;
    }
    keypair_str += `${col} = ${val_str},`;
  });
  return keypair_str.slice(0, -1);
}
function build_multiple_on_duplicate_key_str_no_redeem(data) {
  let dupe_key_str = '';
  let columns = Object.keys(data[0]);
  for (let i = 0; i < columns.length; i++) {
    let col = columns[i];
    if (col == 'is_redeemed') {
      continue;
    } else {
      dupe_key_str += `${col} = VALUES(${col}),\n`;
    }
  }
  // console.log(dupe_key_str.slice(0, -2))
  return dupe_key_str.slice(0, -2);
}

function build_multiple_on_duplicate_key_str(data) {
  let dupe_key_str = '';
  let columns = Object.keys(data[0]);
  for (let i = 0; i < columns.length; i++) {
    let col = columns[i];
    dupe_key_str += `${col} = VALUES(${col}),\n`;
  }
  // console.log(dupe_key_str.slice(0, -2))
  return dupe_key_str.slice(0, -2);
}

function build_redeem_entries_str(entries) {
  let keypair_str = '';
  let redeem_cols = [
    'is_redeemed',
    'redeem_hash',
    'redeem_timestamp',
    'redeem_time',
    'redeem_block',
    'redeem_wallet',
  ];
  entries.forEach(function ([col, val]) {
    let val_str;
    if (typeof val == 'string') {
      // console.log(col, val)
      val_str = `\"${val}\"`;
    } else {
      val_str = val;
    }
    if (redeem_cols.includes(col)) {
      keypair_str += `${col} = ${val_str},`;
    }
  });
  return keypair_str.slice(0, -1);
}

function build_redeem_key_str(entries) {
  let keypair_str = '';
  let redeem_cols = ['emitter_address', 'emitter_chain', 'seqnum'];
  entries.forEach(function ([col, val]) {
    let val_str;
    if (typeof val == 'string') {
      val_str = `\"${val}\"`;
    } else {
      val_str = val;
    }
    if (redeem_cols.includes(col)) {
      keypair_str += `${col} = ${val_str},`;
    }
  });
  return keypair_str.slice(0, -1);
}

function format_sequences(data) {
  var seqs = [];
  let seq_str = '';
  data.forEach((tx) => seqs.push(tx.seq));
  for (let i = 0; i < seqs.length; i++) {
    if (i == 0) {
      seq_str += `${seqs[i]}`;
    } else {
      seq_str += `, ${seqs[i]}`;
    }
  }

  return seq_str;
}

export async function getQuery(con, sql) {
  return new Promise(function (resolve, reject) {
    con.query(sql, function (err, results) {
      if (err) reject(err);
      if (results === undefined) {
        reject(new Error('Error rows is undefined'));
      } else {
        resolve(results);
      }
    });
  });
}

export async function write_to_table(connection, action, table, data) {
  if (action == 'INSERT') {
    //console.log("here", data)
    // one entry

    // TODO:  Fix the below line
    if (data == undefined || data == {}) {
      console.log('empty set');
      return;
    }
    let columns_str = build_col_str(Object.keys(data));
    let values_str = build_values_str(Object.values(data));
    let entries_str = build_on_duplicate_key_str(Object.entries(data));

    var sql = `INSERT INTO ${table} (${columns_str}) 
                  VALUES (${values_str}) 
                  ON DUPLICATE KEY UPDATE ${entries_str};`;
    //

    await connection.query(sql, function (err, result) {
      if (err) {
        console.log(err); // throw err;
      }
      console.log('inserted');
    });
  } else if (action == 'INSERT_MANY') {
    // multiple entries
    let entry = data[0];
    let columns_str = build_col_str(Object.keys(entry));
    let values = [];
    data.forEach((x) => values.push(Object.values(x)));
    let multiple_row_dupe_str = build_multiple_on_duplicate_key_str_no_redeem(data);
    var sql = `INSERT INTO ${table} (${columns_str}) 
                  VALUES ? 
                  ON DUPLICATE KEY UPDATE
                  ${multiple_row_dupe_str};`;

    connection.query(sql, [values], function (err, result) {
      if (err) throw err;
      console.log('inserted many');
    });
  } else if (action == 'INSERT_MANY_REDEEMS') {
    // multiple entries
    let entry = data[0];
    let columns_str = build_col_str(Object.keys(entry));
    let values = [];
    data.forEach((x) => values.push(Object.values(x)));
    let multiple_row_dupe_str = build_multiple_on_duplicate_key_str(data);
    var sql = `INSERT INTO ${table} (${columns_str}) 
                  VALUES ? 
                  ON DUPLICATE KEY UPDATE
                  ${multiple_row_dupe_str};`;

    // console.log(sql)
    connection.query(sql, [values], function (err, result) {
      if (err) throw err;
      console.log('inserted many');
    });
  } else if (action == 'UPDATE') {
    // update existing entry.. like a redeem
    var entry;
    if (data != undefined && data.length > 0) {
      entry = data[0];
    }
    let columns_str = build_col_str(Object.keys(entry));
    let values_str = build_values_str(Object.values(entry));
    let redeem_entries_str = build_redeem_entries_str(Object.entries(entry));
    let redeem_keypairs_str = build_redeem_key_str(Object.entries(entry));
    // console.log(data)
    // console.log(values_str)
    var sql = `UPDATE ${table} SET ${redeem_entries_str} 
                    WHERE ${redeem_keypairs_str};`;
    // console.log(sql)

    connection.query(sql, function (err, result) {
      if (err) throw err;
      console.log('updated');
    });
  } else if (action == 'DELETE_ROWS') {
    var entry;
    var chain;
    var emitter_address;
    var sequence_str = '';
    if (data != undefined && data.length > 0) {
      entry = data[0];
    }
    chain = entry.chain_id;
    emitter_address = entry.emitter_address;
    sequence_str = format_sequences(data);

    console.log('deleting rows');
    var sql = `DELETE FROM ${table} WHERE chain_id=${chain} and emitter_address='${emitter_address}' 
                    and seq in (${sequence_str});`;
    console.log(sql);
    connection.query(sql, function (err, result) {
      if (err) throw err;
      console.log('deleted rows');
    });
  }
  return;
}
