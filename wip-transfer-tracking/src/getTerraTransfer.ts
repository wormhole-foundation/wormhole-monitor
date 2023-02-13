import { getTxns, CHAIN_INFO_MAP, PATH } from './transaction_helper';
import { getQuery, write_to_table } from './mysqldb';
import { runTerra } from './index';

import connection from './mysqldb2';
require('dotenv').config();

// let getLastEntry = function (chain_id) {
//   return new Promise(function (resolve, reject) {
//     let blockNumber = 0;
//     var sql = `SELECT * FROM ${tableName} where chain_id = ${chain_id} and source_time is not null ORDER BY source_time DESC limit 1`;
//     connection.query(sql, function (err, results) {
//       if (err) throw err;
//       if (results === undefined) {
//         reject(new Error("Error rows is undefined"));
//       } else {
//         console.log("inside", results[0]); // good
//         //blockNumber = results[0].source_block
//         resolve(results[0]);
//       }
//     });
//   });
// };

async function getPrevHashes(chain) {
  var sql_query = `SELECT distinct source_hash as hash FROM ${tableName} WHERE chain_id=${chain} and source_time is not null
                        UNION 
                        SELECT distinct redeem_hash as hash FROM ${tableName} WHERE target_chain_id=${chain} and redeem_time is not null`;
  var prevTxns;
  await getQuery(connection, sql_query)
    .then(function (results) {
      prevTxns = results;
    })
    .catch(function (err) {
      console.log('Promise rejection error: ' + err);
    });
  return prevTxns;
}

async function getLatestHashes(chain) {
  var sql_query = `SELECT source_block as block, source_time as time, source_hash as hash FROM ${tableName} 
                        WHERE chain_id=${chain} and source_time is not null and source_block=(SELECT MAX(source_block) FROM ${tableName} WHERE chain_id=${chain}) 
                        UNION 
                        SELECT redeem_block as block, redeem_time as time, redeem_hash as hash FROM ${tableName} 
                        WHERE target_chain_id=${chain} and redeem_time is not null and redeem_block=(SELECT MAX(redeem_block) FROM ${tableName} WHERE target_chain_id=${chain})`;
  var blocks;
  console.log(sql_query);
  await getQuery(connection, sql_query)
    .then(function (results) {
      blocks = results;
      console.log(results);
    })
    .catch(function (err) {
      console.log('Promise rejection error: ' + err);
    });
  return blocks;
}

const chain = process.env.chain || 3;
// filter transactions using txn hashes in table?
const filter = process.env.filter || 0;
// save txns and parsed transfers/redeems
const save = process.env.save || 0;
// load from saved txns file when pulling more txns.
// useful when the scan site errs out in the middle
const preload = process.env.preload || '0';
// skip the getTxns call, read directly from saved txns file
const loadFromFile = process.env.loadFromFile || '0';
// starting hash in lieu of grabbing last saved hash in table
const startHash = process.env.startHash || undefined;
const endId = process.env.endId || undefined;
const tableName = process.env.onchainTable;

(async () => {
  var transactionArray = [];
  // get last known block number from table
  let lastTxnHash = '';
  if (startHash != undefined) {
    lastTxnHash = startHash;
  } else {
    //let lastBlockNumber = await getLastBlockNumber(connection, chain)
    // const sql_query = `SELECT * FROM ${tableName}
    //     where chain_id = ${chain} and source_time is not null
    //     ORDER BY source_time DESC limit 1`
    // await getQuery(connection, sql_query)
    //     .then(function(results){
    //     lastTxnHash = results[0]['source_hash']
    // })
    //     .catch(function(err){
    //     console.log("Promise rejection error: " + err);
    // })
    var latestHashes = await getLatestHashes(chain);
    var lastTxnSorted = latestHashes.sort(function (a, b) {
      return a.block - b.block;
    });
    lastTxnHash = lastTxnSorted[0]?.hash;
    console.log(latestHashes);
    console.log(lastTxnHash);
  }

  console.log('found last txnhash=', lastTxnHash);

  //get previous hashes if looking for gaps
  if (loadFromFile != '0') {
    const fs = require('fs'); // Or `import fs from "fs";` with ESM
    if (fs.existsSync(loadFromFile)) {
      transactionArray = require(loadFromFile);
      console.log(`${loadFromFile} file exists`, transactionArray.length);
    } else {
      transactionArray = [];
      console.log(`${loadFromFile} file does not exist`);
    }
  } else {
    transactionArray = await getTxns(CHAIN_INFO_MAP[chain], lastTxnHash, endId, preload);
  }

  if (save) {
    require('fs').writeFile(
      `${PATH}/${CHAIN_INFO_MAP[chain].name}Txns.json`,

      JSON.stringify(transactionArray),

      function (err) {
        if (err) {
          console.error(`Crap happens`);
        }
      }
    );
  }

  console.log(`found ${transactionArray.length} transactions for chain_id=${chain}`);

  if (filter != 0) {
    const prevTxnRows = await getPrevHashes(chain);
    const prevTxnHashes = prevTxnRows.map((txn) => txn.hash);
    console.log(`filtering ${prevTxnHashes.length} prev hashes`);
    transactionArray = transactionArray.filter((txn) => !prevTxnHashes.includes(txn.txhash));
    console.log(`now there are ${transactionArray.length} transactions for chain_id=${chain}`);
  }
  var transfers = [];
  var completes = [];
  [transfers, completes] = await runTerra(transactionArray, CHAIN_INFO_MAP[chain]);
  if (save) {
    require('fs').writeFile(
      `${PATH}/${CHAIN_INFO_MAP[chain].name}.transfers.json`,

      JSON.stringify(transfers),

      function (err) {
        if (err) {
          console.error(`Crap happens`);
        }
      }
    );
    require('fs').writeFile(
      `${PATH}/${CHAIN_INFO_MAP[chain].name}.redeems.json`,

      JSON.stringify(completes),

      function (err) {
        if (err) {
          console.error(`Crap happens`);
        }
      }
    );
  }

  if (transfers.length > 0) {
    // upload to mysqldb
    //let table_name = 'onchain_test'
    write_to_table(connection, 'INSERT_MANY', tableName, transfers);
  }

  if (completes.length > 0) {
    // find transfer in mysqldb & update with redeem details
    //let table_name = 'onchain_test'
    write_to_table(connection, 'INSERT_MANY_REDEEMS', tableName, completes);
  }

  console.log(transfers.length);
  console.log(completes.length);

  connection.end();
})();
