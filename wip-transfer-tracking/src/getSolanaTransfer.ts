import { getTxns, sleep, CHAIN_INFO_MAP, PATH } from './transaction_helper';
import { getQuery, write_to_table } from './mysqldb';
import { runSolana } from './index';
import connection from './mysqldb2';

require('dotenv').config();

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

const chain = 1;
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
const endHash = process.env.endHash || undefined;
const tableName = process.env.onchainTable;

(async () => {
  var transactionArray = [];
  // get last known block number from table
  let lastTxnHash = '';
  if (startHash != undefined) {
    lastTxnHash = startHash;
  } else {
    var latestHashes = await getLatestHashes(chain);
    var lastTxnSorted = latestHashes.sort(function (a, b) {
      return a.block - b.block;
    });
    lastTxnHash = lastTxnSorted[0]?.hash;
    console.log(latestHashes);
    console.log(lastTxnHash);
    //lastBlockNumber = Math.min(...blockArray)
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
    transactionArray = await getTxns(CHAIN_INFO_MAP['1'], lastTxnHash, endHash, preload);
  }

  if (save) {
    require('fs').writeFile(
      `${PATH}/${CHAIN_INFO_MAP['1'].name}Txns.json`,

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
    transactionArray = transactionArray.filter((txn) => !prevTxnHashes.includes(txn.txHash));
    console.log(`now there are ${transactionArray.length} transactions for chain_id=${chain}`);
  }

  var transfers = [];
  var completes = [];
  [transfers, completes] = await runSolana(transactionArray, CHAIN_INFO_MAP['1']);
  if (save) {
    require('fs').writeFile(
      `${PATH}/${CHAIN_INFO_MAP['1'].name}.transfers.json`,

      JSON.stringify(transfers),

      function (err) {
        if (err) {
          console.error(`Crap happens`);
        }
      }
    );
    require('fs').writeFile(
      `${PATH}/${CHAIN_INFO_MAP['1'].name}.redeems.json`,

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
    write_to_table(connection, 'INSERT_MANY', tableName, transfers);
  }

  await sleep(2000);
  if (completes.length > 0) {
    // find transfer in mysqldb & update with redeem details
    write_to_table(connection, 'INSERT_MANY_REDEEMS', tableName, completes);
  }

  console.log(transfers.length);
  console.log(completes.length);

  connection.end();
})();
