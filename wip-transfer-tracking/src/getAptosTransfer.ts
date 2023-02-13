import { getTxns, CHAIN_INFO_MAP, PATH } from './transaction_helper';
import { getQuery, write_to_table } from './mysqldb';
import connection from './mysqldb2';
import { runAptos } from './index';

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

async function getLatestBlocks(chain) {
  var sql_query = `SELECT MAX(source_block) as block FROM ${tableName} 
                        WHERE chain_id=${chain} and source_time is not null
                        UNION 
                        SELECT MAX(redeem_block) as block FROM ${tableName} 
                        WHERE target_chain_id=${chain} and redeem_time is not null`;
  var blocks;
  console.log(sql_query);
  await getQuery(connection, sql_query)
    .then(function (results) {
      blocks = results;
    })
    .catch(function (err) {
      console.log('Promise rejection error: ' + err);
    });
  return blocks;
}

// which sweet chain bro?
const chain = process.env.chain;
// filter transactions using txn hashes in table?
const filter = process.env.filter || 0;
// save txns and parsed transfers/redeems
const save = process.env.save || 0;
// load from saved txns file when pulling more txns.
// useful when the scan site errs out in the middle
// const preload = process.env.preload || "0";
// skip the getTxns call, read directly from saved txns file
const loadFromFile = process.env.loadFromFile || '0';
// manual start block for evm scan site to start from
// skips finding last saved block in table
const startBlock = process.env.startBlock || undefined;
const endBlock = process.env.endBlock || undefined;
const tableName = process.env.onchainTable;

(async () => {
  var transactionArray = [];
  // get last known block number from table
  let lastBlockNumber;
  if (startBlock != undefined) {
    lastBlockNumber = startBlock;
  } else {
    var latestBlocks = await getLatestBlocks(chain);
    var blockArray = latestBlocks.map((x) => x.block);
    lastBlockNumber = Math.min(...blockArray);
  }

  console.log('last known blocknumber=', lastBlockNumber);

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
    transactionArray = await getTxns(CHAIN_INFO_MAP[chain], lastBlockNumber, endBlock);
  }

  console.log(`found ${transactionArray.length} transactions for chain_id=${chain}`);

  if (filter != 0) {
    const prevTxnRows = await getPrevHashes(chain);
    const prevTxnHashes = prevTxnRows.map((txn) => txn.hash);
    console.log(`filtering ${prevTxnHashes.length} prev hashes`);
    transactionArray = transactionArray.filter((txn) => !prevTxnHashes.includes(txn.hash));
    console.log(`now there are ${transactionArray.length} transactions for chain_id=${chain}`);
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
  var transfers = [];
  var completes = [];
  [transfers, completes] = await runAptos(transactionArray, CHAIN_INFO_MAP[chain]);

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
    write_to_table(connection, 'INSERT_MANY', tableName, transfers);
  }

  if (completes.length > 0) {
    // find transfer in mysqldb & update with redeem details
    write_to_table(connection, 'INSERT_MANY_REDEEMS', tableName, completes);
  }
  console.log(transfers.length);
  console.log(completes.length);

  connection.end();
})();
