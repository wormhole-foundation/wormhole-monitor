import { getEmitterAddressSolana } from '@certusone/wormhole-sdk';
import getSignedVAAWithRetry from '@certusone/wormhole-sdk/lib/cjs/rpc/getSignedVAAWithRetry';
import { NodeHttpTransport } from '@improbable-eng/grpc-web-node-http-transport';
import { getTxns, sleep, CHAIN_INFO_MAP, WORMHOLE_RPC_HOSTS, PATH } from './transaction_helper';
import { getQuery, write_to_table } from './mysqldb';
import connection from './mysqldb2';

import { runSolana, checkPayloadType } from './index';

require('dotenv').config();

async function getMissingVaas(chain) {
  var sql_query = `SELECT * FROM ${tableName} WHERE chain_id=${chain} and signed_vaa is null`;

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
                        WHERE chain_id=${chain} and source_time is not null and source_block < (SELECT MIN(source_block) FROM ${tableName} WHERE chain_id=${chain} and signed_vaa is null) 
                        order by source_block DESC limit 1`;
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
// const filter = 1;
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

  const prevTxnRows = await getMissingVaas(chain);
  const prevTxnHashes = prevTxnRows.map((txn) => txn.source_hash);
  console.log(`filtering ${prevTxnHashes.length} for only missing vaa hashes`);
  transactionArray = transactionArray.filter((txn) => prevTxnHashes.includes(txn.txHash));
  console.log(`now there are ${transactionArray.length} transactions for chain_id=${chain}`);

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

  // if the transfer hash doesn't come back, check if there's a vaa, parse it, check if it's a token transfer
  const found_transactions = transfers.map((tx) => tx.source_hash);
  const still_missing_vaas = prevTxnRows.filter(
    (tx) => !found_transactions.includes(tx.source_hash)
  );
  const rows_to_remove = [];
  // console.log('missing vaas:', still_missing_vaas)
  const token_bridge_address = CHAIN_INFO_MAP[chain].token_bridge_address;
  const emitter_address = await getEmitterAddressSolana(token_bridge_address);
  for (let i = 0; i < still_missing_vaas.length; i++) {
    const tx = still_missing_vaas[i];
    console.log('seq=', tx.seq);
    var vaa = undefined;
    try {
      vaa = await getSignedVAAWithRetry(
        WORMHOLE_RPC_HOSTS,
        tx.chain_id,
        emitter_address,
        tx.seq,
        { transport: NodeHttpTransport() },
        1000,
        4
      );
    } catch (e) {
      console.log('cannot find signed VAA');
    }
    // console.log(vaa)
    if (vaa != undefined) {
      const payloadType = await checkPayloadType(vaa);
      console.log(payloadType);
      if (payloadType != 1) {
        // need to remove from table
        rows_to_remove.push(tx);
      }
    }
  }

  console.log('removing=', rows_to_remove);
  if (rows_to_remove.length > 0) {
    write_to_table(connection, 'DELETE_ROWS', tableName, rows_to_remove);
  }

  connection.end();
})();
