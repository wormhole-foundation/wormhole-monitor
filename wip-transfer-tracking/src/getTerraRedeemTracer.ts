import { hexToUint8Array } from '@certusone/wormhole-sdk';
import { checkRedeemed, CHAIN_INFO_MAP, getTerraTxnsByTime } from './transaction_helper';
import { getQuery, write_to_table } from './mysqldb';
import { runTerra } from './index';

import connection from './mysqldb2';

require('dotenv').config();

async function getMissingRedeems(chain) {
  var sql_query = `SELECT * FROM ${tableName} WHERE target_chain_id=${chain} 
                        and target_address is not null 
                        and (redeem_hash is null or redeem_time is null or redeem_time='1970-01-01') 
                        order by source_time DESC`;
  var missingRedeems;
  await getQuery(connection, sql_query)
    .then(function (results) {
      missingRedeems = results;
    })
    .catch(function (err) {
      console.log('Promise rejection error: ' + err);
    });
  return missingRedeems;
}

async function getBookEndHashes(terraIndexer, t0) {
  var sql_query0 = `SELECT * FROM ${terraIndexer} WHERE time < '${t0}' order by time DESC limit 1`;
  var index0;
  await getQuery(connection, sql_query0)
    .then(function (results) {
      index0 = results;
    })
    .catch(function (err) {
      console.log('Promise rejection error: ' + err);
    });

  var sql_query1 = `SELECT * FROM ${terraIndexer} WHERE time > ('${t0}' + interval 1 day) order by time limit 1`;
  var index1;
  await getQuery(connection, sql_query1)
    .then(function (results) {
      index1 = results;
    })
    .catch(function (err) {
      console.log('Promise rejection error: ' + err);
    });
  return [index0, index1];
}

// which sweet chain bro?
const chain = process.env.chain || 3;
// filter transactions using txn hashes in table?
// const filter = process.env.filter || 0;
// save txns and parsed transfers/redeems
// const save = process.env.save || 0;
// load from saved txns file when pulling more txns.
// useful when the scan site errs out in the middle
// const preload = process.env.preload || "0";
// skip the getTxns call, read directly from saved txns file
// const loadFromFile = process.env.loadFromFile || "0";
// const contract = process.env.contract || "";
// manual start block for evm scan site to start from
// skips finding last saved block in table
// const startBlock = process.env.startBlock || undefined;
// const endBlock = process.env.endBlock || undefined;
const tableName = process.env.onchainTable;

const terraIndexer = `${process.env.WORMHOLE_DB}.terra_classic_indexer`;
async function grab(chainInfo, hashes) {
  for (let i = 0; i < hashes.length; i++) {
    const missingRedeem = hashes[i];
    var redeemed = missingRedeem.is_redeemed;
    // not reliable since older terra transactions return false negatives
    if (redeemed == 0 || redeemed == null || redeemed == undefined) {
      //recheck bridge contract to see if redeemed yet
      const hexVaa = missingRedeem.signed_vaa;
      const bytesVaa = hexToUint8Array(hexVaa);

      const is_redeemed = await checkRedeemed(chainInfo, bytesVaa);
      if (is_redeemed == false) {
        console.log(
          `chain=${missingRedeem.chain_id}, seq=${missingRedeem.seq} is not yet redeemed`
        );
        continue;
      }
    }

    const targetAddress = missingRedeem.target_address;
    const transferTime = missingRedeem.source_time;
    var hashInterval = await getBookEndHashes(terraIndexer, transferTime.toISOString());
    // const startHash = hashInterval[0][0]?.hash;
    // const endId = hashInterval[1][0]?.next_id;
    const chainInfoByWallet = { ...chainInfo };
    chainInfoByWallet.token_bridge_address = targetAddress;
    let transactionArray = await getTerraTxnsByTime(chainInfoByWallet, transferTime);

    var transfers = [];
    var completes = [];

    [transfers, completes] = await runTerra(transactionArray, chainInfo);

    if (transfers.length > 0) {
      write_to_table(connection, 'INSERT_MANY', tableName, transfers);
    }

    if (completes.length > 0) {
      write_to_table(connection, 'INSERT_MANY_REDEEMS', tableName, completes);
    }
  }
  return;
}

(async () => {
  // get last known block number from table
  var missingRedeems = await getMissingRedeems(chain);
  console.log(`found ${missingRedeems.length} missing hashes`);
  const chainInfo = CHAIN_INFO_MAP[chain];
  var res = await grab(chainInfo, missingRedeems);

  connection.end();
})();
