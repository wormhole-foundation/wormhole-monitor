import { hexToUint8Array, getIsTransferCompletedAptos } from '@certusone/wormhole-sdk';
import { getTxns, CHAIN_INFO_MAP, PATH } from './transaction_helper';
import { getQuery, write_to_table } from './mysqldb';
import connection from './mysqldb2';
import { AptosClient } from 'aptos';
import { runAptos } from './index';

require('dotenv').config();

// async function getPrevHashes(chain) {
//   var sql_query = `SELECT distinct source_hash as hash FROM ${tableName} WHERE chain_id=${chain} and source_time is not null
//                         UNION
//                         SELECT distinct redeem_hash as hash FROM ${tableName} WHERE target_chain_id=${chain} and redeem_time is not null`;
//   var prevTxns;
//   await getQuery(connection, sql_query)
//     .then(function (results) {
//       prevTxns = results;
//     })
//     .catch(function (err) {
//       console.log("Promise rejection error: " + err);
//     });
//   return prevTxns;
// }

// async function getLatestBlocks(chain) {
//   var sql_query = `SELECT MAX(source_block) as block FROM ${tableName}
//                         WHERE chain_id=${chain} and source_time is not null
//                         UNION
//                         SELECT MAX(redeem_block) as block FROM ${tableName}
//                         WHERE target_chain_id=${chain} and redeem_time is not null`;
//   var blocks;
//   // console.log(sql_query)
//   await getQuery(connection, sql_query)
//     .then(function (results) {
//       blocks = results;
//     })
//     .catch(function (err) {
//       console.log("Promise rejection error: " + err);
//     });
//   return blocks;
// }

async function getMissingRedeems(chain) {
  var sql_query = `SELECT * FROM ${tableName} WHERE target_chain_id=${chain} 
                        and target_address is not null
                        and (redeem_hash is null or redeem_time is null or redeem_time='1970-01-01') order by source_time DESC`;
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
async function getBookEndBlocks(chain, t0) {
  var sql_query0 = `SELECT * FROM ${tableName} WHERE chain_id=${chain} and source_time < '${t0}'
                        and source_time is not null and source_block is not null order by source_block DESC limit 1`;
  var startBlocks;
  await getQuery(connection, sql_query0)
    .then(function (results) {
      startBlocks = results;
    })
    .catch(function (err) {
      console.log('Promise rejection error: ' + err);
    });

  var sql_query1 = `SELECT * FROM ${tableName} WHERE chain_id=${chain} and source_time > '${t0}'
                        and source_time is not null and source_block is not null order by source_block limit 1`;
  var endBlocks;
  await getQuery(connection, sql_query1)
    .then(function (results) {
      endBlocks = results;
    })
    .catch(function (err) {
      console.log('Promise rejection error: ' + err);
    });
  return [startBlocks, endBlocks];
}

// which sweet chain bro?
const chain = process.env.chain;
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
const endBlock = process.env.endBlock || undefined;
const tableName = process.env.onchainTable;

async function grab(chainInfo, hashes) {
  const client = await new AptosClient('https://mainnet.aptoslabs.com');
  // const interval = 500;

  for (let i = 0; i < hashes.length; i++) {
    console.log(`${i} out of ${hashes.length}`);
    const missingRedeem = hashes[i];
    if (
      missingRedeem.source_hash ==
      '0x8586402c86301cd9068c7b69dd7c5c4ea2b3efc3edc48170fa9414840d56aea8'
    ) {
      console.log('skipping problematic hash');
      continue;
    }

    var redeemed = missingRedeem.is_redeemed;
    if (redeemed == 0 || redeemed == null || redeemed == undefined) {
      //recheck bridge contract to see if redeemed yet
      const hexVaa = missingRedeem.signed_vaa;
      const bytesVaa = hexToUint8Array(hexVaa);

      const is_redeemed = await getIsTransferCompletedAptos(
        client,
        chainInfo.token_bridge_address,
        bytesVaa
      );
      // console.log('is_redeemed?', is_redeemed)
      if (is_redeemed == false) {
        console.log(
          `chain=${missingRedeem.chain_id}, seq=${missingRedeem.seq} is not yet redeemed`
        );
        continue;
      }
    }

    const targetAddress = missingRedeem.target_address;
    const transferTime = missingRedeem.source_time;
    console.log(transferTime.toISOString());
    var blocks = await getBookEndBlocks(chain, transferTime.toISOString());
    console.log(blocks);
    const startBlock = blocks[0][0]?.source_block;
    // const endBlock = blocks[1][0]?.source_block
    console.log(missingRedeem);
    console.log(startBlock, endBlock);
    const chainInfoByWallet = { ...chainInfo };
    chainInfoByWallet.token_bridge_address = `0x${targetAddress}`;
    let transactionArray = await getTxns(chainInfoByWallet, startBlock - 1000, endBlock);
    console.log(transactionArray.length);
    var transfers = [];
    var completes = [];

    [transfers, completes] = await runAptos(transactionArray, chainInfo);

    // if (transfers.length > 0){
    //     write_to_table(connection, 'INSERT_MANY', tableName, transfers)
    // }

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
