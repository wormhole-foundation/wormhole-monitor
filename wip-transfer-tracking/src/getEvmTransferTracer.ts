import { getTxns, sleep, CHAIN_INFO_MAP } from './transaction_helper';
import { getQuery, write_to_table } from './mysqldb';
import { runEvm } from './index';
import connection from './mysqldb2';

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
//   console.log(sql_query);
//   await getQuery(connection, sql_query)
//     .then(function (results) {
//       blocks = results;
//     })
//     .catch(function (err) {
//       console.log("Promise rejection error: " + err);
//     });
//   return blocks;
// }

async function getMissingTransfers(chain) {
  var sql_query = `SELECT * FROM ${tableName} WHERE chain_id=${chain} and source_time is null order by seq`;
  var seqs;
  await getQuery(connection, sql_query)
    .then(function (results) {
      seqs = results;
    })
    .catch(function (err) {
      console.log('Promise rejection error: ' + err);
    });
  return seqs;
}

// async function getMissingTransfersBySeqs(chain) {
//   var sql_query = `SELECT seq, source_block FROM ${tableName} WHERE chain_id=${chain} and seq is not null order by seq`;
//   var seqs;
//   await getQuery(connection, sql_query)
//     .then(function (results) {
//       seqs = results;
//     })
//     .catch(function (err) {
//       console.log("Promise rejection error: " + err);
//     });
//   return seqs;
// }

async function getBookEndBlocksBySeq(chain, seq) {
  var sql_query0 = `SELECT source_block FROM ${tableName} WHERE chain_id=${chain} and seq < ${seq} and seq is not null and source_block is not null order by source_block DESC limit 1`;
  var startBlocks;
  await getQuery(connection, sql_query0)
    .then(function (results) {
      startBlocks = results;
    })
    .catch(function (err) {
      console.log('Promise rejection error: ' + err);
    });

  var sql_query1 = `SELECT source_block FROM ${tableName} WHERE chain_id=${chain} and seq > ${seq} and seq is not null and source_block is not null order by source_block limit 1`;
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
// const endBlock = process.env.endBlock || undefined;
const tableName = process.env.onchainTable;

async function grab(chainInfo, redeems) {
  for (let i = 0; i < redeems.length; i++) {
    const missingTransfer = redeems[i];
    // const targetAddress = missingTransfer.target_address;
    const seq = missingTransfer.seq;
    console.log('missing seq=', seq);
    var blocks = await getBookEndBlocksBySeq(chain, seq);
    var startBlock = undefined;
    var endBlock = undefined;
    if (blocks.length > 1) {
      if (blocks[0].length > 0) {
        startBlock = blocks[0][0]?.source_block;
      }
      if (blocks[1].length > 0) {
        endBlock = blocks[1][0]?.source_block;
      }
    }
    const chainInfoByWallet = { ...chainInfo };
    let transactionArray = await getTxns(chainInfoByWallet, startBlock, endBlock);

    var transfers = [];
    var completes = [];

    [transfers, completes] = await runEvm(transactionArray, chainInfo);

    if (transfers.length > 0) {
      write_to_table(connection, 'INSERT_MANY', tableName, transfers);
    }

    if (completes.length > 0) {
      write_to_table(connection, 'INSERT_MANY_REDEEMS', tableName, completes);
    }
    await sleep(1000);
  }
  return;
}

// async function grabBySeqs(chainInfo, seqs) {
//   let i = 0;
//   let seq = 0;
//   if (seqs.length > 0) {
//     seq = seqs[0].seq; //starting seq
//   } else {
//     return;
//   }
//   const maxSeq = seqs[seqs.length - 1].seq;
//   while (seq < maxSeq) {
//     const transfer = seqs.filter((x) => x.seq == seq);

//     if (transfer.length == 0) {
//       console.log("missing seq=", seq);
//       var blocks = await getBookEndBlocksBySeq(chain, seq);

//       var startBlock = undefined;
//       var endBlock = undefined;
//       if (blocks.length > 1) {
//         if (blocks[0].length > 0) {
//           startBlock = blocks[0][0]?.source_block;
//         }
//         if (blocks[1].length > 0) {
//           endBlock = blocks[1][0]?.source_block;
//         }
//       }

//       let transactionArray = await getTxns(chainInfo, startBlock, endBlock);

//       var transfers = [];
//       var completes = [];

//       [transfers, completes] = await runEvm(transactionArray, chainInfo);

//       if (transfers.length > 0) {
//         write_to_table(connection, "INSERT_MANY", tableName, transfers);
//       }

//       if (completes.length > 0) {
//         write_to_table(connection, "INSERT_MANY_REDEEMS", tableName, completes);
//       }
//       await sleep(1000);
//     }
//     seq++;
//   }
//   return;
// }

(async () => {
  // get last known block number from table
  var missingTransfers = await getMissingTransfers(chain);
  console.log(`found ${missingTransfers.length} missing hashes`);
  const chainInfo = CHAIN_INFO_MAP[chain];
  var res = await grab(chainInfo, missingTransfers);
  connection.end();
})();
