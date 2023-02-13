import { getTxns, sleep, CHAIN_INFO_MAP, getSolanaTxns } from './transaction_helper';
import { getQuery, write_to_table } from './mysqldb';
import { runSolana } from './index';
import connection from './mysqldb2';

require('dotenv').config();

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

async function getBookEndHashesBySeq(chain, seq) {
  var sql_query0 = `SELECT source_hash FROM ${tableName} WHERE chain_id=${chain} and seq < ${seq} and seq is not null and source_hash is not null order by seq DESC limit 1`;
  var startHashes;
  await getQuery(connection, sql_query0)
    .then(function (results) {
      startHashes = results;
    })
    .catch(function (err) {
      console.log('Promise rejection error: ' + err);
    });

  var sql_query1 = `SELECT source_hash, source_timestamp FROM ${tableName} WHERE chain_id=${chain} and seq > ${seq} and seq is not null and source_hash is not null order by seq limit 1`;
  var endHashes;
  await getQuery(connection, sql_query1)
    .then(function (results) {
      endHashes = results;
    })
    .catch(function (err) {
      console.log('Promise rejection error: ' + err);
    });
  return [startHashes, endHashes];
}

// which sweet chain bro?
const chain = 1;
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
    var hashes = await getBookEndHashesBySeq(chain, seq);
    console.log(hashes);
    var startHash = undefined;
    var endHash = undefined;
    var endTime = undefined;
    if (hashes.length > 1) {
      if (hashes[0].length > 0) {
        startHash = hashes[0][0]?.source_hash;
      }
      if (hashes[1].length > 0) {
        endHash = hashes[1][0]?.source_hash;
        endTime = hashes[1][0]?.source_time;
      }
    }

    console.log(missingTransfer);
    console.log(startHash, endHash, endTime);
    let transactionArray = await getSolanaTxns(chainInfo, startHash, endHash, endTime, '0');

    var transfers = [];
    var completes = [];

    [transfers, completes] = await runSolana(transactionArray, chainInfo);

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
//       var hashes = await getBookEndHashesBySeq(chain, seq);
//       console.log(hashes);
//       var startHash = undefined;
//       var endHash = undefined;
//       if (hashes.length > 1) {
//         if (hashes[0].length > 0) {
//           startHash = hashes[0][0]?.source_hash;
//         }
//         if (hashes[1].length > 0) {
//           endHash = hashes[1][0]?.source_hash;
//         }
//       }

//       console.log(startHash, endHash);
//       let transactionArray = await getTxns(chainInfo, startHash, endHash);

//       var transfers = [];
//       var completes = [];

//       [transfers, completes] = await runSolana(transactionArray, chainInfo);

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
  console.log(chainInfo);
  var res = await grab(chainInfo, missingTransfers);

  // var seqs = await getMissingTransfersBySeqs(chain)
  // var res = await grabBySeqs(chainInfo, seqs)
  connection.end();
})();
