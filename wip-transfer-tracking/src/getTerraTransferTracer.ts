import { getTxns, sleep, CHAIN_INFO_MAP } from './transaction_helper';
import { getQuery, write_to_table } from './mysqldb';
import { runTerra } from './index';
import connection from './mysqldb2';

require('dotenv').config();

const terraIndexer = `${process.env.WORMHOLE_DB}.terra_classic_indexer`;

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

async function getMissingTransfersBySeqs(chain) {
  var sql_query = `SELECT seq, source_block FROM ${tableName} WHERE chain_id=${chain} and seq is not null order by seq`;
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

async function getBookEndTimesBySeq(chain, seq) {
  var sql_query0 = `SELECT source_time FROM ${tableName} WHERE chain_id=${chain} and seq < ${seq} and seq is not null and source_hash is not null order by seq DESC limit 1`;
  var startTime;
  await getQuery(connection, sql_query0)
    .then(function (results) {
      startTime = results;
    })
    .catch(function (err) {
      console.log('Promise rejection error: ' + err);
    });

  var sql_query1 = `SELECT source_time FROM ${tableName} WHERE chain_id=${chain} and seq > ${seq} and seq is not null and source_hash is not null order by seq limit 1`;
  var endTime;
  await getQuery(connection, sql_query1)
    .then(function (results) {
      endTime = results;
    })
    .catch(function (err) {
      console.log('Promise rejection error: ' + err);
    });
  return [startTime, endTime];
}

async function getBookEndHashes(terraIndexer, t0) {
  var sql_query0 = `SELECT * FROM ${terraIndexer} WHERE time < '${t0}' order by time DESC limit 1`;
  console.log(sql_query0);
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
const chain = 3;
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
    const targetAddress = missingTransfer.target_address;
    const seq = missingTransfer.seq;
    console.log('missing seq=', seq);
    var times = await getBookEndTimesBySeq(chain, seq);
    var startHash = undefined;
    var endId = undefined;
    if (times.length > 1) {
      if (times[0].length > 0) {
        const startTime = times[0][0]?.source_time;
        const startHashInterval = await getBookEndHashes(terraIndexer, startTime.toISOString());
        startHash = startHashInterval[0][0]?.hash;
      }
      if (times[1].length > 0) {
        const endTime = times[1][0]?.source_time;
        const endHashInterval = await getBookEndHashes(terraIndexer, endTime.toISOString());
        endId = endHashInterval[1][0]?.next_id;
      }
    }

    console.log(missingTransfer);
    console.log(startHash, endId);

    let transactionArray = await getTxns(chainInfo, startHash, endId);
    const prevTxnRows = await getPrevHashes(chain);
    const prevTxnHashes = prevTxnRows.map((txn) => txn.hash);
    console.log(`filtering ${prevTxnHashes.length} prev hashes`);
    transactionArray = transactionArray.filter((txn) => !prevTxnHashes.includes(txn.txhash));
    console.log(`now there are ${transactionArray.length} transactions for chain_id=${chain}`);

    var transfers = [];
    var completes = [];

    [transfers, completes] = await runTerra(transactionArray, chainInfo);

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

async function grabBySeqs(chainInfo, seqs) {
  let i = 0;
  let seq = 0;
  if (seqs.length > 0) {
    seq = seqs[0].seq; //starting seq
  } else {
    return;
  }
  const maxSeq = seqs[seqs.length - 1].seq;
  console.log('max seq=', maxSeq);
  while (seq < maxSeq) {
    const transfer = seqs.filter((x) => x.seq == seq);

    if (transfer.length == 0) {
      console.log('missing seq=', seq);
      // var times = await getBookEndTimesBySeq(chain, seq)
      // console.log(times)
      // var startHash = undefined;
      // var endId = undefined;
      // if (times.length > 1){
      //     if (times[0].length > 0){
      //         const startTime = times[0][0]?.source_time
      //         const startHashInterval = await getBookEndHashes(terraIndexer, startTime)
      //         startHash = startHashInterval[0][0]?.hash
      //     }
      //     if(times[1].length > 0){
      //         const endTime = times[1][0]?.source_time
      //         const endHashInterval = await getBookEndHashes(terraIndexer, endTime)
      //         endId = endHashInterval[1][0]?.next_id
      //     }
      // }

      // console.log(startHash, endId)
      // let transactionArray = await getTxns(chainInfo, startHash, endId)

      // var transfers = [];
      // var completes = [];

      // [transfers, completes] = await runTerra(transactionArray, chainInfo)

      // if (transfers.length > 0){
      //     write_to_table(connection, 'INSERT_MANY', tableName, transfers)
      // }

      // if (completes.length > 0) {
      //     write_to_table(connection, 'INSERT_MANY', tableName, completes)

      // }
      await sleep(1000);
    }
    seq++;
  }
  return;
}

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
