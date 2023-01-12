import * as dotenv from 'dotenv';
dotenv.config();
import { ChainName } from '@certusone/wormhole-sdk/lib/cjs/utils/consts';
import ora from 'ora';
import { initDb, makeBlockKey, makeVaaKey } from '../src/databases/utils';
import axios from 'axios';
import { VaasByBlock } from '../src/databases/types';
import { writeFileSync } from 'fs';
import { RPCS_BY_CHAIN } from '../src/consts';

// Ensure `DB_SOURCE` and Bigtable environment variables are set to backfill Bigtable database.
// Otherwise, the script will backfill the local JSON database.
//   terra: 'https://columbus-lcd.terra.dev',
// Note: when grabbing terra classic transactions via web: https://columbus-fcd.terra.dev/v1/txs?limit=100&offset=277262223&account=terra1dq03ugtd40zu9hcgdzrsq6z2z4hwhc9tqk2uy5
// transactions in the same block can span across pages. so prev contents of blockKeys can be overwritten by the next page
// may need to update the storeVaasByBlock code to make sure the array of vaaKeys get appended to, not overwritten
// then run backfill.ts to write to bigtable, then run locateMessageGaps or fetchMissingVaas
const ENCODING = 'utf8';
const BATCH_SIZE = 100;
const TERRA_CORE = 'terra1dq03ugtd40zu9hcgdzrsq6z2z4hwhc9tqk2uy5';
const terraIndexerFile = '/home/ceun/wormhole-monitor/watcher/terraIndexer.json';
const terraRpc = RPCS_BY_CHAIN['terra'];
const terraHashTag = 'cosmos/tx/v1beta1/txs/';
// var fd = openSync(
//   process.env.JSON_DB_FILE || '/home/ceun/wormhole-monitor/watcher/terraBackfill.json',
//   'a'
// );

type TxResponse = {
  id: string;
  chainId: string;
  height: string;
  txhash: string;
  codespace: string;
  code: 0;
  data: string;
  raw_log: string;
  logs: [{ msg_index: number; log: string; events: EventsType[] }];
  info: string;
  gas_wanted: string;
  gas_used: string;
  tx: {
    '@type': '/cosmos.tx.v1beta1.Tx';
    body: {
      messages: [
        {
          '@type': '/cosmos.staking.v1beta1.MsgBeginRedelegate';
          delegator_address: string;
          validator_src_address: string;
          validator_dst_address: string;
          amount: { denom: string; amount: string };
        }
      ];
      memo: '';
      timeout_height: '0';
      extension_options: [];
      non_critical_extension_options: [];
    };
    auth_info: {
      signer_infos: [
        {
          public_key: {
            '@type': '/cosmos.crypto.secp256k1.PubKey';
            key: string;
          };
          mode_info: { single: { mode: string } };
          sequence: string;
        }
      ];
      fee: {
        amount: [{ denom: string; amount: string }];
        gas_limit: string;
        payer: string;
        granter: string;
      };
    };
    signatures: string[];
  };
  timestamp: string; // eg. '2023-01-03T12:12:54Z'
  events: EventsType[];
};

type EventsType = {
  type: string;
  attributes: [
    {
      key: string;
      value: string;
      index: boolean;
    }
  ];
};

export async function getTerraTransactions(startId: string): Promise<TxResponse[]> {
  let url =
    startId !== ''
      ? `https://columbus-fcd.terra.dev/v1/txs?limit=${BATCH_SIZE}&offset=${startId}&account=${TERRA_CORE}`
      : `https://columbus-fcd.terra.dev/v1/txs?limit=${BATCH_SIZE}&account=${TERRA_CORE}`;
  console.log(url);

  const response = await axios.get(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
  const dataResponse = response?.data;

  const transactions = dataResponse?.txs || [];

  return transactions;
}

(async () => {
  try {
    let ix = 0;
    const db = initDb();
    const chain: ChainName = 'terra';
    const getLastId = require(terraIndexerFile);
    // fetch all transactions for core bridge contract from explorer
    let log = ora('Fetching transactions from Terra Explorer...').start();
    let startId = getLastId.lastId || ''; //; //'285127454'
    if (startId !== '') {
      console.log('setting start Id = ', startId);
    }
    while (true) {
      const transactions: TxResponse[] = await getTerraTransactions(startId);
      log.succeed(
        `Fetched ${ix} * ${transactions.length} transactions from Terra Explorer at ${new Date()}`
      );
      let vaasByBlock: VaasByBlock = {};

      for (const tx of transactions) {
        if (tx && tx?.raw_log) {
          const hash = tx.txhash;
          const blockKey = makeBlockKey(tx.height.toString(), new Date(tx.timestamp).toISOString());
          const logs = JSON.parse(tx.raw_log);
          for (const log of logs) {
            const events = log?.events;
            for (const event of events) {
              let type: string = event.type;
              if (type === 'wasm') {
                if (event.attributes) {
                  let attrs = event.attributes;
                  let emitter: string = '';
                  let sequence: string = '';
                  let coreContract: boolean = false;
                  // only care about _contract_address, message.sender and message.sequence
                  for (const attr of attrs) {
                    const key = attr.key;
                    if (key === '_contract_address' || key === 'contract_address') {
                      let addr = attr.value;
                      if (addr === TERRA_CORE) {
                        coreContract = true;
                      }
                    }
                    if (coreContract) {
                      if (key === 'message.sender') {
                        emitter = attr.value;
                      } else if (key === 'message.sequence') {
                        sequence = attr.value;
                      }
                    }
                    if (emitter !== '' && sequence !== '' && coreContract === true) {
                      let vaaKey = makeVaaKey(hash, 3, emitter, sequence);
                      console.log('blockKey: ' + blockKey);
                      console.log('Making vaaKey: ' + vaaKey);
                      vaasByBlock[blockKey] = [...(vaasByBlock[blockKey] || []), vaaKey];
                      emitter = '';
                      sequence = '';
                      coreContract = false;
                    }
                  }
                }
              }
            }
          }
        }
      }
      log.succeed(`Fetched ${Object.keys(vaasByBlock).length} messages!`);
      await db.storeVaasByBlock(chain, vaasByBlock);
      log.succeed('Updating messages to db successfully');
      let indexedInfo = {
        lastId: transactions[transactions.length - 1].id,
        lastTimestamp: transactions[transactions.length - 1].timestamp,
      };
      if (indexedInfo.lastId) {
        writeFileSync(terraIndexerFile, JSON.stringify(indexedInfo), ENCODING);
      }
      console.log('Updated terra index successfully');

      startId = indexedInfo.lastId;
      ix++;
    }
  } catch (e) {
    console.log(e);
  }
})();
