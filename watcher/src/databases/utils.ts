import { ChainName } from '@certusone/wormhole-sdk/lib/cjs/utils/consts';
import {
  INITIAL_DEPLOYMENT_BLOCK_BY_CHAIN,
  padUint16,
  padUint64,
  WormholeMessage,
} from '@wormhole-foundation/wormhole-monitor-common';
import { DB_SOURCE, MESSAGE_KEY_SEPARATOR } from '../consts';
import { BigtableDatabase } from './BigtableDatabase';
import { Database } from './Database';
import { JsonDatabase } from './JsonDatabase';

let database: Database = new Database();
export const initDb = (): Database => {
  database = DB_SOURCE === 'bigtable' ? new BigtableDatabase() : new JsonDatabase();
  return database;
};

export const getLastBlockNumberByChain = async (chain: ChainName): Promise<number | null> => {
  const blockNumber: string | undefined =
    (await database.getLastMessageByChain(chain))?.blockNumber ??
    INITIAL_DEPLOYMENT_BLOCK_BY_CHAIN[chain];
  return blockNumber === undefined ? null : Number(blockNumber);
};

export const compareWormholeMessages = (m1: WormholeMessage, m2: WormholeMessage): number => {
  const tokens1 = m1.key.split(MESSAGE_KEY_SEPARATOR).map(Number);
  const tokens2 = m2.key.split(MESSAGE_KEY_SEPARATOR).map(Number);

  // account for case where first key is empty string or second key has additional identifiers
  return (
    tokens1.reduce((a, b, i) => a || b - (tokens2[i] || 0), 0) || tokens2.length - tokens1.length
  );
};

export const padWormholeMessageKey = (key: string) => {
  const [chainId, blockNumber, transactionIndex] = key.split(MESSAGE_KEY_SEPARATOR);
  return [padUint16(chainId), padUint64(blockNumber), padUint64(transactionIndex)].join(
    MESSAGE_KEY_SEPARATOR
  );
};

export const storeWormholeMessages = async (
  chain: ChainName,
  messages: WormholeMessage[]
): Promise<void> => {
  return database.storeWormholeMessages(chain, messages);
};

export function printRow(rowkey: string, rowData: { [x: string]: any }) {
  console.log(`Reading data for ${rowkey}:`);

  for (const columnFamily of Object.keys(rowData)) {
    const columnFamilyData = rowData[columnFamily];
    console.log(`Column Family ${columnFamily}`);

    for (const columnQualifier of Object.keys(columnFamilyData)) {
      const col = columnFamilyData[columnQualifier];

      for (const cell of col) {
        const labels = cell.labels.length ? ` [${cell.labels.join(',')}]` : '';
        console.log(`\t${columnQualifier}: ${cell.value} @${cell.timestamp}${labels}`);
      }
    }
  }
  console.log();
}
