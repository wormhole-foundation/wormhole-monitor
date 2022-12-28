import {
  ChainId,
  ChainName,
  coalesceChainName,
} from '@certusone/wormhole-sdk/lib/cjs/utils/consts';
import { sleep, WormholeMessage } from '@wormhole-foundation/wormhole-monitor-common';
import * as dotenv from 'dotenv';
import { BigtableDatabase } from '../src/databases/BigtableDatabase';
import { JsonDatabase } from '../src/databases/JsonDatabase';

dotenv.config();

function chunkArray<T>(arr: T[], size: number): T[][] {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

const CHUNK_SIZE = 1000;

(async () => {
  const localDb = new JsonDatabase();
  const remoteDb = new BigtableDatabase();

  const dbEntries: [ChainName, WormholeMessage[]][] = Object.entries(localDb.db).map(
    ([chainIdStr, messagesByKey]) => [
      coalesceChainName(Number(chainIdStr) as ChainId),
      Object.values(messagesByKey),
    ]
  );
  for (const [chain, messages] of dbEntries) {
    console.log('backfilling', chain);
    const chunks = chunkArray(messages, CHUNK_SIZE);
    let chunkIdx = 1;
    for (const chunk of chunks) {
      console.log('chunk', chunkIdx++, 'of', chunks.length);
      await remoteDb.storeWormholeMessages(coalesceChainName(Number(chain) as ChainId), chunk);
      await sleep(500);
    }
  }

  const lastBlockEntries = Object.entries(localDb.lastMessageByChain);
  for (const [chain, message] of lastBlockEntries) {
    console.log('backfilling last block for', chain, message.key, message.timestamp);
    await remoteDb.storeLatestMessageByChain(coalesceChainName(Number(chain) as ChainId), message);
    await sleep(500);
  }
})();
