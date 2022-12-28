import { CHAIN_ID_SOLANA } from '@certusone/wormhole-sdk/lib/cjs/utils/consts';
import { expect, test } from '@jest/globals';
import {
  INITIAL_DEPLOYMENT_BLOCK_BY_CHAIN,
  WormholeMessage,
} from '@wormhole-foundation/wormhole-monitor-common';
import { MESSAGE_KEY_SEPARATOR } from '../../consts';
import { JsonDatabase } from '../JsonDatabase';
import { getLastBlockNumberByChain, initDb } from '../utils';

test('getLastBlockNumberByChain', async () => {
  const db = initDb() as JsonDatabase;
  const fauxMessage: WormholeMessage = {
    key: [CHAIN_ID_SOLANA, 98765, 0].join(MESSAGE_KEY_SEPARATOR),
    blockNumber: '98765',
    timestamp: '1234567890',
    transactionHash: '0x123456',
    chain: 'solana',
    emitter: '0x123456',
    sequence: '0',
  };
  db.lastMessageByChain = { [CHAIN_ID_SOLANA]: fauxMessage };

  // if a chain is in the database, that number should be returned
  expect((await db.getLastMessageByChain('solana'))?.key).toEqual(fauxMessage.key);
  expect(await getLastBlockNumberByChain('solana')).toEqual(
    Number(fauxMessage.key.split(MESSAGE_KEY_SEPARATOR)[1])
  );

  // if a chain is not in the database, the initial deployment block should be returned
  expect(INITIAL_DEPLOYMENT_BLOCK_BY_CHAIN.moonbeam).toBeDefined();
  expect(await getLastBlockNumberByChain('moonbeam')).toEqual(
    Number(INITIAL_DEPLOYMENT_BLOCK_BY_CHAIN.moonbeam)
  );

  // if neither, null should be returned
  expect(INITIAL_DEPLOYMENT_BLOCK_BY_CHAIN.unset).toBeUndefined();
  expect(await getLastBlockNumberByChain('unset')).toEqual(null);
});
