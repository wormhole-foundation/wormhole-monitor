import dotenv from 'dotenv';
dotenv.config();

import { getPostedMessage } from '@certusone/wormhole-sdk/lib/cjs/solana/wormhole/accounts/postedVaa';
import { CONTRACTS } from '@certusone/wormhole-sdk/lib/cjs/utils/consts';
import { Commitment, Connection, VersionedTransactionResponse } from '@solana/web3.js';
import { RPCS_BY_CHAIN } from '../src/consts';
import { makeBlockKey, makeVaaKey } from '../src/databases/utils';
import { isLegacyMessage, normalizeCompileInstruction } from '../src/utils/solana';

const helper = async (connection: Connection, res: VersionedTransactionResponse) => {
  const WORMHOLE_PROGRAM_ID = CONTRACTS.MAINNET.solana.core;
  const COMMITMENT: Commitment = 'finalized';

  const message = res.transaction.message;
  const accountKeys = isLegacyMessage(message) ? message.accountKeys : message.staticAccountKeys;
  const programIdIndex = accountKeys.findIndex((i) => i.toBase58() === WORMHOLE_PROGRAM_ID);
  const instructions = message.compiledInstructions;
  const innerInstructions =
    res.meta?.innerInstructions?.flatMap((i) => i.instructions.map(normalizeCompileInstruction)) ||
    [];
  const whInstructions = innerInstructions
    .concat(instructions)
    .filter((i) => i.programIdIndex === programIdIndex);
  for (const instruction of whInstructions) {
    // skip if not postMessage instruction
    const instructionId = instruction.data;
    if (instructionId[0] !== 0x01 && instructionId[0] !== 0x08) continue;

    const accountId = accountKeys[instruction.accountKeyIndexes[1]];
    const {
      message: { emitterAddress, sequence },
    } = await getPostedMessage(connection, accountId.toBase58(), COMMITMENT);
    const blockKey = makeBlockKey(
      res.slot.toString(),
      new Date(res.blockTime! * 1000).toISOString()
    );
    const vaaKey = makeVaaKey(
      res.transaction.signatures[0],
      'aptos',
      emitterAddress.toString('hex'),
      sequence.toString()
    );
    console.log(blockKey, vaaKey);
  }
};

(async () => {
  // const json = JSON.parse(fs.readFileSync('messages.json', 'utf8'));
  // console.log(Object.values(json).flat().length);

  const connection = new Connection(RPCS_BY_CHAIN.solana!, 'finalized');
  const txs = await connection.getTransactions(
    ['2E5cZudbrNKKX2ZcrvBaQgDdVbyseGpNDmt1tBSczDxxnwjB862tjv6m6VyoRt4sYrqPmoEy1FP9zXxP1F9SprpV'],
    { maxSupportedTransactionVersion: 0 }
  );
  helper(connection, txs[0]!);
})();
