import { CONTRACTS } from '@certusone/wormhole-sdk/lib/cjs/utils';
import { WormholeMessage } from '@wormhole-foundation/wormhole-monitor-common';
import { AptosClient, Types } from 'aptos';
import { RPCS_BY_CHAIN } from '../consts';
import { Watcher } from './Watcher';

const APTOS_CORE_BRIDGE_ADDRESS = CONTRACTS.MAINNET.aptos.core;
const APTOS_EVENT_HANDLE = `${APTOS_CORE_BRIDGE_ADDRESS}::state::WormholeMessageHandle`;
const APTOS_FIELD_NAME = 'event';

/**
 * NOTE: The Aptos watcher differs from other watchers in that it uses the event sequence number to
 * fetch Wormhole messages and therefore also stores sequence numbers instead of block numbers.
 */
export class AptosWatcher extends Watcher {
  client: AptosClient;
  maximumBatchSize: number = 25;

  constructor() {
    super('aptos');
    this.client = new AptosClient(RPCS_BY_CHAIN[this.chain]!);
  }

  async getFinalizedBlockNumber(): Promise<number> {
    return Number(
      (
        await this.client.getEventsByEventHandle(
          APTOS_CORE_BRIDGE_ADDRESS,
          APTOS_EVENT_HANDLE,
          APTOS_FIELD_NAME,
          { limit: 1 }
        )
      )[0].sequence_number
    );
  }

  async getMessagesForBlocks(fromSequence: number, toSequence: number): Promise<WormholeMessage[]> {
    const limit = toSequence - fromSequence + 1;
    const events: WormholeMessageEvent[] = (await this.client.getEventsByEventHandle(
      APTOS_CORE_BRIDGE_ADDRESS,
      APTOS_EVENT_HANDLE,
      APTOS_FIELD_NAME,
      { start: fromSequence, limit }
    )) as WormholeMessageEvent[];
    return Promise.all(
      events.map(async ({ data, sequence_number, version }) => {
        const [block, transaction] = await Promise.all([
          this.client.getBlockByVersion(Number(version)),
          this.client.getTransactionByVersion(Number(version)),
        ]);
        return {
          key: [this.chain, sequence_number, version].join('/'),
          blockNumber: block.block_height,
          timestamp: new Date(Number(block.block_timestamp)).toISOString(),
          transactionHash: transaction.hash,
          chain: this.chain,
          emitter: data.sender.padStart(64, '0'),
          sequence: data.sequence,
        };
      })
    );
  }
}

type WormholeMessageEvent = Omit<Types.Event, 'data'> & {
  version: string;
  data: {
    consistency_level: number;
    nonce: string;
    payload: string;
    sender: string;
    sequence: string;
    timestamp: string;
  };
};
