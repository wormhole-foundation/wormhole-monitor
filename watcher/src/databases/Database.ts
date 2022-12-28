import { ChainName } from '@certusone/wormhole-sdk/lib/cjs/utils/consts';
import { WormholeMessage } from '@wormhole-foundation/wormhole-monitor-common';
import { getLogger, WormholeLogger } from '../utils/logger';

export class Database {
  logger: WormholeLogger;
  constructor() {
    this.logger = getLogger('db');
  }

  async getLastMessageByChain(chain: ChainName): Promise<WormholeMessage | null> {
    throw new Error('Not Implemented');
  }

  async storeWormholeMessages(chain: ChainName, messages: WormholeMessage[]): Promise<void> {
    throw new Error('Not Implemented');
  }
}
