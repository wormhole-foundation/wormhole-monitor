import { sleep } from '@wormhole-foundation/wormhole-monitor-common';
import axios from 'axios';
import { EVM_RPCS_BY_CHAIN } from '../consts';
import { EVMWatcher, EVM_AXIOS_CONFIG } from './EVMWatcher';

export class MoonbeamWatcher extends EVMWatcher {
  constructor() {
    super('moonbeam');
  }
  async getFinalizedBlockNumber(): Promise<number> {
    const latestBlock = await super.getFinalizedBlockNumber();
    let isBlockFinalized = false;
    while (!isBlockFinalized) {
      if (!EVM_RPCS_BY_CHAIN.moonbeam) {
        throw new Error('Moonbeam RPC is not defined!');
      }
      await sleep(100);
      // refetch the block by number to get an up-to-date hash
      try {
        const blockFromNumber = await this.getBlock(latestBlock);
        isBlockFinalized =
          (
            await axios.post(
              EVM_RPCS_BY_CHAIN.moonbeam,
              [
                {
                  jsonrpc: '2.0',
                  id: 1,
                  method: 'moon_isBlockFinalized',
                  params: [blockFromNumber.hash],
                },
              ],
              EVM_AXIOS_CONFIG
            )
          )?.data?.[0]?.result || false;
      } catch (e) {
        this.logger.error(`error while trying to check for finality of block ${latestBlock}`);
      }
    }
    return latestBlock;
  }
}
