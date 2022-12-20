import axios from 'axios';
import { EVM_RPCS_BY_CHAIN } from '../consts';
import { sleep } from '../utils';
import { EVMWatcher } from './EVMWatcher';

export class MoonbeamWatcher extends EVMWatcher {
  constructor() {
    super('moonbeam');
  }
  async getFinalizedBlockNumber(): Promise<number | null> {
    const latestBlock = await super.getFinalizedBlockNumber();
    if (latestBlock !== null) {
      let isBlockFinalized = false;
      while (!isBlockFinalized) {
        if (!EVM_RPCS_BY_CHAIN.moonbeam) {
          throw new Error('Moonbeam RPC is not defined!');
        }
        await sleep(100);
        // refetch the block by number to get an up-to-date hash
        try {
          const blockFromNumber = await this.provider.getBlock(latestBlock);
          isBlockFinalized =
            (
              await axios.post(EVM_RPCS_BY_CHAIN.moonbeam, [
                {
                  jsonrpc: '2.0',
                  id: '1',
                  method: 'moon_isBlockFinalized',
                  params: [blockFromNumber.hash],
                },
              ])
            )?.data?.[0]?.result || false;
        } catch (e) {
          this.logger.error(`error while trying to check for finality of block ${latestBlock}`);
        }
      }
    }
    return null;
  }
}