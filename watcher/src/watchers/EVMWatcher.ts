import { CeloProvider } from '@celo-tools/celo-ethers-wrapper';
import { CONTRACTS, ethers_contracts, EVMChainName } from '@certusone/wormhole-sdk';
import { ethers } from 'ethers';
import { EVM_RPCS_BY_CHAIN } from '../consts';
import { makeBlockKey, makeVaaKey, VaasByBlock } from '../db';
import { Watcher } from './Watcher';

export type EVMProvider =
  | ethers.providers.JsonRpcProvider
  | ethers.providers.JsonRpcBatchProvider
  | CeloProvider;

const wormholeInterface = ethers_contracts.Implementation__factory.createInterface();

export type BlockTag = 'finalized' | 'safe' | 'latest';

export class EVMWatcher extends Watcher {
  finalizedBlockTag: BlockTag;
  provider: EVMProvider;
  constructor(chain: EVMChainName, finalizedBlockTag: BlockTag = 'latest') {
    super(chain);
    this.finalizedBlockTag = finalizedBlockTag;
    if (chain === 'celo') {
      this.provider = new CeloProvider(EVM_RPCS_BY_CHAIN[chain]);
    } else {
      this.provider = new ethers.providers.JsonRpcBatchProvider(EVM_RPCS_BY_CHAIN[chain]);
    }
  }
  async getFinalizedBlockNumber(): Promise<number | null> {
    try {
      this.logger.info(`fetching block ${this.finalizedBlockTag}`);
      const block = await this.provider.getBlock(this.finalizedBlockTag);
      return block.number;
    } catch (e) {
      this.logger.error(e);
      return null;
    }
  }
  async getMessagesForBlocks(fromBlock: number, toBlock: number): Promise<VaasByBlock> {
    // TODO: fix avax "requested to block 23734227 after last accepted block 23734226"
    const logs = await this.provider.getLogs({
      fromBlock,
      toBlock,
      address: CONTRACTS.MAINNET[this.chain].core,
      topics: ['0x6eb224fb001ed210e379b335e35efe88672a8ce935d981a6896b27ffdf52a3b2'],
    });
    const timestampsByBlock: { [block: number]: string } = {};
    // fetch timestamps for each block
    const vaasByBlock: VaasByBlock = {};
    const blockPromises = [];
    this.logger.info(`fetching info for blocks ${fromBlock} to ${toBlock}`);
    for (let blockNumber = fromBlock; blockNumber <= toBlock; blockNumber++) {
      blockPromises.push(this.provider.getBlock(blockNumber));
    }
    const blocks = await Promise.all(blockPromises);
    for (const block of blocks) {
      // TODO: fix "Cannot read properties of null (reading 'timestamp')" (fantom)
      if (!block) {
        this.logger.error(`bad block`);
      }
      const timestamp = new Date(block.timestamp * 1000).toISOString();
      timestampsByBlock[block.number] = timestamp;
      vaasByBlock[makeBlockKey(block.number.toString(), timestamp)] = [];
    }
    this.logger.info(`processing ${logs.length} logs`);
    for (const log of logs) {
      const blockNumber = log.blockNumber;
      const emitter = log.topics[1].slice(2);
      const {
        args: { sequence },
      } = wormholeInterface.parseLog(log);
      const vaaKey = makeVaaKey(log.transactionHash, this.chain, emitter, sequence.toString());
      const blockKey = makeBlockKey(blockNumber.toString(), timestampsByBlock[blockNumber]);
      vaasByBlock[blockKey] = [...(vaasByBlock[blockKey] || []), vaaKey];
    }
    return vaasByBlock;
  }
}