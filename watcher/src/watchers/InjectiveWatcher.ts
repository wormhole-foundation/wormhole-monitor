import { CONTRACTS } from '@certusone/wormhole-sdk/lib/cjs/utils/consts';
import axios from 'axios';
import { RPCS_BY_CHAIN } from '../consts';
import { VaasByBlock } from '../databases/types';
import { makeBlockKey, makeVaaKey } from '../databases/utils';
import { Watcher } from './Watcher';
import { SHA256 } from 'jscrypto/SHA256';
import { Base64 } from 'jscrypto/Base64';

export class InjectiveWatcher extends Watcher {
  latestBlockTag: string;
  getBlockTag: string;
  hashTag: string;
  rpc: string | undefined;
  latestBlockHeight: number;

  constructor() {
    super('injective');
    this.rpc = RPCS_BY_CHAIN[this.chain];
    if (!this.rpc) {
      throw new Error(`${this.chain} RPC is not defined!`);
    }
    this.latestBlockHeight = 0;
    this.latestBlockTag = 'api/explorer/v1/blocks'; // This returns a page of the latest blocks
    this.getBlockTag = 'api/explorer/v1/blocks/';
    this.hashTag = 'api/explorer/v1/txs/';
  }

  /**
   * Calculates the transaction hash from Amino-encoded string.
   * @param data Amino-encoded string (base64)
   * Taken from https://github.com/terra-money/terra.js/blob/9e5f553de3ff3e975eaaf91b1f06e45658b1a5e0/src/util/hash.ts
   */
  hexToHash(data: string): string {
    return SHA256.hash(Base64.parse(data)).toString().toUpperCase();
  }

  async getFinalizedBlockNumber(): Promise<number> {
    const result: ExplorerBlocks = (await axios.get(`${this.rpc}/${this.latestBlockTag}`)).data;
    if (result && result.paging.total) {
      let blockHeight: number = result.paging.total;
      if (blockHeight !== this.latestBlockHeight) {
        this.latestBlockHeight = blockHeight;
        this.logger.info('blockHeight = ' + blockHeight);
      }
      return blockHeight;
    }
    throw new Error(`Unable to parse result of ${this.latestBlockTag} on ${this.rpc}`);
  }

  async getMessagesForBlocks(fromBlock: number, toBlock: number): Promise<VaasByBlock> {
    const address = CONTRACTS.MAINNET[this.chain].core;
    if (!address) {
      throw new Error(`Core contract not defined for ${this.chain}`);
    }
    this.logger.debug(`core contract for ${this.chain} is ${address}`);
    let vaasByBlock: VaasByBlock = {};
    this.logger.info(`fetching info for blocks ${fromBlock} to ${toBlock}`);

    // For each block number, call {RPC}/{getBlockTag}/{block_number}
    // Then call {RPC}/{hashTag}/{hash} to get the logs/events
    // Walk the logs/events

    for (let blockNumber = fromBlock; blockNumber <= toBlock; blockNumber++) {
      this.logger.debug('Getting block number ' + blockNumber);
      const blockResult: ExplorerMoreData = (
        await axios.get(`${this.rpc}/${this.getBlockTag}${blockNumber}`)
      ).data;
      if (!blockResult || !blockResult.data) {
        throw new Error('bad result for block ${blockNumber}');
      }
      const blockKey = makeBlockKey(
        blockNumber.toString(),
        new Date(blockResult.data.timestamp).toISOString()
      );
      vaasByBlock[blockKey] = [];
      let vaaKey: string = '';
      const numTxs: number = blockResult.data.num_txs;
      for (let i: number = 0; i < numTxs; i++) {
        const tx: ExplorerBlockResultDataTx = blockResult.data.txs[i];
        let hashInHex: string = tx.hash;
        let hash: string = this.hexToHash(hashInHex);
        this.logger.debug('blockNumber = ' + blockNumber + ', txHash[' + i + '] = ' + hashInHex);
        // The explorer uses the hex version of the hash
        const hashResult: ExplorerHashResult = (
          await axios.get(`${this.rpc}/${this.hashTag}${hashInHex}`)
        ).data;
        if (hashResult && hashResult.data.logs) {
          const numLogs = hashResult.data.logs.length;
          for (let j = 0; j < numLogs; j++) {
            if (hashResult.data.logs[j].events) {
              const numEvents = hashResult.data.logs[j].events.length;
              for (let k = 0; k < numEvents; k++) {
                let type: string = hashResult.data.logs[j].events[k].type;
                if (type === 'wasm') {
                  if (hashResult.data.logs[j].events[k].attributes) {
                    let attrs = hashResult.data.logs[j].events[k].attributes;
                    let emitter: string = '';
                    let sequence: string = '';
                    let coreContract: boolean = false;
                    // only care about _contract_address, message.sender and message.sequence
                    const numAttrs = attrs.length;
                    for (let l = 0; l < numAttrs; l++) {
                      const key = attrs[l].key;
                      if (key === 'message.sender') {
                        emitter = attrs[l].value;
                      } else if (key === 'message.sequence') {
                        sequence = attrs[l].value;
                      } else if (key === '_contract_address' || key === 'contract_address') {
                        let addr = attrs[l].value;
                        if (addr === address) {
                          coreContract = true;
                        }
                      }
                    }
                    if (coreContract && emitter !== '' && sequence !== '') {
                      vaaKey = makeVaaKey(hash, this.chain, emitter, sequence);
                      this.logger.debug('blockKey: ' + blockKey);
                      this.logger.debug('Making vaaKey: ' + vaaKey);
                      vaasByBlock[blockKey] = [...(vaasByBlock[blockKey] || []), vaaKey];
                    }
                  }
                }
              }
            }
          }
        } else {
          this.logger.error(`There were no hashResults for block number ${blockNumber}`);
        }
      }
    }
    return vaasByBlock;
  }
}

type ExplorerBlocks = {
  paging: { total: number; from: number; to: number };
  data: ExplorerBlocksData[];
};

type ExplorerBlocksData = {
  height: number;
  proposer: string;
  moniker: string;
  block_hash: string;
  parent_hash: string;
  num_pre_commits: number;
  num_txs: number;
  timestamp: string;
};

type ExplorerMoreData = {
  s: string; //'ok',
  data: {
    height: number; //20908590,
    proposer: string; //'injvalcons1uykwqng8hefrstwdcvsewhteg6phdmxwa34sph',
    moniker: string; //'Ping',
    block_hash: string; //'0xd4037ccb5f5043d5126b73f5c2ae8873a58740149cf4ece8cbfdd3d33d96cc67',
    parent_hash: string; //'0xebfc3c90b19996be9baecf9c62dd66646d4975a30d30518646c1dfc3d1fe9a47',
    num_pre_commits: number; //0,
    num_txs: number; //7,
    total_txs: number; //0,
    txs: ExplorerBlockResultDataTx[];
    timestamp: string; //'2022-12-05 22:09:51.851 +0000 UTC';
  };
};

type ExplorerBlockResultDataTx = {
  id: string; //'';
  block_number: number; //20908590;
  block_timestamp: string; //'2022-12-05 22:09:51.851 +0000 UTC';
  hash: string; //'0xea92fc8f9e0e191ce49475ff6b5c1038ee7d5f20124457ef43f495da6ebfa48f';
  messages: [{ type: string; value: [Object] }];
  tx_number: number; //133531009;
  error_log: string; //'';
  code: number; //0;
  tx_msg_types: string[]; //['/injective.exchange.v1beta1.MsgCancelDerivativeOrder'];
  logs: null;
};

type ExplorerHashResult = {
  s: string; //'ok';
  data: {
    id: string; //'';
    block_number: number; //20908590;
    block_timestamp: string; //'2022-12-05 22:09:51.851 +0000 UTC';
    hash: string; //'0xea92fc8f9e0e191ce49475ff6b5c1038ee7d5f20124457ef43f495da6ebfa48f';
    code: number; //0;
    data: string; //'CiYKJC9jb3Ntd2FzbS53YXNtLnYxLk1zZ0V4ZWN1dGVDb250cmFjdA==';
    info: string; //'';
    gas_wanted: number; //400000;
    gas_used: number; //314387;
    gas_fee: {
      amount: [{ denom: string; amount: string }];
      gas_limit: number; //400000;
      payer: string; //'inj1ut3dncca0cwvz9u0urgut9g0djqfs94r2u2k80';
      granter: string; //'';
    };
    codespace: string; //'';
    tx_type: string; //'injective';
    messages: ExplorerResultMessage[];
    signatures: ExplorerResultSignature[];
    memo: string; //'';
    tx_number: number; //22;
    block_unix_timestamp: number; //1670278191851;
    error_log: string; //'';
    logs: ExplorerLogsType[];
  };
};

type ExplorerLogsType = {
  events: ExplorerEventsType[];
};

type ExplorerEventsType = {
  type: string;
  attributes: [
    {
      key: string;
      value: string;
    }
  ];
};

type ExplorerResultMessage = {
  type: string;
  value: {
    contract: string;
    sender: string;
  };
};

type ExplorerResultSignature = {
  pubkey: string;
  address: string;
  sequence: number;
  signature: string;
};
