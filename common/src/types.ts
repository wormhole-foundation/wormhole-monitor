import { ChainId, ChainName } from '@certusone/wormhole-sdk/lib/cjs/utils/consts';

export type MessagesByChain = {
  [chain in ChainId]?: {
    [key: string]: WormholeMessage;
  };
};

export type LastMessageByChain = {
  [chain in ChainId]?: WormholeMessage;
};

export type WormholeMessage = {
  key: string; // 'chainId/blockNumber/transactionIndex'
  blockNumber: string;
  timestamp: string;
  transactionHash: string;
  chain: ChainName;
  emitter: string;
  sequence: string;
};
