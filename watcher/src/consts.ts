import { ChainName, CONTRACTS } from '@certusone/wormhole-sdk/lib/cjs/utils/consts';
import { AxiosRequestConfig } from 'axios';

export const TIMEOUT = 0.5 * 1000;

// Notes about RPCs
// Ethereum
//   ethereum: "https://rpc.ankr.com/eth", // "finalized" does not work on Ankr as of 2022-12-16
// BSC
//   https://docs.bscscan.com/misc-tools-and-utilities/public-rpc-nodes
//   bsc: "https://bsc-dataseed1.binance.org", // Cannot read properties of undefined (reading 'error')
//   'https://rpc.ankr.com/bsc' has been very slow, trying a diff rpc
// Avalanche
//   https://docs.avax.network/apis/avalanchego/public-api-server
//   avalanche: "https://api.avax.network/ext/bc/C/rpc", // 500 error on batch request
// Fantom
//   fantom: "https://rpc.ftm.tools", // Cannot read properties of null (reading 'timestamp')"
// Klaytn
// this one immediately 429s
// klaytn: 'https://public-node-api.klaytnapi.com/v1/cypress',

export const RPCS_BY_CHAIN: { [key in ChainName]?: string } = {
  ethereum: process.env.ETH_RPC,
  bsc: process.env.BSC_RPC || 'https://bsc-dataseed2.defibit.io',
  polygon: 'https://rpc.ankr.com/polygon',
  avalanche: 'https://rpc.ankr.com/avalanche',
  oasis: 'https://emerald.oasis.dev',
  algorand: 'https://mainnet-api.algonode.cloud',
  fantom: 'https://rpc.ankr.com/fantom',
  karura: 'https://eth-rpc-karura.aca-api.network',
  acala: 'https://eth-rpc-acala.aca-api.network',
  klaytn: 'https://klaytn-mainnet-rpc.allthatnode.com:8551',
  celo: 'https://forno.celo.org',
  moonbeam: 'https://rpc.ankr.com/moonbeam',
  arbitrum: 'https://arb1.arbitrum.io/rpc',
  aptos: 'https://fullnode.mainnet.aptoslabs.com/',
  near: 'https://rpc.mainnet.near.org',
};

// Separating for now so if we max out infura we can keep Polygon going
export const POLYGON_ROOT_CHAIN_RPC = 'https://rpc.ankr.com/eth';
export const POLYGON_ROOT_CHAIN_ADDRESS = '0x86E4Dc95c7FBdBf52e33D563BbDB00823894C287';

export const ALGORAND_INFO = {
  appid: Number(CONTRACTS.MAINNET.algorand.core),
  algodToken: '',
  algodServer: RPCS_BY_CHAIN.algorand,
  algodPort: 443,
  server: 'https://mainnet-idx.algonode.cloud',
  port: 443,
  token: '',
};

export const DB_SOURCE = process.env.DB_SOURCE || 'local';
export const JSON_DB_FILE = process.env.JSON_DB_FILE || '../server/db.json';
export const DB_LAST_BLOCK_FILE =
  process.env.DB_LAST_BLOCK_FILE || '../server/lastBlockByChain.json';

// without this, axios request will error `Z_BUF_ERROR`: https://github.com/axios/axios/issues/5346
export const AXIOS_CONFIG_JSON: AxiosRequestConfig = {
  headers: { 'Accept-Encoding': 'application/json' },
};
