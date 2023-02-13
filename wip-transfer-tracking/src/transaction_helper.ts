import {
  getIsTransferCompletedAlgorand,
  getIsTransferCompletedAptos,
  getIsTransferCompletedEth,
  getIsTransferCompletedNear,
  getIsTransferCompletedSolana,
  getIsTransferCompletedTerra,
  getIsTransferCompletedTerra2,
} from '@certusone/wormhole-sdk';
import { Connection } from '@solana/web3.js';
import { LCDClient } from '@terra-money/terra.js';
import algosdk from 'algosdk';
import { ethers } from 'ethers';

import { AptosClient } from 'aptos';

const near = require('near-api-js');

import { providers as nearProviders } from 'near-api-js';

require('dotenv').config();

const SOLANA_RPC = process.env.SOLANA_RPC;

export const WORMHOLE_RPC_HOSTS = [
  'https://wormhole-v2-mainnet-api.certus.one',
  'https://wormhole.inotel.ro',
  'https://wormhole-v2-mainnet-api.mcf.rocks',
  'https://wormhole-v2-mainnet-api.chainlayer.network',
  'https://wormhole-v2-mainnet-api.staking.fund',
  'https://wormhole-v2-mainnet.01node.com',
];

export async function sleep(timeout: number) {
  return new Promise((resolve) => setTimeout(resolve, timeout));
}

export async function checkRedeemed(chainInfo, signedVaa) {
  if (chainInfo.evm == true) {
    const evm_token_bridge_addresss = chainInfo.token_bridge_address;
    const provider = new ethers.providers.JsonRpcProvider(chainInfo.endpoint_url);
    return await getIsTransferCompletedEth(evm_token_bridge_addresss, provider, signedVaa);
  } else if (chainInfo.chain_id == 1) {
    //solana
    const sol_token_bridge_addresss = chainInfo.token_bridge_address;
    const connection = new Connection(SOLANA_RPC);
    await sleep(200);
    return await getIsTransferCompletedSolana(sol_token_bridge_addresss, signedVaa, connection);
  } else if (chainInfo.chain_id == 8) {
    const ALGORAND_HOST = {
      algodToken: '',
      algodServer: 'https://mainnet-idx.algonode.cloud',
      algodPort: '',
    };
    const provider = new algosdk.Algodv2(
      ALGORAND_HOST.algodToken,
      ALGORAND_HOST.algodServer,
      ALGORAND_HOST.algodPort
    );
    return await getIsTransferCompletedAlgorand(
      provider,
      BigInt(chainInfo['token_bridge_address']),
      signedVaa
    );
  } else if (chainInfo.chain_id == 3) {
    //terra classic
    const terra_token_bridge_addresss = chainInfo.token_bridge_address;
    const TERRA_HOST = {
      URL: 'https://columbus-lcd.terra.dev',
      chainID: 'columbus-5',
      name: 'mainnet',
      isClassic: true,
    };
    const lcd = new LCDClient(TERRA_HOST);
    const TERRA_GAS_PRICES_URL = 'https://fcd.terra.dev/v1/txs/gas_prices';
    return await getIsTransferCompletedTerra(
      terra_token_bridge_addresss,
      signedVaa,
      lcd,
      TERRA_GAS_PRICES_URL
    );
  } else if (chainInfo.chain_id == 18) {
    //terra 2
    const terra_token_bridge_addresss = chainInfo.token_bridge_address;
    const TERRA_HOST = {
      URL: 'https://phoenix-lcd.terra.dev',
      chainID: 'phoenix-1',
      name: 'mainnet',
    };

    const lcd = new LCDClient(TERRA_HOST);
    return await getIsTransferCompletedTerra2(terra_token_bridge_addresss, signedVaa, lcd);
  } else if (chainInfo.chain_id == 15) {
    const near_bridge = chainInfo.token_bridge_address;
    const nearProvider = new nearProviders.JsonRpcProvider(chainInfo.endpoint_url);
    return await getIsTransferCompletedNear(nearProvider, near_bridge, signedVaa);
  } else if (chainInfo.chain_id == 22) {
    const client = await new AptosClient('https://mainnet.aptoslabs.com');

    return await getIsTransferCompletedAptos(client, chainInfo['token_bridge_address'], signedVaa);
  } else {
    console.log(`do not recognize chain=${chainInfo.name} for redeem`);
    return false;
  }
}
