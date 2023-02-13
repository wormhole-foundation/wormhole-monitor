import {
  CHAIN_ID_SOLANA,
  hexToUint8Array,
  parseTransferPayload,
  tryHexToNativeString,
  uint8ArrayToHex,
  getSignedVAAWithRetry,
  parseSequencesFromLogEth,
  getEmitterAddressEth,
  parseSequencesFromLogTerra,
  getEmitterAddressTerra,
  parseSequencesFromLogSolana,
  getEmitterAddressSolana,
  getEmitterAddressAlgorand,
  CHAIN_ID_TERRA2,
  CHAIN_ID_NEAR,
  tryHexToNativeAssetString,
  parseVaa,
} from '@certusone/wormhole-sdk';
import { Connection, PublicKey } from '@solana/web3.js';
import {
  CHAIN_INFO_MAP,
  checkRedeemed,
  findMetadata,
  getOriginalAsset,
  NATIVE_TERRA2,
  queryExternalId,
  SOLANA_RPC,
} from './transaction_helper';
import { toUint8Array } from 'js-base64';
import { NodeHttpTransport } from '@improbable-eng/grpc-web-node-http-transport';
import { ethers } from 'ethers';
import { WORMHOLE_RPC_HOSTS } from './transaction_helper';
import {
  connect as nearConnect,
  Account as nearAccount,
  providers as nearProviders,
} from 'near-api-js';

import { AptosClient } from 'aptos';
import { BridgeImplementation__factory } from '@certusone/wormhole-sdk/lib/cjs/ethers-contracts';

const interf = BridgeImplementation__factory.createInterface(); //copy the interface that the bridge uses

/*
pub struct MessageData {
    /// Header of the posted VAA
    pub vaa_version: u8,

    /// Level of consistency requested by the emitter
    pub consistency_level: u8,

    /// Time the vaa was submitted
    pub vaa_time: u32,

    /// Account where signatures are stored
    pub vaa_signature_account: Pubkey,

    /// Time the posted message was created
    pub submission_time: u32,

    /// Unique nonce for this message
    pub nonce: u32,

    /// Sequence number of this message
    pub sequence: u64,

    /// Emitter of the message
    pub emitter_chain: u16,

    /// Emitter of the message
    pub emitter_address: [u8; 32],

    /// Message payload
    pub payload: Vec<u8>,
}
*/

export async function checkPayloadType(vaa) {
  const signedVaaBytes = vaa.vaaBytes;

  // parse the payload of the VAA
  let parsedVAA = parseVaa(signedVaaBytes);
  let payload = Buffer.from(parsedVAA.payload);
  // payloads that start with 1 or 3 are token transfers
  if (payload[0] == 1 || payload[0] == 3) {
    return 1;
  } else {
    return 0;
  }
}

function parsePostedMessage(data_: string) {
  //find emitter address, emitter chain, & seqnum

  if (data_.length < 182) {
    console.log('no enough data length', data_.length);
    return {
      payload: '',
      emitter_chain: 0,
      emitter_address: '',
      sequence: null,
    };
  }
  let data = data_.slice(6);
  let sequence_ = data.slice(92, 108); //little endian
  let emitter_chain_ = data.slice(108, 112); //little endian
  let emitter_address = data.slice(112, 176);
  let payload = data.slice(176);

  // convert emitter_chain adn sequence from lil endien to big
  let sequence_be = hexToUint8Array(sequence_).reverse();
  let emitter_chain_be = hexToUint8Array(emitter_chain_).reverse();

  let sequence = parseInt(uint8ArrayToHex(sequence_be), 16);
  let emitter_chain = parseInt(uint8ArrayToHex(emitter_chain_be), 16);
  let parsedMessage = {
    payload: payload,
    emitter_chain: emitter_chain,
    emitter_address: emitter_address,
    sequence: sequence,
  };
  return parsedMessage;
}

export function checkPostVaaAndMint(logMsgs) {
  var logMsg = logMsgs.join();
  if (logMsg.includes('MintTo')) {
    return 1;
  } else if (logMsg.includes('Transfer')) {
    return 2;
  } else {
    return 0;
  }
  return 0;
}

export async function processTransferSolana(seq, vaa, txn, emitter_address, chainInfo) {
  var results = [];

  var sourceAddress = txn.transaction.message.accountKeys[0].pubkey.toString();
  // iterate through all sequences in tx log
  var originAddress = undefined;
  var originChain = undefined;
  var tokenAmount = undefined;
  var tokenDecimal = undefined;
  var targetChain = undefined;
  var targetAddress = undefined;
  var fee = undefined;
  var redeemed = undefined;
  var vaaHex = undefined;
  var owner = undefined;

  if (vaa !== undefined) {
    const signedVaa = vaa.vaaBytes;
    vaaHex = uint8ArrayToHex(signedVaa);

    // parse the payload of the VAA
    let parsedVAA = parseVaa(signedVaa);
    // convert the Uint8ByteAarray into a buffer?  ¯\_(ツ)_/¯
    let payload = Buffer.from(parsedVAA.payload);
    // payloads that start with 1 are token transfers
    if (payload[0] == 1 || payload[0] == 3) {
      // parse the payload
      let parsedPayload = parseTransferPayload(payload);
      try {
        targetAddress = tryHexToNativeString(
          parsedPayload.targetAddress,
          CHAIN_INFO_MAP[parsedPayload.targetChain].chain_id
        );
      } catch (e) {
        targetAddress = parsedPayload.targetAddress;
      }
      targetChain = parsedPayload.targetChain;
      originChain = parsedPayload.originChain;

      try {
        originAddress = await getOriginalAsset(parsedPayload.originAddress, originChain);
      } catch (e) {
        console.log('cannot native string origin address for chain=', originChain);
      }
      tokenAmount = parsedPayload.amount.toString();
      fee = parsedPayload.fee === undefined ? 0 : parsedPayload.fee.toString();
      if (payload[0] == 3) {
        fee = 0; // transferWithPayload doesn't have fees?
      }
      owner = targetAddress;
      tokenDecimal = 0; //default for wormhole
      try {
        var meta_data = await findMetadata(originChain, originAddress);
        tokenDecimal = Math.min(meta_data.decimals, 8);
      } catch (e) {
        console.log(`could not find meta_data for ${originChain}, ${originAddress}`);
      }

      if (isNaN(tokenDecimal) || tokenDecimal == undefined || tokenDecimal == null) {
        tokenDecimal = -1;
      }

      redeemed = false;
      try {
        redeemed = await checkRedeemed(CHAIN_INFO_MAP[targetChain], signedVaa);
      } catch (e) {
        console.log('could not checkRedeem', txn['txHash'], seq, targetChain, targetAddress);
      }
      if (originAddress.length > 150) {
        // handling aptos fully qualified addresses?
        console.log(`originAddress=${originAddress} is too long. switching to payload address`);
        originAddress = parsedPayload.originAddress;
      }
    } else {
      // not a token transfer, skip
      return results;
    }
  }

  let source_timestamp = txn['blockTime'];
  let source_time = new Date(source_timestamp * 1000);
  let date = source_time.toISOString().slice(0, 10);
  var result = {
    date: date,
    source_time: source_time,
    source_timestamp: source_timestamp,
    source_hash: txn['txHash'],
    chain_id: chainInfo.chain_id,
    emitter_address: tryHexToNativeString(emitter_address, CHAIN_ID_SOLANA),
    seq: seq,
    source_address: sourceAddress,
    source_block: txn['slot'],
    token_address: originAddress,
    token_chain_id: originChain,
    token_amount: tokenAmount, //to facilitate stringifying bigInt
    token_decimal: tokenDecimal, // need to update this
    signed_vaa: vaaHex,
    target_chain_id: targetChain,
    target_address: owner,
    fee: fee,
    is_redeemed: redeemed,
  };
  results.push(result);
  return results;
}

export async function processCompleteTransferSolana(txn) {
  const connection = new Connection(SOLANA_RPC);
  var completeTransResult = {};
  var message_type = 0;

  var redeem_wallet = txn.transaction.message.accountKeys[0].pubkey.toString();
  if (txn.hasOwnProperty('meta')) {
    if (txn['meta'].hasOwnProperty('logMessages')) {
      message_type = checkPostVaaAndMint(txn?.meta?.logMessages);
      var instructions = txn?.transaction?.message?.instructions;

      if (instructions != undefined && instructions.length > 0) {
        var accounts = [];
        const redeem_instruction = instructions.filter(
          (ix) => ix?.data == '3' || ix?.data == '4' || ix?.data == '10' || ix?.data == '11'
        );
        if (redeem_instruction.length > 0) {
          accounts = redeem_instruction[0]?.accounts;
        } else if (instructions.length > 0) {
          // redeem might not be first instruction
          if (txn?.meta?.innerInstructions.length > 0) {
            const inner_ixs = txn?.meta?.innerInstructions[0]?.instructions;
            inner_ixs.forEach((ix) => {
              //completeNativeWithPayload & completeWrappedWithPayload
              if (
                (ix?.data == '3' || ix?.data == '4' || ix?.data == 'A' || ix?.data == 'B') &&
                ix.hasOwnProperty('accounts')
              ) {
                accounts = ix?.accounts;
              }
            });
          }
        } else {
          console.log('cannot find redeem accounts');
        }
        if (accounts != undefined && accounts.length > 3) {
          const vaa_account = accounts[2];
          let vaaAccountInfo = await connection.getAccountInfo(vaa_account);
          let vaaAccountData = vaaAccountInfo?.data;

          if (vaaAccountData != undefined) {
            let parsedMessage = parsePostedMessage(uint8ArrayToHex(vaaAccountData));
            if (parsedMessage.sequence == null) {
              return completeTransResult;
            } else if (parsedMessage.sequence > 1_000_000_000) {
              console.log('seq overflow', parsedMessage, txn['txHash']);
              return completeTransResult;
            }

            let emitter_chain_id = CHAIN_INFO_MAP[parsedMessage.emitter_chain].chain_id;
            if (emitter_chain_id == undefined) {
              console.log(
                'unknown emitter chain_id=',
                emitter_chain_id,
                parsedMessage.emitter_chain
              );
              return completeTransResult;
            } else {
              var emitter_address = '';
              try {
                emitter_address = tryHexToNativeString(
                  parsedMessage.emitter_address,
                  emitter_chain_id
                );
              } catch (e) {
                emitter_address = parsedMessage.emitter_address;
              }

              completeTransResult = {
                emitter_address: emitter_address,
                chain_id: parsedMessage.emitter_chain,
                seq: parsedMessage.sequence,
                is_redeemed: 1,
                redeem_hash: txn['txHash'],
                redeem_timestamp: txn['blockTime'],
                redeem_time: new Date(txn['blockTime'] * 1000),
                redeem_block: txn['slot'],
                redeem_wallet: redeem_wallet,
              };
              return completeTransResult;
            }
          }
        } else {
          return completeTransResult;
        }
      }
    }
  } else {
    return completeTransResult;
  }

  return completeTransResult;
}

export async function runSolana(transactionArray_, chainInfo) {
  var transferResults = [];
  var completeTransResults = [];
  var errTransactions = [];
  /* catch errors */
  var errTransaction = {};

  const emitter_address = await getEmitterAddressSolana(chainInfo.token_bridge_address);
  var transactionArray = transactionArray_.reverse(); // go from descending to ascending

  const numTransactions = transactionArray.length;

  var i = 0;

  while (i < numTransactions) {
    let txn = transactionArray[i];
    // get the transaction response using the solana rpc node
    // grab sequence number from the transaction_response
    console.log(i, txn['txHash']);

    if (txn?.meta?.err != null) {
      console.log('err transaction=', txn['txHash']);
      i++;
      continue;
    }

    var sequences = [];
    try {
      sequences = parseSequencesFromLogSolana(txn);
    } catch (e) {
      console.log(`cannot parse sequence for: ${txn['txHash']}`);
      errTransaction = {
        txId: txn['txHash'],
        timestamp: txn['blockTime'],
        reason: 'no seqnum',
        emitterChain: chainInfo.chain_id,
        seqnum: null,
      };
      errTransactions.push(errTransaction);
      i++;
      continue;
    }

    if (sequences.length == 0) {
      var completeTransferResult = undefined;
      try {
        completeTransferResult = await processCompleteTransferSolana(txn);
      } catch (e) {
        console.log('could not process complete trasnfer=', txn['txHash']);
      }

      if (
        completeTransferResult != undefined &&
        completeTransferResult.hasOwnProperty('redeem_hash')
      ) {
        completeTransferResult = {
          ...completeTransferResult,
          target_chain_id: chainInfo.chain_id,
        };
        console.log(
          `redeem: chain=${completeTransferResult['chain_id']}, 
                    seq=${completeTransferResult['seq']}, hash=${txn['txHash']}, ${i} out of ${numTransactions}`
        );
        completeTransResults.push(completeTransferResult);
      } else {
        i++;
        continue;
      }
    } else {
      for (let j = 0; j < sequences.length; j++) {
        var seq = sequences[j];
        var vaa = undefined;
        try {
          vaa = await getSignedVAAWithRetry(
            WORMHOLE_RPC_HOSTS,
            chainInfo.chain_id,
            emitter_address,
            seq,
            { transport: NodeHttpTransport() },
            1000,
            4
          );
        } catch (e) {
          console.log(`cannot find signed VAA for: ${txn['txHash']}`);
        }
        var transferResult = undefined;
        try {
          transferResult = await processTransferSolana(seq, vaa, txn, emitter_address, chainInfo);
          if (transferResult != undefined && transferResult.length > 0) {
            console.log(
              `transfer: seq=${seq}, target_chain=${transferResult[0]['target_chain_id']}, 
                            hash=${txn['txHash']}, ${i} out of ${numTransactions}`
            );
            transferResults = transferResults.concat(transferResult);
          }
        } catch (e) {
          console.log(e);
          console.log(`cannot processTransfer: ${txn['txHash']}`);
          errTransaction = {
            txId: txn['txHash'],
            timestamp: txn['blockTime'],
            reason: 'cannot parse',
            emitterChain: chainInfo.chain_id,
            seqnum: seq,
          };
          errTransactions.push(errTransaction);
        }
      }
    }
    i++;
  }
  console.log(`# of token transfers from ${chainInfo.name}: ${transferResults.length}`);
  console.log(`# of redeems on ${chainInfo.name}: ${completeTransResults.length}`);
  console.log(`# of errs on ${chainInfo.name}: ${errTransactions.length}`);

  return [transferResults, completeTransResults];
}

export async function processTransferTerra(seq, vaa, txn, emitter_address, chainInfo) {
  var results = [];

  var sourceAddress = '';
  if (txn.hasOwnProperty('tx.value.msg')) {
    try {
      sourceAddress = txn['tx.value.msg'][0]['value']['sender'];
    } catch (e) {
      console.log(txn);
      sourceAddress = '';
    }
  } else if (txn.hasOwnProperty('tx')) {
    if (txn['tx'].hasOwnProperty('body')) {
      if (txn['tx']['body'].hasOwnProperty('messages')) {
        const transfer_message = txn['tx']['body']['messages'].filter((msg) =>
          msg.msg.hasOwnProperty('initiate_transfer')
        );
        if (transfer_message.length > 0) {
          sourceAddress = transfer_message[0].sender;
        } else {
          sourceAddress = '';
        }
      } else {
        sourceAddress = '';
      }
    } else {
      try {
        sourceAddress = txn['tx']['value']['msg'][0]['value']['sender'];
      } catch (e) {
        sourceAddress = '';
      }
    }
  } else {
    try {
      if (txn.hasOwnProperty('raw_log')) {
        var parsedRawLog = JSON.parse(txn.raw_log);
        if (parsedRawLog.length > 0) {
          if (parsedRawLog[0].hasOwnProperty('events')) {
            if (
              parsedRawLog[0]['events'].length > 2 &&
              parsedRawLog[0].events[2]['attributes'].length > 0
            ) {
              sourceAddress = parsedRawLog[0].events[2]['attributes'][0]['value'];
            }
          }
        }
      }
    } catch (e) {
      console.log('cannot extract source address');
    }
  }

  // attempt to extract sender address. hack central
  var originAddress = undefined;
  var originChain = undefined;
  var tokenAmount = undefined;
  var tokenDecimal = undefined;
  var targetChain = undefined;
  var targetAddress = undefined;
  var fee = undefined;
  var redeemed = undefined;
  var vaaHex = undefined;
  var owner = undefined;

  if (vaa !== undefined) {
    const signedVaa = vaa.vaaBytes;
    vaaHex = uint8ArrayToHex(signedVaa);

    // parse the payload of the VAA
    let parsedVAA = parseVaa(signedVaa);

    // convert the Uint8ByteAarray into a buffer?  ¯\_(ツ)_/¯
    let payload = Buffer.from(parsedVAA.payload);
    // payloads that start with 1 are token transfers
    if (payload[0] == 1 || payload[0] == 3) {
      // parse the payload
      let parsedPayload = parseTransferPayload(payload);
      try {
        targetAddress = tryHexToNativeString(
          parsedPayload.targetAddress,
          CHAIN_INFO_MAP[parsedPayload.targetChain].chain_id
        );
      } catch (e) {
        targetAddress = parsedPayload.targetAddress;
      }

      targetChain = parsedPayload.targetChain;
      originChain = parsedPayload.originChain;

      try {
        originAddress = await getOriginalAsset(parsedPayload.originAddress, originChain);
      } catch (e) {
        console.log('cannot native string origin address for chain=', originChain);
      }
      tokenAmount = parsedPayload.amount.toString();
      fee = parsedPayload.fee.toString();
      if (payload[0] == 3) {
        fee = 0; // transferWithPayload doesn't have fees?
      }
      owner = targetAddress;
      if (targetChain == CHAIN_ID_SOLANA) {
        //find owner of token account
        const connection = new Connection(SOLANA_RPC);
        const parsed_account_info = await connection.getParsedAccountInfo(
          new PublicKey(targetAddress)
        );

        try {
          if (parsed_account_info.hasOwnProperty('value')) {
            if (parsed_account_info['value'].hasOwnProperty('data')) {
              const parsed = parsed_account_info.value.data;
              owner = parsed['parsed'].info.owner;
            }
          }
        } catch (e) {
          console.log('could not find owner');
        }
      }

      redeemed = false;

      try {
        redeemed = await checkRedeemed(CHAIN_INFO_MAP[targetChain], signedVaa);
      } catch (e) {
        console.log('could not cehck redeem', txn['txhash'], seq, targetChain, targetAddress);
      }
      var tokenName = '';
      var tokenSymbol = '';
      tokenDecimal = 0;
      try {
        var meta_data = await findMetadata(originChain, originAddress);
        tokenName = meta_data.tokenName;
        tokenSymbol = meta_data.symbol;
        tokenDecimal = Math.min(meta_data.decimals, 8);
      } catch (e) {
        console.log(`could not find meta_data for ${originChain}, ${originAddress}`);
      }
      if (isNaN(tokenDecimal) || tokenDecimal == undefined || tokenDecimal == null) {
        tokenDecimal = -1;
      }
      if (originAddress.length > 150) {
        console.log(`originAddress=${originAddress} is too long. switching to payload address`);
        originAddress = parsedPayload.originAddress;
      }
    } else {
      // not a token transfer, skip
      return results;
    }
  }

  let source_timestamp = new Date(txn['timestamp']).getTime() / 1000;
  let source_time = new Date(source_timestamp * 1000);
  let date = source_time.toISOString().slice(0, 10);
  var result = {
    date: date,
    source_time: source_time,
    source_timestamp: source_timestamp,
    source_hash: txn['txhash'],
    chain_id: chainInfo.chain_id,
    emitter_address: tryHexToNativeString(emitter_address, chainInfo.chain_id),
    seq: seq,
    source_address: sourceAddress,
    source_block: txn['height'],
    token_address: originAddress,
    token_chain_id: originChain,
    token_amount: tokenAmount, //to facilitate stringifying bigInt
    token_decimal: tokenDecimal, // need to update this
    signed_vaa: vaaHex,
    target_chain_id: targetChain,
    target_address: owner,
    fee: fee,
    is_redeemed: redeemed,
  };
  results.push(result);
  return results;
}

export async function processCompleteTransferTerra(txn) {
  var redemptionTxId = txn['txhash'];
  try {
    var raw_log = txn['raw_log'];

    if (raw_log.search('complete_transfer') == -1) {
      return {};
    } else {
      //there has to be a better way to grab the vaa
      var vaa = '';
      try {
        if (txn.hasOwnProperty('tx.value.msg')) {
          var parsedLog = txn['tx.value.msg'];
          if (parsedLog.length > 0 && parsedLog[0].hasOwnProperty('value')) {
            var parsedLogValue = parsedLog[0]['value'];
            if (parsedLogValue.hasOwnProperty('execute_msg')) {
              var execute_msg = parsedLogValue['execute_msg'];
              if (execute_msg.hasOwnProperty('submit_vaa')) {
                vaa = execute_msg['submit_vaa']['data'];
              } else if (execute_msg.hasOwnProperty('process_anchor_message')) {
                vaa = execute_msg['process_anchor_message']['option_token_transfer_vaa'];
              } else {
                console.log('cannot process complete transfer', redemptionTxId);
              }
            }
          }
        } else if (txn.hasOwnProperty('tx')) {
          if (txn['tx'].hasOwnProperty('value')) {
            try {
              var parsedLog = txn['tx']['value']['msg'];
              if (parsedLog.length > 0 && parsedLog[0].hasOwnProperty('value')) {
                var parsedLogValue = parsedLog[0]['value'];
                if (parsedLogValue.hasOwnProperty('execute_msg')) {
                  if (parsedLogValue['execute_msg'].hasOwnProperty('submit_vaa')) {
                    vaa = parsedLogValue['execute_msg']['submit_vaa']['data'];
                  } else if (
                    parsedLogValue['execute_msg'].hasOwnProperty('process_anchor_message')
                  ) {
                    // anchor protocol specific
                    vaa =
                      parsedLogValue['execute_msg']['process_anchor_message'][
                        'option_token_transfer_vaa'
                      ];
                  }
                }
              }
            } catch (e) {
              console.log(e);
            }
          } else if (txn['tx'].hasOwnProperty('body')) {
            if (txn['tx']['body'].hasOwnProperty('messages')) {
              txn.tx.body.messages.forEach((x) => {
                if (x['msg'].hasOwnProperty('submit_vaa')) {
                  vaa = x['msg']['submit_vaa']['data'];
                }
              });
            }
          }
        } else {
          console.log('cannot extract vaa', redemptionTxId);
        }
      } catch (e) {
        console.log('cannot extract vaa2', redemptionTxId);
      }

      var signedVaa = toUint8Array(vaa);

      let parsedVaa = parseVaa(signedVaa);
      var targetEmitterChain = parsedVaa['emitter_chain'];
      var targetEmitterAddress = parsedVaa['emitter_address'];

      var logs = txn['logs'];
      var redeem_wallet = '';
      logs.forEach((log) => {
        if (log.hasOwnProperty('events')) {
          log['events'].forEach((event) => {
            if (event.type == 'from_contract') {
              event['attributes'].forEach((attribute) => {
                if (attribute.key == 'recipient') {
                  redeem_wallet = attribute['value'];
                }
              });
            }
          });
        }
      });
      let emitter_chain_id = CHAIN_INFO_MAP[targetEmitterChain].chain_id;
      if (emitter_chain_id == undefined) {
        console.log('unknown emitter chain_id=', emitter_chain_id, targetEmitterChain);
        return completeTransResult;
      }
      var emitter_address = '';
      try {
        emitter_address = tryHexToNativeString(targetEmitterAddress, emitter_chain_id);
      } catch (e) {
        emitter_address = uint8ArrayToHex(targetEmitterAddress);
      }
      var completeTransResult = {
        emitter_address: emitter_address,
        chain_id: targetEmitterChain,
        seq: parsedVaa.sequence,
        is_redeemed: 1,
        redeem_hash: redemptionTxId, //txn['hash'],<-- could just be txn['hash']
        redeem_timestamp: new Date(txn['timestamp']).getTime() / 1000,
        redeem_time: new Date(txn['timestamp']),
        redeem_block: txn['height'],
        redeem_wallet: redeem_wallet,
      };

      return completeTransResult;
    }
  } catch (e) {
    return {};
  }
}

export async function runTerra(transactionArray_, chainInfo) {
  const emitter_address = await getEmitterAddressTerra(chainInfo.token_bridge_address);
  var transactionArray = transactionArray_.reverse(); // go from descending to ascending
  const numTransactions = transactionArray.length;

  var transferResults = [];
  var completeTransResults = [];
  var errTransactions = [];
  /* catch errors */
  var errTransaction = {};

  var i = 0;

  while (i < numTransactions) {
    let txn = transactionArray[i];
    console.log(i, txn['txhash']);
    // grab sequence number from the transaction_response
    var sequences = [];
    try {
      var sequencesWithDuplicates = parseSequencesFromLogTerra(txn);
      sequences = sequencesWithDuplicates.filter(
        (n, i) => sequencesWithDuplicates.indexOf(n) === i
      );
    } catch (e) {
      console.log(`cannot parse sequence for: ${txn['txhash']}`);
      errTransaction = {
        txId: txn['txhash'],
        timestamp: txn['timestamp'],
        reason: 'no seqnum',
        emitterChain: chainInfo.chain_id,
        seqnum: null,
      };
      errTransactions.push(errTransaction);
      i++;
      continue;
    }
    // iterate through all sequences in tx log
    // if (sequences.length == 0){
    try {
      var completeTransferResult = undefined;
      try {
        completeTransferResult = await processCompleteTransferTerra(txn);
      } catch (e) {
        console.log('could not parse complete trasfners=', txn['txhash']);
      }
      if (
        completeTransferResult != undefined &&
        completeTransferResult.hasOwnProperty('redeem_hash')
      ) {
        completeTransferResult = {
          ...completeTransferResult,
          target_chain_id: chainInfo.chain_id,
        };
        console.log(
          `redeem: chain=${completeTransferResult['chain_id']}, 
                    seq=${completeTransferResult['seq']}, hash=${txn['txhash']}, ${i} out of ${numTransactions}`
        );

        completeTransResults.push(completeTransferResult);
      }
    } catch (e) {
      {
        console.log('could not parse redeem for terra');
      }
    }
    try {
      for (let j = 0; j < 1 /*sequences.length*/; j++) {
        var seq = sequences[j];
        var vaa = undefined;
        try {
          vaa = await getSignedVAAWithRetry(
            WORMHOLE_RPC_HOSTS,
            chainInfo.chain_id,
            emitter_address,
            seq,
            { transport: NodeHttpTransport() },
            1000,
            4
          );
        } catch (e) {
          console.log(`cannot find signed VAA for: seq=${seq}, ${txn['txhash']}`);
        }
        var transferResult = undefined;
        try {
          transferResult = await processTransferTerra(seq, vaa, txn, emitter_address, chainInfo);
          if (transferResult != undefined && transferResult.length > 0) {
            console.log(
              `transfer: seq=${seq}, target_chain=${transferResult[0]['target_chain_id']}, 
                            hash=${txn['txhash']}, ${i} out of ${numTransactions}`
            );
            transferResults = transferResults.concat(transferResult);
          }
        } catch (e) {
          console.log('cannot processTransfer', i, txn['txhash']);
          i++;
          continue;
        }
      }
    } catch (e) {
      console.log('could not parse transfer for terra');
    }
    i++;
  }

  console.log(`# of token transfers from terra: ${transferResults.length}`);
  console.log(`# of redeems on terra: ${completeTransResults.length}`);
  console.log(`# of errs on ${chainInfo.name}: ${errTransactions.length}`);

  return [transferResults, completeTransResults];
}

export async function processTransferEvm(seq, vaa, txn, emitter_address, chainInfo) {
  var results = [];
  // iterate through all sequences in tx log
  var originAddress = undefined;
  var originChain = undefined;
  var tokenAmount = undefined;
  var tokenDecimal = undefined;
  var targetChain = undefined;
  var targetAddress = undefined;
  var fee = undefined;
  var redeemed = undefined;
  var vaaHex = undefined;
  var owner = undefined;

  if (vaa !== undefined) {
    const signedVaa = vaa.vaaBytes;
    vaaHex = uint8ArrayToHex(signedVaa);
    // parse the payload of the VAA
    let parsedVAA = parseVaa(signedVaa);
    // convert the Uint8ByteAarray into a buffer?  ¯\_(ツ)_/¯
    let payload = Buffer.from(parsedVAA.payload);
    // payloads that start with 1 are token transfers
    if (payload[0] == 1 || payload[0] == 3) {
      // parse the payload

      let parsedPayload = parseTransferPayload(payload);
      try {
        targetAddress = tryHexToNativeString(
          parsedPayload.targetAddress,
          CHAIN_INFO_MAP[parsedPayload.targetChain].chain_id
        );
      } catch (e) {
        console.log('cannot native string target address');
        targetAddress = parsedPayload.targetAddress;
      }
      targetChain = parsedPayload.targetChain;
      originChain = parsedPayload.originChain;

      try {
        originAddress = await getOriginalAsset(parsedPayload.originAddress, originChain);
      } catch (e) {
        console.log('cannot native string origin address for chain=', originChain);
      }

      tokenAmount = parsedPayload.amount.toString();
      fee = parsedPayload?.fee ? parsedPayload?.fee.toString() : 0;
      owner = targetAddress;
      if (targetChain == CHAIN_ID_SOLANA) {
        //find owner of token account
        const connection = new Connection(SOLANA_RPC);
        const parsed_account_info = await connection.getParsedAccountInfo(
          new PublicKey(targetAddress)
        );
        try {
          if (parsed_account_info.hasOwnProperty('value')) {
            if (parsed_account_info['value'].hasOwnProperty('data')) {
              const parsed = parsed_account_info.value.data;
              owner = parsed['parsed'].info.owner;
            }
          }
        } catch (e) {
          console.log('could not find owner', txn['hash'], owner, new PublicKey(targetAddress));
        }
      }
      redeemed = false;
      try {
        redeemed = await checkRedeemed(CHAIN_INFO_MAP[targetChain], signedVaa);
      } catch {
        console.log(txn['hash'], 'could not check redeem');
      }
      tokenDecimal = 0; //default for wormhole
      try {
        var meta_data = await findMetadata(originChain, originAddress);
        tokenDecimal = Math.min(meta_data.decimals, 8);
      } catch (e) {
        console.log(`could not find meta_data for ${originChain}, ${originAddress}`);
      }

      if (isNaN(tokenDecimal) || tokenDecimal == undefined || tokenDecimal == null) {
        console.log('nan decimal for txn=', txn['hash'], originChain, originAddress);
        tokenDecimal = -1;
      }
      if (originAddress.length > 150) {
        console.log(`originAddress=${originAddress} is too long. switching to payload address`);
        originAddress = parsedPayload.originAddress;
      }
    } else {
      // not a token transfer, skip
      return results;
    }
  }

  let source_timestamp = txn['timeStamp'] || txn['createdAt'] || txn['round-time'];
  var source_time = undefined;
  var date = undefined;
  if (source_timestamp !== undefined) {
    source_time = new Date(parseInt(source_timestamp) * 1000);
    date = source_time.toISOString().slice(0, 10);
  } else {
    source_time = new Date(txn['block_signed_at']);
    source_timestamp = new Date(source_time).getTime() / 1000;
    date = new Date(source_time).toISOString().slice(0, 10);
  }
  var result = {
    date: date,
    source_time: source_time,
    source_timestamp: source_timestamp,
    source_hash: txn['hash'],
    chain_id: chainInfo.chain_id,
    emitter_address: tryHexToNativeString(emitter_address, chainInfo.chain_id) /*emitter_address*/,
    seq: seq,
    source_address: txn['from'] || txn['fromAddress'] || txn['from_address'] || txn['sender'],
    source_block: txn['blockNumber'] || txn['block_height'] || txn['confirmed-round'],
    token_address: originAddress,
    token_chain_id: originChain,
    token_amount: tokenAmount, //to facilitate stringifying bigInt
    token_decimal: tokenDecimal, // need to update this
    signed_vaa: vaaHex,
    target_chain_id: targetChain,
    target_address: owner,
    fee: fee,
    is_redeemed: redeemed,
  };
  results.push(result);

  return results;
}

export async function processCompleteTransferEvm(txn, provider) {
  var redeem_hash = txn['hash'];
  var completeTransResults = [];
  var completeTransResult = {};
  var from = txn['from'] || txn['fromAddress'] || txn['from_address'] || txn['sender'];
  // completeTransfer ==> redeems wrapped token
  // completeTransferAndUnwrapETH ==> redeems & unwraps the native token
  var data;
  try {
    if (
      (txn.data == undefined && txn.input == undefined) ||
      txn?.data == 'deprecated' ||
      txn?.input == 'deprecated' ||
      txn?.input == ''
    ) {
      console.log('data is deprecated. trying to repull txn');
      const depTxn = await provider.getTransaction(redeem_hash);
      from = depTxn['from'] || depTxn['fromAddress'];
      data = depTxn.data || depTxn.input;
    } else if ('input' in txn) {
      data = txn.input;
    } else {
      data = txn.data;
    }
  } catch (e) {
    console.log('cannot access data');
  }
  var encodedVms = [];
  try {
    let msgTypes = ['completeTransfer', 'completeTransferAndUnwrapETH'];
    for (let i = 0; i < msgTypes.length; i++) {
      let msgType = msgTypes[i];

      var encodedVm = '';
      try {
        var completeTransfer = interf.decodeFunctionData(msgType, data);
        encodedVm = completeTransfer?.encodedVm.slice(2);
        encodedVms.push(encodedVm);
      } catch (e) {
        console.log('could not parse msg type=', msgType);
      }
    }
  } catch (e) {}

  let headers = ['0100000001', '0100000002', '0100000003'];
  for (let i = 0; i < headers.length; i++) {
    let header = headers[i];

    let ixs = [];
    let done = 0;
    let start = 0;
    while (start < data.length && done == 0 && ixs.length < 2) {
      var vm_index = data.indexOf(header, start);
      if (vm_index == -1) {
        done = 1;
      } else {
        ixs.push(vm_index);
        start = vm_index + header.length;
      }
    }
    let j = 0;
    while (j < ixs.length) {
      let vaa_index = ixs[j];
      if (j == ixs.length - 1) {
        encodedVms.push(data.slice(vaa_index));
      } else if (ixs.length > j + 1) {
        encodedVms.push(data.slice(vaa_index, ixs[j + 1] - 1));
      }
      j++;
    }
  }

  for (let i = 0; i < encodedVms.length; i++) {
    encodedVm = encodedVms[i];
    //
    try {
      let signedVaa = hexToUint8Array(encodedVm);

      let parsedVaa = parseVaa(signedVaa);
      var targetEmitterChain = parsedVaa['emitter_chain'];
      var targetEmitterAddress = parsedVaa['emitter_address'];
      let payload = Buffer.from(parsedVaa.payload);
      if (payload[0] == 1 || payload[0] == 3) {
        // token bridge transfer
        let parsedPayload;
        try {
          parsedPayload = parseTransferPayload(payload);
        } catch (e) {
          console.log(e);
        }
        var redeem_timestamp = txn['timeStamp'] || txn['createdAt'];
        var redeem_time = undefined;
        if (redeem_timestamp != undefined) {
          redeem_time = new Date(parseInt(redeem_timestamp) * 1000);
        } else {
          //specifically for klaytn
          redeem_time = new Date(txn['block_signed_at']);
          redeem_timestamp = new Date(redeem_time).getTime() / 1000;
        }

        let emitter_chain_id = CHAIN_INFO_MAP[targetEmitterChain].chain_id;
        if (emitter_chain_id == undefined) {
          console.log('unknown emitter chain_id=', emitter_chain_id, targetEmitterChain);
          return [];
        }

        var emitter_address = '';
        try {
          emitter_address = tryHexToNativeString(targetEmitterAddress, emitter_chain_id);
        } catch (e) {
          emitter_address = uint8ArrayToHex(targetEmitterAddress);
        }

        completeTransResult = {
          emitter_address: emitter_address /*uint8ArrayToHex(targetEmitterAddress)*/,
          chain_id: targetEmitterChain,
          seq: parsedVaa.sequence,
          is_redeemed: 1,
          redeem_hash: redeem_hash, //txn['hash'],<-- could just be txn['hash']
          redeem_timestamp: redeem_timestamp,
          redeem_time: redeem_time /*.toISOString()*/,
          redeem_block: txn['blockNumber'] || txn['block_height'],
          redeem_wallet: from, //txn['from']
        };
        completeTransResults.push(completeTransResult);
      } else {
        continue;
      }
    } catch (e) {
      console.log(e);
      console.log('could not parse redeem=', redeem_hash);
    }
  }
  function onlyUnique(obj, index, arr, prop = 'hash') {
    return arr.map((obj) => obj[prop]).indexOf(obj[prop]) === index;
  }

  completeTransResults = completeTransResults.filter(onlyUnique);

  return completeTransResults;
}

export async function runEvm(transactionArray, chainInfo) {
  const provider = new ethers.providers.JsonRpcProvider(chainInfo.endpoint_url);
  const emitter_address = await getEmitterAddressEth(chainInfo.token_bridge_address);

  const numTransactions = transactionArray.length;
  var transferResults = [];
  var completeTransResults = [];
  /* catch errors */
  var errTransactions = [];
  var errTransaction = {};

  var i = 0;

  while (i < numTransactions) {
    let txn = transactionArray[i];
    console.log(i, txn['hash']);
    if (txn['hash'] === undefined) {
      console.log('skipping undefined hash');
      i++;
      continue;
    }

    // get the receipt from the infura txn
    var receipt;
    try {
      receipt = await provider.getTransactionReceipt(txn['hash']);
    } catch (e) {
      console.log(`cannot find receipt for: ${txn['hash']}`);
    }

    // skip reverted transactions
    if (receipt?.status != 1) {
      console.log('err transaction=', txn['hash'], `${i} out of ${numTransactions}`);
      i++;
      continue;
    }
    // get the sequence number from the receipt
    var sequences;
    var seq;
    try {
      sequences = parseSequencesFromLogEth(receipt, chainInfo.core_bridge);
    } catch (e) {
      console.log(`cannot parse sequence for: ${txn['hash']}, ${i} out of ${numTransactions}`);
      errTransaction = {
        txId: txn['hash'],
        timestamp: txn['timeStamp'],
        reason: 'no seqnum',
        emitterChain: chainInfo.chain_id,
        seqnum: null,
      };
      errTransactions.push(errTransaction);
      i++;
      continue;
    }
    // iterate through all sequences in tx log
    var completeTransferResults_ = undefined;
    if (sequences.length == 0) {
      try {
        completeTransferResults_ = await processCompleteTransferEvm(txn, provider);
      } catch (e) {
        console.log('could not process complete transfer for txn=', txn['hash']);
      }
      if (completeTransferResults_ != undefined) {
        completeTransferResults_.forEach((completeTransferResult) => {
          if (completeTransferResult.hasOwnProperty('redeem_hash')) {
            completeTransferResult = {
              ...completeTransferResult,
              target_chain_id: chainInfo.chain_id,
            };
            console.log(
              `redeem: chain=${completeTransferResult['chain_id']}, 
                            seq=${completeTransferResult['seq']}, hash=${txn['hash']}, ${i} out of ${numTransactions}`
            );

            completeTransResults.push(completeTransferResult);
          }
        });
      }
    } else {
      //check that there's a log msg associated with the token bridge and corebridge
      const logs = receipt.logs;
      const filtered = logs.filter((log) => log.address == chainInfo.core_bridge);
      var topics_array = [];
      filtered.forEach((log) =>
        log.topics.forEach((topic) => {
          if (topic.slice(2) == emitter_address) {
            topics_array.push(topic.slice(2));
          }
        })
      );
      if (topics_array.length == 0) {
        console.log(`cannot find log for token bridge and corebridge....${txn['hash']}`);
        i++;
        continue;
      }
      for (let j = 0; j < topics_array.length; j++) {
        seq = sequences[j];
        var vaa = undefined;
        try {
          vaa = await getSignedVAAWithRetry(
            WORMHOLE_RPC_HOSTS,
            chainInfo.chain_id,
            emitter_address,
            seq,
            { transport: NodeHttpTransport() },
            1000,
            4
          );
        } catch (e) {
          console.log('cannot find signed VAA');
        }
        var transferResult = undefined;
        try {
          transferResult = await processTransferEvm(seq, vaa, txn, emitter_address, chainInfo);
          if (transferResult != undefined && transferResult.length > 0) {
            console.log(
              `transfer: seq=${seq}, target_chain=${transferResult[0]['target_chain_id']}, 
                          hash=${txn['hash']}, ${i} out of ${numTransactions}`
            );

            transferResults = transferResults.concat(transferResult);
          }
        } catch (e) {
          console.log(`cannot parse transfer for: ${txn['hash']}`);
          errTransaction = {
            txId: txn['hash'],
            timestamp: txn['timeStamp'],
            reason: 'no vaa',
            emitterChain: chainInfo.chain_id,
            seqnum: seq,
          };
          errTransactions.push(errTransaction);
          i++;
          continue;
        }
      }
    }
    i++;
  }

  console.log(`# of token transfers from ${chainInfo.name}: ${transferResults.length}`);
  console.log(`# of redeems on ${chainInfo.name}: ${completeTransResults.length}`);
  console.log(`# of errs on ${chainInfo.name}: ${errTransactions.length}`);

  return [transferResults, completeTransResults];
}

export async function processCompleteTransferAlgorand(txn) {
  var redeem_hash = txn['hash'];
  var completeTransResults = [];
  var completeTransResult = {};
  var from = txn['sender'];
  // completeTransfer ==> redeems wrapped token
  // completeTransferAndUnwrapETH ==> redeems & unwraps the native token
  var data;
  try {
    if (txn['application-transaction'] != undefined) {
      if (
        txn['application-transaction']['application-args'] != undefined &&
        txn['application-transaction']['application-args'].length > 1
      ) {
        const data64 = txn['application-transaction']['application-args'][1];
        data = Buffer.from(data64, 'base64').toString('hex');
      }
    } else {
      console.log('cannot access data');
      return [];
    }
  } catch (e) {
    console.log('cannot access data');
  }

  var encodedVms = [];
  try {
    let msgTypes = ['completeTransfer', 'completeTransferAndUnwrapETH'];
    for (let i = 0; i < msgTypes.length; i++) {
      let msgType = msgTypes[i];

      var encodedVm = '';
      try {
        var completeTransfer = interf.decodeFunctionData(msgType, data);
        encodedVm = completeTransfer?.encodedVm.slice(2);
        encodedVms.push(encodedVm);
      } catch (e) {
        console.log('could not parse msg type=', msgType);
      }
    }
  } catch (e) {}

  let headers = ['0100000001', '0100000002', '0100000003'];
  for (let i = 0; i < headers.length; i++) {
    let header = headers[i];

    let ixs = [];
    let done = 0;
    let start = 0;
    while (start < data.length && done == 0 && ixs.length < 2) {
      var vm_index = data.indexOf(header, start);
      if (vm_index == -1) {
        done = 1;
      } else {
        ixs.push(vm_index);
        start = vm_index + header.length;
      }
    }
    let j = 0;
    while (j < ixs.length) {
      let vaa_index = ixs[j];
      if (j == ixs.length - 1) {
        encodedVms.push(data.slice(vaa_index));
      } else if (ixs.length > j + 1) {
        encodedVms.push(data.slice(vaa_index, ixs[j + 1] - 1));
      }
      j++;
    }
  }

  for (let i = 0; i < encodedVms.length; i++) {
    encodedVm = encodedVms[i];
    //
    try {
      let signedVaa = hexToUint8Array(encodedVm);

      let parsedVaa = parseVaa(signedVaa);
      var targetEmitterChain = parsedVaa['emitter_chain'];
      var targetEmitterAddress = parsedVaa['emitter_address'];
      let payload = Buffer.from(parsedVaa.payload);
      if (payload[0] == 1 || payload[0] == 3) {
        // token bridge transfer
        let parsedPayload;
        try {
          parsedPayload = parseTransferPayload(payload);
        } catch (e) {
          console.log(e);
        }
        var targetAddress = tryHexToNativeString(
          parsedPayload.targetAddress,
          CHAIN_INFO_MAP[parsedPayload.targetChain].chain_id
        );
        const originChain = parsedPayload.originChain;
        var originAddress = '';
        if (originChain == CHAIN_ID_TERRA2) {
          if (parsedPayload.originAddress === NATIVE_TERRA2) {
            originAddress = 'uluna';
          } else {
            originAddress = await queryExternalId(parsedPayload.originAddress);
          }
        } else {
          originAddress = tryHexToNativeAssetString(
            parsedPayload.originAddress,
            parsedPayload.originChain
          );
        }

        var redeem_timestamp = txn['timeStamp'] || txn['createdAt'] || txn['round-time'];
        var redeem_time = undefined;
        if (redeem_timestamp != undefined) {
          redeem_time = new Date(parseInt(redeem_timestamp) * 1000);
        } else {
          //specifically for klaytn
          redeem_time = new Date(txn['block_signed_at']);
          redeem_timestamp = new Date(redeem_time).getTime() / 1000;
        }

        let emitter_chain_id = CHAIN_INFO_MAP[targetEmitterChain].chain_id;
        if (emitter_chain_id == undefined) {
          console.log('unknown emitter chain_id=', emitter_chain_id, targetEmitterChain);
          return [];
        }
        var emitter_address = '';
        try {
          emitter_address = tryHexToNativeString(targetEmitterAddress, emitter_chain_id);
        } catch (e) {
          emitter_address = uint8ArrayToHex(targetEmitterAddress);
        }
        completeTransResult = {
          emitter_address: emitter_address,
          chain_id: targetEmitterChain,
          seq: parsedVaa.sequence,
          is_redeemed: 1,
          redeem_hash: redeem_hash, //txn['hash'],<-- could just be txn['hash']
          redeem_timestamp: redeem_timestamp,
          redeem_time: redeem_time /*.toISOString()*/,
          redeem_block: txn['blockNumber'] || txn['block_height'] || txn['confirmed-round'],
          redeem_wallet: from, //txn['from']
        };
        completeTransResults.push(completeTransResult);
      } else {
        continue;
      }
    } catch (e) {
      console.log('could not parse redeem=', redeem_hash);
    }
  }
  function onlyUnique(obj, index, arr, prop = 'hash') {
    return arr.map((obj) => obj[prop]).indexOf(obj[prop]) === index;
  }

  completeTransResults = completeTransResults.filter(onlyUnique);

  return completeTransResults;
}

export const ALGORAND_HOST = {
  algodToken: '',
  algodServer: 'https://mainnet-idx.algonode.cloud',
  algodPort: '',
};

export async function runAlgorand(transactionArray, chainInfo) {
  const emitter_address = getEmitterAddressAlgorand(BigInt(chainInfo.token_bridge_address));
  const numTransactions = transactionArray.length;
  var transferResults = [];
  var completeTransResults = [];
  /* catch errors */
  var errTransactions = [];
  var errTransaction = {};

  var i = 0;

  while (i < numTransactions) {
    let txn = transactionArray[i];
    console.log(i, txn['hash']);
    if (txn['hash'] === undefined) {
      console.log('skipping undefined hash');
      i++;
      continue;
    }
    var seq = undefined;
    try {
      //parseSequenceFromLogAlgorand(txn); // returns "" or errors out when trying to convert base64 to bignumber
      if (txn?.['inner-txns'] != undefined) {
        const logs = txn['inner-txns'][0]?.logs;
        if (logs != undefined) {
          const seq64 = txn?.['inner-txns'][0]?.logs[0];
          const seq16 = Buffer.from(seq64, 'base64').toString('hex');
          seq = parseInt(seq16, 16);
        }
      }
    } catch (e) {
      console.log(e);
    }
    var completeTransferResults_ = undefined;
    if (seq == undefined) {
      try {
        completeTransferResults_ = await processCompleteTransferAlgorand(txn);
      } catch (e) {
        console.log('could not process complete transfer for txn=', txn['hash']);
      }
      if (completeTransferResults_ != undefined) {
        completeTransferResults_.forEach((completeTransferResult) => {
          if (completeTransferResult.hasOwnProperty('redeem_hash')) {
            completeTransferResult = {
              ...completeTransferResult,
              target_chain_id: chainInfo.chain_id,
            };
            console.log(
              `redeem: chain=${completeTransferResult['chain_id']}, 
                            seq=${completeTransferResult['seq']}, hash=${txn['hash']}, ${i} out of ${numTransactions}`
            );

            completeTransResults.push(completeTransferResult);
          }
        });
      }
    } else {
      var vaa = undefined;
      try {
        vaa = await getSignedVAAWithRetry(
          WORMHOLE_RPC_HOSTS,
          chainInfo.chain_id,
          emitter_address,
          seq,
          { transport: NodeHttpTransport() },
          1000,
          4
        );
      } catch (e) {
        console.log('cannot find signed VAA');
      }

      var transferResult = undefined;
      try {
        transferResult = await processTransferEvm(seq, vaa, txn, emitter_address, chainInfo);
        if (transferResult != undefined && transferResult.length > 0) {
          console.log(
            `transfer: seq=${seq}, target_chain=${transferResult[0]['target_chain_id']}, 
                        hash=${txn['hash']}, ${i} out of ${numTransactions}`
          );

          transferResults = transferResults.concat(transferResult);
        }
      } catch (e) {
        console.log(`cannot parse transfer for: ${txn['hash']}`);
        errTransaction = {
          txId: txn['hash'],
          timestamp: txn['timeStamp'],
          reason: 'no vaa',
          emitterChain: chainInfo.chain_id,
          seqnum: seq,
        };
        errTransactions.push(errTransaction);
        i++;
        continue;
      }
    }
    i++;
  }

  console.log(`# of token transfers from ${chainInfo.name}: ${transferResults.length}`);
  console.log(`# of redeems on ${chainInfo.name}: ${completeTransResults.length}`);
  console.log(`# of errs on ${chainInfo.name}: ${errTransactions.length}`);

  return [transferResults, completeTransResults];
}

async function findNearMetadata(txn) {
  const token = txn?.transaction?.receiver_id || undefined;
  if (token === undefined) {
    return {};
  }

  let near = await nearConnect({
    headers: {},
    networkId: 'mainnet',
    nodeUrl: process.env.NEAR_RPC,
  });

  const userAccount = new nearAccount(near.connection, 'foo');
  if (token === 'contract.portalbridge.near') {
    //transfering native near
    return {
      decimals: 24,
      name: 'NEAR',
      symbol: 'NEAR',
    };
  } else {
    const meta_data = await userAccount.viewFunction({
      contractId: token,
      methodName: 'ft_metadata',
      args: {},
    });
    return meta_data;
  }
}

export async function processTransferNear(seq, vaa, txn, emitter_address, chainInfo) {
  var results = [];
  // iterate through all sequences in tx log
  var originAddress = undefined;
  var originChain = undefined;
  var tokenAmount = undefined;
  var tokenDecimal = undefined;
  var targetChain = undefined;
  var targetAddress = undefined;
  var fee = undefined;
  var redeemed = undefined;
  var vaaHex = undefined;
  var owner = undefined;

  if (vaa !== undefined) {
    const signedVaa = vaa.vaaBytes;
    vaaHex = uint8ArrayToHex(signedVaa);

    // parse the payload of the VAA
    let parsedVAA = parseVaa(signedVaa);
    let payload = Buffer.from(parsedVAA.payload);
    // payloads that start with 1 are token transfers
    if (payload[0] == 1 || payload[0] == 3) {
      // parse the payload

      let parsedPayload = parseTransferPayload(payload);
      try {
        targetAddress = tryHexToNativeString(
          parsedPayload.targetAddress,
          CHAIN_INFO_MAP[parsedPayload.targetChain].chain_id
        );
      } catch (e) {
        console.log(
          'cannot hex to native string target address',
          parsedPayload.targetAddress,
          parsedPayload.targetChain
        );
        targetAddress = parsedPayload.targetAddress;
      }
      targetChain = parsedPayload.targetChain;
      originChain = parsedPayload.originChain;

      try {
        originAddress = await getOriginalAsset(parsedPayload.originAddress, originChain);
      } catch (e) {
        console.log('cannot native string origin address for chain=', originChain);
      }
      tokenAmount = parsedPayload.amount.toString();
      fee = parsedPayload.fee.toString();
      if (payload[0] == 3) {
        fee = 0; // transferWithPayload doesn't have fees?
      }
      owner = targetAddress;
      if (targetChain == CHAIN_ID_SOLANA) {
        //find owner of token account
        const connection = new Connection(SOLANA_RPC);
        const parsed_account_info = await connection.getParsedAccountInfo(
          new PublicKey(targetAddress)
        );
        try {
          if (parsed_account_info.hasOwnProperty('value')) {
            if (parsed_account_info['value'].hasOwnProperty('data')) {
              const parsed = parsed_account_info.value.data;
              owner = parsed['parsed'].info.owner;
            }
          }
        } catch (e) {
          console.log(
            'could not find owner',
            txn?.transaction['hash'],
            owner,
            new PublicKey(targetAddress)
          );
        }
      }
      redeemed = false;
      try {
        redeemed = await checkRedeemed(CHAIN_INFO_MAP[targetChain], signedVaa);
      } catch {
        console.log(txn['hash'], 'could not check redeem');
      }

      tokenDecimal = 0; //default for wormhole
      try {
        if (originChain === CHAIN_ID_NEAR) {
          // use token name from transfer
          var meta_data = await findNearMetadata(txn);
          tokenDecimal = Math.min(meta_data.decimals, 8);
        } else {
          var meta_data = await findMetadata(originChain, originAddress);
          tokenDecimal = Math.min(meta_data.decimals, 8);
        }
      } catch (e) {
        console.log(`could not find meta_data for ${originChain}, ${originAddress}`);
      }

      if (isNaN(tokenDecimal) || tokenDecimal == undefined || tokenDecimal == null) {
        console.log('nan decimal for txn=', txn['hash'], originChain, originAddress);
        tokenDecimal = -1;
      }
      if (originAddress.length > 150) {
        console.log(`originAddress=${originAddress} is too long. switching to payload address`);
        originAddress = parsedPayload.originAddress;
      }
    } else {
      // not a token transfer, skip
      return results;
    }
  }

  let source_timestamp = txn['blockTimestamp'];
  var source_time = undefined;
  var date = undefined;
  if (source_timestamp !== undefined) {
    source_time = new Date(parseInt(source_timestamp));
    date = source_time.toISOString().slice(0, 10);
  }

  var result = {
    date: date,
    source_time: source_time,
    source_timestamp: source_timestamp / 1000,
    source_hash: txn['hash'],
    chain_id: chainInfo.chain_id,
    emitter_address: emitter_address,
    seq: seq,
    source_address: txn['signerId'],
    source_block: txn['block_height'],
    token_address: originAddress,
    token_chain_id: originChain,
    token_amount: tokenAmount, //to facilitate stringifying bigInt
    token_decimal: tokenDecimal, // need to update this
    signed_vaa: vaaHex,
    target_chain_id: targetChain,
    target_address: owner,
    fee: fee,
    is_redeemed: redeemed,
  };
  results.push(result);

  return results;
}

export async function processCompleteTransferNear(txn) {
  var redeem_hash = txn['hash'];
  var completeTransResults = [];
  var completeTransResult = {};
  var from = txn['signerId'];
  const encodedVm = txn['vaa'];
  //
  try {
    let signedVaa = hexToUint8Array(encodedVm);

    let parsedVaa = parseVaa(signedVaa);
    var targetEmitterChain = parsedVaa['emitter_chain'];
    var targetEmitterAddress = parsedVaa['emitter_address'];

    let payload = Buffer.from(parsedVaa.payload);
    if (payload[0] == 1 || payload[0] == 3) {
      // token bridge transfer
      let parsedPayload;
      try {
        parsedPayload = parseTransferPayload(payload);
      } catch (e) {
        console.log(e);
      }
      var redeem_timestamp = txn['blockTimestamp'];
      var redeem_time = undefined;
      if (redeem_timestamp != undefined) {
        redeem_time = new Date(parseInt(redeem_timestamp));
      }

      let emitter_chain_id = CHAIN_INFO_MAP[targetEmitterChain].chain_id;
      if (emitter_chain_id == undefined) {
        console.log('unknown emitter chain_id=', emitter_chain_id, targetEmitterChain);
        return [];
      }
      var emitter_address = '';
      try {
        emitter_address = tryHexToNativeString(targetEmitterAddress, emitter_chain_id);
      } catch (e) {
        emitter_address = targetEmitterAddress;
      }
      completeTransResult = {
        emitter_address: emitter_address,
        chain_id: targetEmitterChain,
        seq: parsedVaa.sequence,
        is_redeemed: 1,
        redeem_hash: redeem_hash, //txn['hash'],<-- could just be txn['hash']
        redeem_timestamp: redeem_timestamp / 1000,
        redeem_time: redeem_time /*.toISOString()*/,
        redeem_block: txn['block_height'],
        redeem_wallet: from, //txn['from']
      };
      completeTransResults.push(completeTransResult);
    } else {
    }
  } catch (e) {
    console.log('could not parse redeem=', redeem_hash);
  }

  function onlyUnique(obj, index, arr, prop = 'hash') {
    return arr.map((obj) => obj[prop]).indexOf(obj[prop]) === index;
  }

  completeTransResults = completeTransResults.filter(onlyUnique);

  return completeTransResults;
}

export async function runNear(transactionArray, chainInfo) {
  const ACCOUNT_ID = 'sender.mainnet';

  const emitter_address = '148410499d3fcda4dcfd68a1ebfcdddda16ab28326448d4aae4d2f0465cdfcb7';

  const numTransactions = transactionArray.length;
  var transferResults = [];
  var completeTransResults = [];
  /* catch errors */
  var errTransactions = [];

  var i = 0;

  //network config (replace testnet with mainnet or betanet)
  const provider = new nearProviders.JsonRpcProvider(chainInfo.endpoint_url);

  while (i < numTransactions) {
    let txn = transactionArray[i];
    var transfers = [];
    var redeems = [];
    let actions = txn.actions;
    actions.forEach((action) => {
      if (
        action.args.methodName === 'ft_transfer_call' ||
        action.args.methodName === 'send_transfer_near' ||
        action.args.methodName === 'send_transfer_wormhole_token'
      ) {
        //transfer
        transfers.push(txn);
      } else if (action.args.methodName === 'submit_vaa') {
        //complete transfer
        redeems.push(txn);
      }
    });

    if (transfers.length !== 0 || redeems.length !== 0) {
      var result = undefined;
      try {
        result = await provider.txStatus(txn.hash, ACCOUNT_ID);
      } catch (e) {
        console.log(e);
        continue;
      }

      var block_height = undefined;
      try {
        const block_height_response = await provider.block({
          blockId: result?.transaction_outcome?.block_hash,
        });
        if (block_height_response !== undefined) {
          block_height = block_height_response?.header.height;
        }
      } catch (e) {
        console.log(e);
        console.log('could not retrieve block height');
      }
    }
    if (transfers.length !== 0) {
      var events = [];
      result.receipts_outcome.forEach((x: { outcome: { executor_id: string; logs: any } }) => {
        if (x.outcome.executor_id === 'contract.wormhole_crypto.near') {
          const core_bridge_logs = x.outcome.logs;
          core_bridge_logs.forEach((log) => {
            if (log.includes('seq')) {
              let header = 'EVENT_JSON:';
              let ix = log.indexOf(header);
              const event = JSON.parse(log.slice(ix + header.length));
              events.push({
                ...txn,
                ...result,
                ...event,
                block_height: block_height,
              });
            }
          });
        }
      });

      for (let j = 0; j < events.length; j++) {
        let event_ = events[j];
        let tx = event_?.transaction?.hash;
        let seq = event_?.seq;
        var vaa = undefined;

        if (seq === undefined) {
          continue;
        }

        try {
          vaa = await getSignedVAAWithRetry(
            WORMHOLE_RPC_HOSTS,
            CHAIN_ID_NEAR,
            emitter_address,
            seq,
            { transport: NodeHttpTransport() },
            1000,
            6
          );
          if (vaa !== undefined) {
            console.log(`vaa found for seq=${seq}, tx=${tx}`);
          }
        } catch (e) {
          console.log(`vaa not found for seq=${seq}, tx=${tx}`);
        }

        var transferResult = undefined;
        try {
          transferResult = await processTransferNear(seq, vaa, event_, emitter_address, chainInfo);
          if (transferResult != undefined && transferResult.length > 0) {
            console.log(
              `transfer: seq=${seq}, target_chain=${transferResult[0]['target_chain_id']}, 
                              hash=${txn['hash']}, ${i} out of ${numTransactions}`
            );

            transferResults = transferResults.concat(transferResult);
          }
        } catch (e) {
          console.log(`cannot parse transfer for: ${txn['hash']}`);
        }
      }
    } else if (redeems.length !== 0) {
      var completeTransferResults_ = undefined;
      var vaa = undefined;
      result.transaction.actions.forEach(
        (action) =>
          (vaa = JSON.parse(Buffer.from(action['FunctionCall']['args'], 'base64').toString())[
            'vaa'
          ])
      );
      if (vaa !== undefined) {
        var redeem_info = {
          ...txn,
          ...result,
          vaa: vaa,
          block_height: block_height,
        };
        try {
          completeTransferResults_ = await processCompleteTransferNear(redeem_info);
        } catch (e) {
          console.log('could not process complete transfer for txn=', txn['hash']);
        }
        if (completeTransferResults_ != undefined) {
          completeTransferResults_.forEach((completeTransferResult) => {
            if (completeTransferResult.hasOwnProperty('redeem_hash')) {
              completeTransferResult = {
                ...completeTransferResult,
                target_chain_id: chainInfo.chain_id,
              };
              console.log(
                `redeem: chain=${completeTransferResult['chain_id']}, 
                            seq=${completeTransferResult['seq']}, hash=${txn['hash']}, ${i} out of ${numTransactions}`
              );

              completeTransResults.push(completeTransferResult);
            }
          });
        }
      }
    }
    i++;
  }

  console.log(`# of token transfers from ${chainInfo.name}: ${transferResults.length}`);
  console.log(`# of redeems on ${chainInfo.name}: ${completeTransResults.length}`);
  console.log(`# of errs on ${chainInfo.name}: ${errTransactions.length}`);

  return [transferResults, completeTransResults];
}

export async function processTransferAptos(seq, vaa, txn, emitter_address, chainInfo) {
  var results = [];
  // iterate through all sequences in tx log
  var originAddress = undefined;
  var originChain = undefined;
  var tokenAmount = undefined;
  var tokenDecimal = undefined;
  var targetChain = undefined;
  var targetAddress = undefined;
  var fee = undefined;
  var redeemed = undefined;
  var vaaHex = undefined;
  var owner = undefined;

  if (vaa !== undefined) {
    const signedVaa = vaa.vaaBytes;
    vaaHex = uint8ArrayToHex(signedVaa);

    // parse the payload of the VAA
    let parsedVAA = parseVaa(signedVaa);
    let payload = Buffer.from(parsedVAA.payload);
    // payloads that start with 1 are token transfers
    if (payload[0] == 1 || payload[0] == 3) {
      // parse the payload

      let parsedPayload = parseTransferPayload(payload);
      try {
        targetAddress = tryHexToNativeString(
          parsedPayload.targetAddress,
          CHAIN_INFO_MAP[parsedPayload.targetChain].chain_id
        );
      } catch (e) {
        console.log('cannot native string target address');
        targetAddress = parsedPayload.targetAddress;
      }
      targetChain = parsedPayload.targetChain;
      originChain = parsedPayload.originChain;

      try {
        originAddress = await getOriginalAsset(parsedPayload.originAddress, originChain);
      } catch (e) {
        console.log('cannot native string origin address for chain=', originChain);
        originAddress = parsedPayload.originAddress;
      }
      tokenAmount = parsedPayload.amount.toString();
      fee = parsedPayload.fee !== undefined ? parsedPayload.fee.toString() : '0';
      if (payload[0] == 3) {
        fee = 0; // transferWithPayload doesn't have fees?
      }
      owner = targetAddress;
      if (targetChain == CHAIN_ID_SOLANA) {
        //find owner of token account
        const connection = new Connection(SOLANA_RPC);
        const parsed_account_info = await connection.getParsedAccountInfo(
          new PublicKey(targetAddress)
        );
        try {
          if (parsed_account_info.hasOwnProperty('value')) {
            if (parsed_account_info['value'].hasOwnProperty('data')) {
              const parsed = parsed_account_info.value.data;
              owner = parsed['parsed'].info.owner;
            }
          }
        } catch (e) {
          console.log('could not find owner', txn['hash'], owner, new PublicKey(targetAddress));
        }
      }
      redeemed = false;
      try {
        redeemed = await checkRedeemed(CHAIN_INFO_MAP[targetChain], signedVaa);
      } catch {
        console.log(txn['hash'], 'could not check redeem');
      }
      tokenDecimal = 0; //default for wormhole
      try {
        var meta_data = await findMetadata(originChain, originAddress);
        tokenDecimal = Math.min(meta_data.decimals, 8);
      } catch (e) {
        console.log(`could not find meta_data for ${originChain}, ${originAddress}`);
      }
      if (isNaN(tokenDecimal) || tokenDecimal == undefined || tokenDecimal == null) {
        console.log('nan decimal for txn=', txn['hash'], originChain, originAddress);
        tokenDecimal = -1;
      }

      if (originAddress.length > 150) {
        console.log(`originAddress=${originAddress} is too long. switching to payload address`);
        originAddress = parsedPayload.originAddress;
      }
    } else {
      // not a token transfer, skip
      return results;
    }
  }
  let source_timestamp = Math.trunc(txn['timestamp'] / 1000);
  var source_time = undefined;
  var date = undefined;
  if (source_timestamp !== undefined) {
    source_time = new Date(source_timestamp);
    date = source_time.toISOString().slice(0, 10);
  }

  var result = {
    date: date,
    source_time: source_time,
    source_timestamp: Math.trunc(source_timestamp / 1000),
    source_hash: txn['hash'],
    chain_id: chainInfo.chain_id,
    emitter_address: emitter_address,
    seq: seq,
    source_address: txn['sender'],
    source_block: txn['version'],
    token_address: originAddress,
    token_chain_id: originChain,
    token_amount: tokenAmount, //to facilitate stringifying bigInt
    token_decimal: tokenDecimal, // need to update this
    signed_vaa: vaaHex,
    target_chain_id: targetChain,
    target_address: owner,
    fee: fee,
    is_redeemed: redeemed,
  };
  results.push(result);
  return results;
}

export async function processCompleteTransferAptos(txn, provider) {
  var redeem_hash = txn['hash'];
  var completeTransResults = [];
  var completeTransResult = {};
  var from = txn['sender'];
  var data = undefined;
  try {
    if (txn.payload !== undefined) {
      data = txn.payload.arguments[0];
    }
  } catch (e) {
    console.log('cannot access data');
  }
  if (data === undefined) {
    console.log('no vaa');
    return completeTransResults;
  }
  var encodedVms = [];
  var encodedVm = '';

  let headers = ['0100000001', '0100000002', '0100000003'];
  for (let i = 0; i < headers.length; i++) {
    let header = headers[i];

    let ixs = [];
    let done = 0;
    let start = 0;
    while (start < data.length && done == 0 && ixs.length < 2) {
      var vm_index = data.indexOf(header, start);
      if (vm_index == -1) {
        done = 1;
      } else {
        ixs.push(vm_index);
        start = vm_index + header.length;
      }
    }
    let j = 0;
    while (j < ixs.length) {
      let vaa_index = ixs[j];
      if (j == ixs.length - 1) {
        encodedVms.push(data.slice(vaa_index));
      } else if (ixs.length > j + 1) {
        encodedVms.push(data.slice(vaa_index, ixs[j + 1] - 1));
      }
      j++;
    }
  }

  for (let i = 0; i < encodedVms.length; i++) {
    encodedVm = encodedVms[i];
    //
    try {
      let signedVaa = hexToUint8Array(encodedVm);

      let parsedVaa = parseVaa(signedVaa);
      var targetEmitterChain = parsedVaa['emitter_chain'];
      var targetEmitterAddress = parsedVaa['emitter_address'];
      let payload = Buffer.from(parsedVaa.payload);
      if (payload[0] == 1 || payload[0] == 3) {
        // token bridge transfer
        let parsedPayload;
        try {
          parsedPayload = parseTransferPayload(payload);
        } catch (e) {
          console.log(e);
        }
        var redeem_timestamp = Math.trunc(txn['timestamp'] / 1000);
        var redeem_time = undefined;
        if (redeem_timestamp != undefined) {
          redeem_time = new Date(redeem_timestamp);
        }

        let emitter_chain_id = CHAIN_INFO_MAP[targetEmitterChain].chain_id;
        if (emitter_chain_id == undefined) {
          console.log('unknown emitter chain_id=', emitter_chain_id, targetEmitterChain);
          return [];
        }

        var emitter_address = '';
        try {
          emitter_address = tryHexToNativeString(targetEmitterAddress, emitter_chain_id);
        } catch (e) {
          emitter_address = uint8ArrayToHex(targetEmitterAddress);
        }

        completeTransResult = {
          emitter_address: emitter_address /*uint8ArrayToHex(targetEmitterAddress)*/,
          chain_id: targetEmitterChain,
          seq: parsedVaa.sequence,
          is_redeemed: 1,
          redeem_hash: redeem_hash, //txn['hash'],<-- could just be txn['hash']
          redeem_timestamp: Math.trunc(redeem_timestamp / 1000),
          redeem_time: redeem_time /*.toISOString()*/,
          redeem_block: txn['version'],
          redeem_wallet: from, //txn['from']
        };
        completeTransResults.push(completeTransResult);
      } else {
        continue;
      }
    } catch (e) {
      console.log(e);
      console.log('could not parse redeem=', redeem_hash);
    }
  }
  function onlyUnique(obj, index, arr, prop = 'hash') {
    return arr.map((obj) => obj[prop]).indexOf(obj[prop]) === index;
  }

  completeTransResults = completeTransResults.filter(onlyUnique);
  return completeTransResults;
}

function onlyUnique(obj, index, arr, prop = 'transaction_version') {
  return arr.map((obj) => obj[prop]).indexOf(obj[prop]) === index;
}

export async function runAptos(transactionArray, chainInfo) {
  const nodeUrl = chainInfo.endpoint_url;
  const client = await new AptosClient(nodeUrl);
  // const emitter_address = await getEmitterAddressAptos( // not supported
  //   chainInfo.token_bridge_address
  // );
  const emitter_address = '0000000000000000000000000000000000000000000000000000000000000001';
  var transferResults = [];
  var completeTransResults = [];
  /* catch errors */
  var errTransactions = [];
  var errTransaction = {};

  var i = 0;
  transactionArray = transactionArray.filter(onlyUnique);
  const numTransactions = transactionArray.length;

  while (i < numTransactions) {
    let txnVersion = transactionArray[i];
    console.log(i, txnVersion['transaction_version']);
    if (txnVersion['transaction_version'] === undefined) {
      console.log('skipping undefined transaction_version');
      i++;
      continue;
    }

    // get Txn details
    const txn = await client.getTransactionByVersion(txnVersion.transaction_version);
    console.log(txn);
    // skip reverted transactions
    if (Object.keys(txn).includes('success')) {
      if (txn['success'] !== true) {
        console.log('err transaction=', txn['hash'], `${i} out of ${numTransactions}`);
        i++;
        continue;
      }
    }
    // get the sequence number from the receipt
    const payloadEvent = txn['payload']?.function;
    console.log('payloadEvent=', payloadEvent);
    const events = txn['events'];
    var seq = undefined;
    events.forEach((event) => {
      const eventType = event.type;
      if (eventType.includes('WormholeMessage')) {
        seq = event.sequence_number;
      }
    });
    // if (payloadEvent.includes("transfer_tokens")) {
    if (seq != undefined) {
      var vaa = undefined;
      try {
        vaa = await getSignedVAAWithRetry(
          WORMHOLE_RPC_HOSTS,
          chainInfo.chain_id,
          emitter_address,
          seq,
          { transport: NodeHttpTransport() },
          1000,
          4
        );
      } catch (e) {
        console.log('cannot find signed VAA');
      }
      var transferResult = undefined;
      try {
        transferResult = await processTransferAptos(seq, vaa, txn, emitter_address, chainInfo);
        if (transferResult != undefined && transferResult.length > 0) {
          console.log(
            `transfer: seq=${seq}, target_chain=${transferResult[0]['target_chain_id']}, 
                        hash=${txn['hash']}, ${i} out of ${numTransactions}`
          );
          transferResults = transferResults.concat(transferResult);
        }
      } catch (e) {
        console.log(`cannot parse transfer for: ${txn['hash']}`);
        errTransaction = {
          txId: txn['hash'],
          timestamp: txn['timeStamp'],
          reason: 'no vaa',
          emitterChain: chainInfo.chain_id,
          seqnum: seq,
        };
        errTransactions.push(errTransaction);
        i++;
        continue;
      }
    } else if (payloadEvent.includes('complete_transfer')) {
      console.log('found redeem!');
      var completeTransferResults_ = undefined;
      try {
        completeTransferResults_ = await processCompleteTransferAptos(txn, client);
      } catch (e) {
        console.log('could not process complete transfer for txn=', txn['hash']);
      }
      if (completeTransferResults_ != undefined) {
        completeTransferResults_.forEach((completeTransferResult) => {
          if (completeTransferResult.hasOwnProperty('redeem_hash')) {
            completeTransferResult = {
              ...completeTransferResult,
              target_chain_id: chainInfo.chain_id,
            };
            console.log(
              `redeem: chain=${completeTransferResult['chain_id']}, 
                              seq=${completeTransferResult['seq']}, hash=${txn['hash']}, ${i} out of ${numTransactions}`
            );

            completeTransResults.push(completeTransferResult);
          }
        });
      }
    }
    i++;
  }

  console.log(`# of token transfers from ${chainInfo.name}: ${transferResults.length}`);
  console.log(`# of redeems on ${chainInfo.name}: ${completeTransResults.length}`);
  console.log(`# of errs on ${chainInfo.name}: ${errTransactions.length}`);

  return [transferResults, completeTransResults];
}

export async function processRelayerRedeemEvm(txn, provider) {
  var redeem_hash = txn['hash'];
  var completeTransResults = [];
  var completeTransResult = {};
  var from = txn['from'];
  // completeTransfer ==> redeems wrapped token
  // completeTransferAndUnwrapETH ==> redeems & unwraps the native token
  var data;
  try {
    if (txn?.data == 'deprecated' || txn?.input == 'deprecated' || txn?.input == '') {
      console.log('data is deprecated. trying to repull txn');
      const depTxn = await provider.getTransaction(redeem_hash);
      from = depTxn['from'];
      data = depTxn.data || depTxn.input;
    } else if ('input' in txn) {
      data = txn.input;
    } else {
      data = txn.data;
    }
  } catch (e) {
    console.log('cannot access data');
  }
  var encodedVms = [];
  try {
    let msgTypes = ['completeTransfer', 'completeTransferAndUnwrapETH'];
    for (let i = 0; i < msgTypes.length; i++) {
      let msgType = msgTypes[i];

      var encodedVm = '';
      try {
        var completeTransfer = interf.decodeFunctionData(msgType, data);
        encodedVm = completeTransfer?.encodedVm.slice(2);
        encodedVms.push(encodedVm);
      } catch (e) {
        console.log('could not parse msg type=', msgType);
      }
    }
  } catch (e) {}

  let headers = ['0100000001', '0100000002', '0100000003'];
  for (let i = 0; i < headers.length; i++) {
    let header = headers[i];

    let ixs = [];
    let done = 0;
    let start = 0;
    while (start < data.length && done == 0 && ixs.length < 2) {
      var vm_index = data.indexOf(header, start);
      if (vm_index == -1) {
        done = 1;
      } else {
        ixs.push(vm_index);
        start = vm_index + header.length;
      }
    }
    let j = 0;
    while (j < ixs.length) {
      let vaa_index = ixs[j];
      if (j == ixs.length - 1) {
        encodedVms.push(data.slice(vaa_index));
      } else if (ixs.length > j + 1) {
        encodedVms.push(data.slice(vaa_index, ixs[j + 1] - 1));
      }
      j++;
    }
  }

  for (let i = 0; i < encodedVms.length; i++) {
    encodedVm = encodedVms[i];
    //
    try {
      let signedVaa = hexToUint8Array(encodedVm);

      let parsedVaa = parseVaa(signedVaa);
      var targetEmitterChain = parsedVaa['emitter_chain'];
      var targetEmitterAddress = parsedVaa['emitter_address'];
      let payload = Buffer.from(parsedVaa.payload);
      let parsedPayload;
      try {
        parsedPayload = parseTransferPayload(payload);
      } catch (e) {
        console.log(e);
      }
      var targetAddress = tryHexToNativeString(
        parsedPayload.targetAddress,
        CHAIN_INFO_MAP[parsedPayload.targetChain].chain_id
      );
      const originChain = parsedPayload.originChain;
      var originAddress = '';
      if (originChain == CHAIN_ID_TERRA2) {
        if (parsedPayload.originAddress === NATIVE_TERRA2) {
          originAddress = 'uluna';
        } else {
          originAddress = await queryExternalId(parsedPayload.originAddress);
        }
      } else {
        originAddress = tryHexToNativeAssetString(
          parsedPayload.originAddress,
          parsedPayload.originChain
        );
      }
      const amount = parsedPayload.amount;

      let chain_id = CHAIN_INFO_MAP[targetEmitterChain].chain_id;
      if (chain_id == undefined) {
        console.log('unknown emitter chain_id=', chain_id, targetEmitterChain);
        return [];
      }

      var decimals = 0; //default for wormhole
      try {
        var meta_data = await findMetadata(originChain, originAddress);
        decimals = Math.min(meta_data.decimals, 8);
      } catch (e) {
        console.log(`could not find meta_data for ${originChain}, ${originAddress}`);
      }
      if (isNaN(decimals) || decimals == undefined || decimals == null) {
        console.log('nan decimal for txn=', txn['hash'], originChain, originAddress);
        decimals = -1;
      }

      let redeem_time = new Date(parseInt(txn['timeStamp']) * 1000);

      completeTransResult = {
        date: redeem_time.toISOString().slice(0, 10),
        redeem_hash: redeem_hash, //txn['hash'],<-- could just be txn['hash']
        redeem_block: txn['blockNumber'],
        redeem_time: redeem_time /*.toISOString()*/,
        redeem_timestamp: txn['timeStamp'],
        redeem_wallet: from, //txn['from']
        chain_id: chain_id,
        emitter_address: tryHexToNativeString(targetEmitterAddress, chain_id),
        seq: parsedVaa.sequence,
        token_address: originAddress,
        token_chain: originChain,
        token_amount: amount,
        token_decimal: decimals,
        signed_vaa: encodedVm,
        target_address: targetAddress,
        fee: parsedPayload.fee.toString(),
      };

      completeTransResults.push(completeTransResult);
    } catch (e) {
      console.log('could not parse redeem=', redeem_hash);
    }
  }
  function onlyUnique(obj, index, arr, prop = 'hash') {
    return arr.map((obj) => obj[prop]).indexOf(obj[prop]) === index;
  }

  completeTransResults = completeTransResults.filter(onlyUnique);

  return completeTransResults;
}

export async function processRelayerRedeemSolana(txn, chainInfo) {
  var redeem_hash = txn['txHash'];

  var completeTransResult = {};
  const connection = new Connection(SOLANA_RPC);
  var completeTransResult = {};
  var message_type = 0;

  var redeem_wallet = txn.transaction.message.accountKeys[0].pubkey.toString();

  if (txn.hasOwnProperty('meta')) {
    if (txn['meta'].hasOwnProperty('logMessages')) {
      message_type = checkPostVaaAndMint(txn?.meta?.logMessages);
      var instructions = txn?.transaction?.message?.instructions;
      if (instructions != undefined && instructions.length > 0) {
        var accounts = [];
        const redeem_instruction = instructions.filter((ix) => ix?.data == '3' || ix?.data == '4');
        if (redeem_instruction.length > 0) {
          accounts = redeem_instruction[0]?.accounts;
        } else if (instructions.length > 1) {
          // redeem might not be first instruction
          if (txn.meta.innerInstructions.length > 1) {
            if (txn.meta.innerInstructions[1].instructions.length > 2) {
              if (txn.meta.innerInstructions[1].instructions[2].hasOwnProperty('accounts')) {
                accounts = txn.meta.innerInstructions[1].instructions[2]['accounts'];
              } else {
                accounts = [];
              }
            } else {
              accounts = [];
            }
          } else {
            accounts = [];
          }
        } else {
          console.log('cannot find redeem accounts');
        }
        if (accounts != undefined && accounts.length > 3) {
          const vaa_account = accounts[2];
          let vaaAccountInfo = await connection.getAccountInfo(vaa_account);
          let vaaAccountData = vaaAccountInfo?.data;

          if (vaaAccountData != undefined) {
            let parsedMessage = parsePostedMessage(uint8ArrayToHex(vaaAccountData));
            if (parsedMessage.sequence == null) {
              return completeTransResult;
            } else if (parsedMessage.sequence > 1_000_000_000) {
              console.log('seq overflow', parsedMessage, txn['txHash']);
              return completeTransResult;
            }
            let emitter_chain_id = CHAIN_INFO_MAP[parsedMessage.emitter_chain].chain_id;
            if (emitter_chain_id == undefined) {
              console.log(
                'unknown emitter chain_id=',
                emitter_chain_id,
                parsedMessage.emitter_chain
              );
              return completeTransResult;
            } else {
              // look for the vaa from certus
              const emitter_address = await getEmitterAddressSolana(chainInfo.token_bridge_address);
              var vaa = undefined;
              try {
                vaa = await getSignedVAAWithRetry(
                  WORMHOLE_RPC_HOSTS,
                  chainInfo.chain_id,
                  emitter_address,
                  parsedMessage.sequence.toString(),
                  { transport: NodeHttpTransport() },
                  1000,
                  4
                );
              } catch (e) {
                console.log(`cannot find signed VAA for: ${txn['txHash']}`);
                return {};
              }
              const signedVaa = vaa.vaaBytes;
              // parse the payload of the VAA
              let parsedVAA = parseVaa(signedVaa);
              let payload = Buffer.from(parsedVAA.payload);

              let parsedPayload;
              try {
                parsedPayload = parseTransferPayload(payload);
              } catch (e) {
                console.log(e);
              }

              var targetAddress = tryHexToNativeString(
                parsedPayload.targetAddress,
                CHAIN_INFO_MAP[parsedPayload.targetChain].chain_id
              );
              const originChain = parsedPayload.originChain;

              var originAddress = '';
              if (originChain == CHAIN_ID_TERRA2) {
                if (parsedPayload.originAddress === NATIVE_TERRA2) {
                  originAddress = 'uluna';
                } else {
                  originAddress = await queryExternalId(parsedPayload.originAddress);
                }
              } else {
                originAddress = tryHexToNativeAssetString(
                  parsedPayload.originAddress,
                  parsedPayload.originChain
                );
              }
              const amount = parsedPayload.amount;

              var decimals = 0; //default for wormhole
              try {
                var meta_data = await findMetadata(originChain, originAddress);
                decimals = Math.min(meta_data.decimals, 8);
              } catch (e) {
                console.log(`could not find meta_data for ${originChain}, ${originAddress}`);
              }
              if (isNaN(decimals) || decimals == undefined || decimals == null) {
                console.log('nan decimal for txn=', txn['hash'], originChain, originAddress);
                decimals = -1;
              }
              const redeem_time = new Date(txn['blockTime'] * 1000);
              completeTransResult = {
                date: redeem_time.toISOString().slice(0, 10),
                redeem_hash: redeem_hash, //txn['hash'],<-- could just be txn['hash']
                redeem_block: txn['slot'],
                redeem_time: redeem_time /*.toISOString()*/,
                redeem_timestamp: txn['blockTime'],
                redeem_wallet: redeem_wallet,
                chain_id: emitter_chain_id,
                emitter_address: tryHexToNativeString(
                  parsedMessage.emitter_address,
                  emitter_chain_id
                ) /*uint8ArrayToHex(targetEmitterAddress)*/,
                seq: parsedMessage.sequence,
                token_address: originAddress,
                token_chain: originChain,
                token_amount: amount,
                token_decimal: decimals,
                signed_vaa: uint8ArrayToHex(signedVaa),
                target_address: targetAddress,
                fee: parsedPayload.fee.toString(),
              };

              return completeTransResult;
            }
          }
        }
      }
    }
  }
  return completeTransResult;
}

export async function processRelayerRedeemTerra(txn, chainInfo) {
  var redemptionTxId = txn['txhash'];
  var completeTransResult = {};

  try {
    //there has to be a better way to grab the vaa
    var vaa = '';
    try {
      if (txn.hasOwnProperty('tx.value.msg')) {
        var parsedLog = txn['tx.value.msg'];
        if (parsedLog.length > 0 && parsedLog[0].hasOwnProperty('value')) {
          var parsedLogValue = parsedLog[0]['value'];
          if (parsedLogValue.hasOwnProperty('execute_msg')) {
            var execute_msg = parsedLogValue['execute_msg'];
            if (execute_msg.hasOwnProperty('submit_vaa')) {
              vaa = execute_msg['submit_vaa']['data'];
            } else if (execute_msg.hasOwnProperty('process_anchor_message')) {
              vaa = execute_msg['process_anchor_message']['option_token_transfer_vaa'];
            } else {
              console.log('cannot process complete transfer', redemptionTxId);
            }
          }
        }
      } else if (txn.hasOwnProperty('tx')) {
        if (txn['tx'].hasOwnProperty('value')) {
          try {
            var parsedLog = txn['tx']['value']['msg'];
            if (parsedLog.length > 0 && parsedLog[0].hasOwnProperty('value')) {
              var parsedLogValue = parsedLog[0]['value'];
              if (parsedLogValue.hasOwnProperty('execute_msg')) {
                if (parsedLogValue['execute_msg'].hasOwnProperty('submit_vaa')) {
                  vaa = parsedLogValue['execute_msg']['submit_vaa']['data'];
                } else if (parsedLogValue['execute_msg'].hasOwnProperty('process_anchor_message')) {
                  // anchor protocol specific
                  vaa =
                    parsedLogValue['execute_msg']['process_anchor_message'][
                      'option_token_transfer_vaa'
                    ];
                }
              }
            }
          } catch (e) {
            console.log(e);
          }
        }
      } else {
        console.log('cannot extract vaa', redemptionTxId);
      }
    } catch (e) {
      console.log('cannot extract vaa2', redemptionTxId);
    }

    if (vaa == '') {
      return {};
    } else {
      var signedVaa = toUint8Array(vaa);

      let parsedVaa = parseVaa(signedVaa);
      var targetEmitterChain = parsedVaa['emitter_chain'];
      var targetEmitterAddress = parsedVaa['emitter_address'];

      let payload = Buffer.from(parsedVaa.payload);

      let parsedPayload;
      try {
        parsedPayload = parseTransferPayload(payload);
      } catch (e) {
        console.log(e);
      }

      var targetAddress = tryHexToNativeString(
        parsedPayload.targetAddress,
        CHAIN_INFO_MAP[parsedPayload.targetChain].chain_id
      );
      const originChain = parsedPayload.originChain;

      var originAddress = '';
      if (originChain == CHAIN_ID_TERRA2) {
        if (parsedPayload.originAddress === NATIVE_TERRA2) {
          originAddress = 'uluna';
        } else {
          originAddress = await queryExternalId(parsedPayload.originAddress);
        }
      } else {
        originAddress = tryHexToNativeAssetString(
          parsedPayload.originAddress,
          parsedPayload.originChain
        );
      }
      const amount = parsedPayload.amount;

      var decimals = 0; //default for wormhole
      try {
        var meta_data = await findMetadata(originChain, originAddress);
        decimals = Math.min(meta_data.decimals, 8);
      } catch (e) {
        console.log(`could not find meta_data for ${originChain}, ${originAddress}`);
      }
      if (isNaN(decimals) || decimals == undefined || decimals == null) {
        console.log('nan decimal for txn=', txn['hash'], originChain, originAddress);
        decimals = -1;
      }

      var logs = txn['logs'];
      var redeem_wallet = '';
      logs.forEach((log) => {
        if (log.hasOwnProperty('events')) {
          log['events'].forEach((event) => {
            if (event.type == 'from_contract') {
              event['attributes'].forEach((attribute) => {
                if (attribute.key == 'recipient') {
                  redeem_wallet = attribute['value'];
                }
              });
            }
          });
        }
      });

      let emitter_chain_id = CHAIN_INFO_MAP[targetEmitterChain].chain_id;
      if (emitter_chain_id == undefined) {
        console.log('unknown emitter chain_id=', emitter_chain_id, targetEmitterChain);
        return completeTransResult;
      }

      const redeem_time = new Date(txn['timestamp']);
      completeTransResult = {
        //emitter_address: uint8ArrayToHex(targetEmitterAddress),
        date: redeem_time.toISOString().slice(0, 10),
        redeem_hash: redemptionTxId, //txn['hash'],<-- could just be txn['hash']
        redeem_block: txn['height'],
        redeem_time: redeem_time,
        redeem_timestamp: new Date(txn['timestamp']).getTime() / 1000,
        redeem_wallet: redeem_wallet,
        chain_id: emitter_chain_id,
        emitter_address: tryHexToNativeString(
          targetEmitterAddress,
          emitter_chain_id
        ) /*uint8ArrayToHex(targetEmitterAddress)*/,
        seq: parsedVaa.sequence,
        token_address: originAddress,
        token_chain: originChain,
        token_amount: amount,
        token_decimal: decimals,
        signed_vaa: uint8ArrayToHex(signedVaa),
        target_address: targetAddress,
        fee: parsedPayload.fee.toString(),
      };

      return completeTransResult;
    }
  } catch (e) {
    return {};
  }
}

export async function runEvmRelayer(transactionArray, chainInfo) {
  const provider = new ethers.providers.JsonRpcProvider(chainInfo.endpoint_url);

  const numTransactions = transactionArray.length;
  var redeems = [];
  /* catch errors */
  var errTransactions = [];
  var errTransaction = {};

  var i = 0;

  while (i < numTransactions) {
    let txn = transactionArray[i];
    console.log(i, txn['hash']);

    // get the receipt from the infura txn
    var receipt;
    try {
      receipt = await provider.getTransactionReceipt(txn['hash']);
    } catch (e) {
      console.log(`cannot find receipt for: ${txn['hash']}`);
      errTransaction = {
        txId: txn['hash'],
        timestamp: txn['timeStamp'],
        reason: 'no receipt',
        emitterChain: chainInfo.chain_id,
        seqnum: null,
      };
      errTransactions.push(errTransaction);
      i++;
      continue;
    }

    try {
      var completeTransferResults_ = await processRelayerRedeemEvm(txn, provider);
    } catch (e) {
      console.log('could not process complete transfer for txn=', txn['hash']);
    }
    completeTransferResults_.forEach((completeTransferResult) => {
      if (completeTransferResult.hasOwnProperty('redeem_hash')) {
        completeTransferResult = {
          ...completeTransferResult,
          target_chain_id: chainInfo.chain_id,
          success: receipt?.status,
        };
        console.log(
          `redeem: chain=${completeTransferResult['chain_id']}, 
                    seq=${completeTransferResult['seq']}, hash=${txn['hash']}, ${i} out of ${numTransactions}`
        );

        redeems.push(completeTransferResult);
      }
    });

    i++;
  }

  console.log(`# of redeems on ${chainInfo.name}: ${redeems.length}`);

  return redeems;
}

export async function runSolanaRelayer(transactionArray, chainInfo) {
  const numTransactions = transactionArray.length;
  var redeems = [];
  /* catch errors */

  var i = 0;

  while (i < numTransactions) {
    let txn = transactionArray[i];
    console.log(i, txn['txHash']);
    var success = 0;
    if (txn?.meta?.err != null) {
      success = 0;
    } else {
      success = 1;
    }

    try {
      var completeTransferResult = await processRelayerRedeemSolana(txn, chainInfo);
    } catch (e) {
      console.log('could not process complete trasnfer=', txn['txHash']);
    }

    if (completeTransferResult.hasOwnProperty('redeem_hash')) {
      completeTransferResult = {
        ...completeTransferResult,
        target_chain_id: chainInfo.chain_id,
        success: success,
      };
      console.log(
        `redeem: chain=${completeTransferResult['chain_id']}, 
                seq=${completeTransferResult['seq']}, hash=${txn['txHash']}, ${i} out of ${numTransactions}`
      );
      redeems.push(completeTransferResult);
    }

    i++;
  }

  console.log(`# of redeems on ${chainInfo.name}: ${redeems.length}`);

  return redeems;
}

export async function runTerraRelayer(transactionArray, chainInfo) {
  const numTransactions = transactionArray.length;
  var redeems = [];
  /* catch errors */

  var i = 0;

  while (i < numTransactions) {
    let txn = transactionArray[i];
    console.log(i, txn['txhash']);
    var reason;
    var success = 0;
    if (txn['raw_log'].search('VaaAlreadyExecuted')) {
      success = 0;
      reason = 'VaaAlreadyExecuted';
    } else if (txn?.code != undefined) {
      success = 0;
      reason = txn['code'].toString();
    } else if (txn['raw_log'].search('complete_transfer') != -1) {
      success = 1;
    }

    try {
      var completeTransferResult = await processRelayerRedeemTerra(txn, chainInfo);
    } catch (e) {
      console.log('could not process complete trasnfer=', txn['txhash']);
    }

    if (completeTransferResult.hasOwnProperty('redeem_hash')) {
      completeTransferResult = {
        ...completeTransferResult,
        target_chain_id: chainInfo.chain_id,
        success: success,
        reason: reason,
      };
      console.log(
        `redeem: chain=${completeTransferResult['chain_id']}, 
                seq=${completeTransferResult['seq']}, hash=${txn['txhash']}, ${i} out of ${numTransactions}`
      );
      redeems.push(completeTransferResult);
    }

    i++;
  }

  console.log(`# of redeems on ${chainInfo.name}: ${redeems.length}`);

  return redeems;
}
