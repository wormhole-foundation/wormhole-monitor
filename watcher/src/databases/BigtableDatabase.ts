import { ChainName, coalesceChainId } from '@certusone/wormhole-sdk/lib/cjs/utils/consts';
import { Bigtable } from '@google-cloud/bigtable';
import { WormholeMessage } from '@wormhole-foundation/wormhole-monitor-common';
import { cert, initializeApp } from 'firebase-admin/app';
import { getFirestore } from 'firebase-admin/firestore';
import { assertEnvironmentVariable } from '../utils/environment';
import { getLogger } from '../utils/logger';
import { Database } from './Database';
import { compareWormholeMessages, padWormholeMessageKey } from './utils';

export class BigtableDatabase extends Database {
  tableId: string;
  instanceId: string;
  bigtable: Bigtable;
  firestoreDb: FirebaseFirestore.Firestore;
  latestCollectionName: string;

  constructor() {
    super();
    this.tableId = assertEnvironmentVariable('BIGTABLE_TABLE_ID');
    this.instanceId = assertEnvironmentVariable('BIGTABLE_INSTANCE_ID');
    this.latestCollectionName = assertEnvironmentVariable('FIRESTORE_LATEST_COLLECTION');
    try {
      this.bigtable = new Bigtable();
      const serviceAccount = require(assertEnvironmentVariable('FIRESTORE_ACCOUNT_KEY_PATH'));
      initializeApp({
        credential: cert(serviceAccount),
      });
      this.firestoreDb = getFirestore();
    } catch (e) {
      throw new Error('Could not load bigtable db');
    }
  }

  async getLastMessageByChain(chain: ChainName): Promise<WormholeMessage | null> {
    const chainId = coalesceChainId(chain);
    const lastObservedMessage = this.firestoreDb
      .collection(this.latestCollectionName)
      .doc(chainId.toString());
    const lastObservedMessageByChain = await lastObservedMessage.get();
    const messageData = lastObservedMessageByChain.data();
    const lastMessageJson = messageData?.lastBlockKey;
    if (lastMessageJson) {
      // TODO: ensure we've parsed a valid message
      const lastMessage: WormholeMessage = JSON.parse(lastMessageJson);
      getLogger(chain, this.logger).info(`found most recent firestore message=${lastMessage.key}`);
      return lastMessage;
    }

    return null;
  }

  async storeLatestMessageByChain(chain: ChainName, message: WormholeMessage): Promise<void> {
    const chainId = coalesceChainId(chain);
    getLogger(chain, this.logger).info(`storing last message=${message}`);
    const lastObservedBlock = this.firestoreDb
      .collection(this.latestCollectionName)
      .doc(`${chainId.toString()}`);
    await lastObservedBlock.set({ lastBlockKey: JSON.stringify(message) });
  }

  async storeWormholeMessages(chain: ChainName, messages: WormholeMessage[]): Promise<void> {
    if (this.bigtable === undefined) {
      this.logger.warn('no bigtable instance set');
      return;
    }

    if (messages.length === 0) return;

    const sortedMessages = messages.sort(compareWormholeMessages);
    const instance = this.bigtable.instance(this.instanceId);
    const table = instance.table(this.tableId);
    const rowsToInsert: {
      key: string;
      data: {
        // column family
        info: {
          // columns
          timestamp: { value: string; timestamp: string };
          txHash: { value: string; timestamp: string };
          hasSignedVaa: { value: number; timestamp: string };
        };
      };
    }[] = [];
    for (const message of sortedMessages) {
      rowsToInsert.push({
        key: padWormholeMessageKey(message.key),
        data: {
          info: {
            timestamp: {
              value: message.timestamp,
              // write 0 timestamp to only keep 1 cell each
              // https://cloud.google.com/bigtable/docs/gc-latest-value
              timestamp: '0',
            },
            txHash: {
              value: message.transactionHash,
              timestamp: '0',
            },
            hasSignedVaa: {
              value: 0,
              timestamp: '0',
            },
          },
        },
      });
    }
    await table.insert(rowsToInsert);

    // store latest messages to firestore
    const lastNewMessage = sortedMessages.at(-1)!;
    const lastStoredMessage = await this.getLastMessageByChain(chain);
    if (lastStoredMessage && compareWormholeMessages(lastNewMessage, lastStoredMessage) > 0) {
      this.logger.info(`for chain=${chain}, storing last bigtable message=${lastNewMessage.key}`);
      await this.storeLatestMessageByChain(chain, lastNewMessage);
    }
  }
}
