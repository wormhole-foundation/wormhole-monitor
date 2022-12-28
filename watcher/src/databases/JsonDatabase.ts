import { ChainId, ChainName, coalesceChainId } from '@certusone/wormhole-sdk/lib/cjs/utils/consts';
import { MessagesByChain, WormholeMessage } from '@wormhole-foundation/wormhole-monitor-common';
import { readFileSync, writeFileSync } from 'fs';
import { DB_LAST_BLOCK_FILE, JSON_DB_FILE } from '../consts';
import { Database } from './Database';
import { compareWormholeMessages } from './utils';

const ENCODING = 'utf8';
export class JsonDatabase extends Database {
  db: MessagesByChain;
  lastMessageByChain: { [chain in ChainId]?: WormholeMessage };
  dbFile: string;
  dbLastMessageFile: string;

  constructor() {
    super();
    this.db = {};
    this.lastMessageByChain = {};
    if (!process.env.JSON_DB_FILE) {
      this.logger.info(`no db file set, using default path=${JSON_DB_FILE}`);
    }
    if (!process.env.DB_LAST_BLOCK_FILE) {
      this.logger.info(`no db file set, using default path=${DB_LAST_BLOCK_FILE}`);
    }
    this.dbFile = JSON_DB_FILE;
    this.dbLastMessageFile = DB_LAST_BLOCK_FILE;

    try {
      const rawDb = readFileSync(this.dbFile, ENCODING);
      this.db = JSON.parse(rawDb);
      const rawLast = readFileSync(this.dbLastMessageFile, ENCODING);
      this.lastMessageByChain = JSON.parse(rawLast);
    } catch (e) {
      this.logger.warn('Failed to load DB, initiating a fresh one.');
      this.db = {};
    }
  }

  async getLastMessageByChain(chain: ChainName): Promise<WormholeMessage | null> {
    const chainId = coalesceChainId(chain);
    return this.lastMessageByChain[chainId] ?? null;
  }

  async storeWormholeMessages(chain: ChainName, messages: WormholeMessage[]): Promise<void> {
    if (messages.length === 0) return;

    const chainId = coalesceChainId(chain);
    const sortedMessages = messages.sort(compareWormholeMessages);
    for (const message of sortedMessages) {
      this.db[chainId] = this.db[chainId] ?? {};
      this.db[chainId]![message.key] = message;
    }
    writeFileSync(this.dbFile, JSON.stringify(this.db), ENCODING);

    const lastNewMessage = sortedMessages.at(-1)!;
    const lastStoredMessage = await this.getLastMessageByChain(chain);
    if (lastStoredMessage && compareWormholeMessages(lastNewMessage, lastStoredMessage) > 0) {
      this.lastMessageByChain[chainId] = lastNewMessage;
      writeFileSync(this.dbLastMessageFile, JSON.stringify(this.lastMessageByChain), ENCODING);
    }
  }
}
