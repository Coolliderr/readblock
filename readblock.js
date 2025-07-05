const fs = require('fs');
const path = require('path');
const nodereal = require('@api/nodereal');

// === 配置 ===
const ENV_FILE = './.env';
const LOG_FILE = path.join(__dirname, 'logs.jsonl');
const API_KEY = getEnvValue("NODEREAL_API_KEY");
const START_BLOCK = parseInt(getEnvValue("LAST_BLOCK_NUMBER")) || 0;

const V2_FACTORY = "0xca143ce32fe78f1f7019d7d551a6402fc5350c73";
const V3_FACTORY = "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865";
const V4_FACTORY = "0xa0FfB9c1CE1Fe56963B0321B32E7A0302114058b";

const PAIR_CREATED = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9";
const POOL_CREATED = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118";
const V4_INITIALIZE = "0x426cc62fe6a33a40ba2788c2c87a9c34ee4582b95bc9fa5a7bb7ae70b750b99c";

function getEnvValue(key) {
  if (!fs.existsSync(ENV_FILE)) return null;
  const env = fs.readFileSync(ENV_FILE, 'utf8');
  const match = env.match(new RegExp(`^${key}=(.*)$`, 'm'));
  return match ? match[1] : null;
}

function updateEnvValue(key, value) {
  let env = fs.existsSync(ENV_FILE) ? fs.readFileSync(ENV_FILE, 'utf8') : '';
  const regex = new RegExp(`^${key}=.*$`, 'm');
  if (env.match(regex)) {
    env = env.replace(regex, `${key}=${value}`);
  } else {
    env += `\n${key}=${value}`;
  }
  fs.writeFileSync(ENV_FILE, env);
}

function saveLog(entryObject) {
  const line = JSON.stringify(entryObject) + '\n';
  fs.appendFileSync(LOG_FILE, line, 'utf8');
}

async function queryLogs(factory, topic, type, parseFn, blockNumber) {
  try {
    const res = await nodereal.ethGetlogs({
      id: 1,
      jsonrpc: '2.0',
      method: 'eth_getLogs',
      params: [{
        fromBlock: '0x' + blockNumber.toString(16),
        toBlock: '0x' + blockNumber.toString(16),
        address: factory,
        topics: [topic]
      }]
    }, { apiKey: API_KEY });

    const logs = res.data.result;
    if (logs.length > 0) {
      console.log(`📦 ${type}: ${logs.length} events`);
      for (const log of logs) {
        const parsed = parseFn(log);
        console.log(parsed.print);
        saveLog(parsed.save);
      }
    }
  } catch (e) {
    console.warn(`⚠️ ${type} getLogs error at block ${blockNumber}:`, e.message);
  }
}

function parseV2(log) {
  const token0 = '0x' + log.topics[1].slice(26);
  const token1 = '0x' + log.topics[2].slice(26);
  const pair = '0x' + log.data.slice(26, 66);
  return {
    print: `  PairCreated: ${token0} - ${token1} -> ${pair}`,
    save: { type: 'V2', block: parseInt(log.blockNumber, 16), token0, token1, pair }
  };
}

function parseV3(log) {
  const token0 = '0x' + log.topics[1].slice(26);
  const token1 = '0x' + log.topics[2].slice(26);
  const fee = parseInt(log.topics[3], 16);
  const pool = '0x' + log.data.slice(130, 170);
  return {
    print: `  PoolCreated: ${token0} - ${token1} | Fee: ${fee} -> ${pool}`,
    save: { type: 'V3', block: parseInt(log.blockNumber, 16), token0, token1, fee, pool }
  };
}

function parseV4(log) {
  const id = log.topics[1];
  const currency0 = '0x' + log.topics[2].slice(26);
  const currency1 = '0x' + log.topics[3].slice(26);
  const feeHex = log.data.slice(66, 66 + 64);
  const fee = parseInt(feeHex, 16);
  return {
    print: `  Initialize: ${currency0} - ${currency1} | Fee: ${fee} | ID: ${id}`,
    save: { type: 'V4', block: parseInt(log.blockNumber, 16), currency0, currency1, fee, id }
  };
}

let lastBlock = START_BLOCK;

setInterval(async () => {
  try {
    const blockRes = await nodereal.ethGetlogs({
      id: 1,
      jsonrpc: '2.0',
      method: 'eth_blockNumber',
      params: []
    }, { apiKey: API_KEY });

    const latest = parseInt(blockRes.data.result, 16);

    if (latest > lastBlock) {
      lastBlock++;
      console.log(`🔍 Scanning block: ${lastBlock}`);
      await queryLogs(V2_FACTORY, PAIR_CREATED, "V2 PairCreated", parseV2, lastBlock);
      await queryLogs(V3_FACTORY, POOL_CREATED, "V3 PoolCreated", parseV3, lastBlock);
      await queryLogs(V4_FACTORY, V4_INITIALIZE, "V4 Initialize", parseV4, lastBlock);
      updateEnvValue("LAST_BLOCK_NUMBER", lastBlock);
    }
  } catch (e) {
    console.warn("🚫 Error:", e.message || e);
  }
}, 1000);
