require('dotenv').config();
const fs = require('fs');
const { ethers } = require('ethers');
const mysql = require('mysql2/promise');

const {
  RPC_URL,
  START_BLOCK,
  DB_HOST,
  DB_USER,
  DB_PASS,
  DB_NAME
} = process.env;

const ENV_PATH = './.env';
const BATCH_SIZE = 100;

const POOL_CREATED_TOPIC = ethers.id("PoolCreated(address,address,uint24,int24,address)");
const SWAP_TOPIC = ethers.id("Swap(address,uint256,uint256,uint256,uint256,address)");
const SYNC_TOPIC = ethers.id("Sync(uint112,uint112)");
const V3_SWAP_TOPIC = ethers.id("Swap(address,address,int256,int256,uint160,uint128,int24,uint128,uint128)");

const provider = new ethers.JsonRpcProvider(RPC_URL);

// ✅ 主流币配置（包含名称 + 汇率）
const MAIN_TOKENS = {
  "0x55d398326f99059ff775485246999027b3197955": { name: "USDT", rate: 1 },
  "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d": { name: "USDC", rate: 1 },
  "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c": { name: "WBNB", rate: 700 },
  "0xe9e7cea3dedca5984780bafc599bd69add087d56": { name: "BUSD", rate: 1 },
  "0x2170ed0880ac9a755fd29b2688956bd959f933f8": { name: "ETH", rate: 3500 },
  "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3": { name: "DAI", rate: 1 }
};

const V3_FACTORY = "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865".toLowerCase();

const ERC20_ABI = [
  "function symbol() view returns (string)",
  "function totalSupply() view returns (uint256)",
  "function decimals() view returns (uint8)",
  "function balanceOf(address) view returns (uint256)"
];

const V2_LP_ABI = [
  "function token0() view returns (address)",
  "function token1() view returns (address)"
];

const V3_POOL_ABI = [
  "function token0() view returns (address)",
  "function token1() view returns (address)"
];

// ✅ 安全获取 decimals（不存在则返回 18）
async function safeDecimals(address) {
  try {
    const code = await provider.getCode(address);
    if (code === '0x') {
      console.warn(`⚠️ ${address} 不是合约，跳过 decimals()`);
      return 18;
    }

    const contract = new ethers.Contract(address, ERC20_ABI, provider);
    return await contract.decimals();
  } catch (err) {
    console.warn(`⚠️ ${address} decimals() 调用失败: ${err.message}`);
    return 18;
  }
}

// ✅ 更新 .env 中 START_BLOCK
function updateStartBlock(newBlock) {
  try {
    const envData = fs.readFileSync(ENV_PATH, 'utf8').split('\n');
    let updated = false;
    const newEnvData = envData.map(line => {
      if (line.startsWith('START_BLOCK=')) {
        updated = true;
        return `START_BLOCK=${newBlock}`;
      }
      return line;
    });
    if (!updated) newEnvData.push(`START_BLOCK=${newBlock}`);
    fs.writeFileSync(ENV_PATH, newEnvData.join('\n'));
    console.log(`✅ 已更新 START_BLOCK=${newBlock}`);
  } catch (err) {
    console.error('❌ 更新 .env 失败:', err.message);
  }
}

function containsMainToken(token0, token1) {
  const t0 = token0?.toLowerCase();
  const t1 = token1?.toLowerCase();
  return !!(MAIN_TOKENS[t0] || MAIN_TOKENS[t1]);
}

function getUSDTValue(tokenAddress, amount, decimals) {
  const info = MAIN_TOKENS[tokenAddress.toLowerCase()];
  if (!info) return 0;
  return parseFloat(ethers.formatUnits(amount, decimals)) * info.rate;
}

function getParentToken(token0, token1) {
  const t0Main = MAIN_TOKENS[token0.toLowerCase()];
  const t1Main = MAIN_TOKENS[token1.toLowerCase()];
  if (!t0Main) return token0;
  if (!t1Main) return token1;
  if (token0.toLowerCase() === "0x55d398326f99059fF775485246999027B3197955".toLowerCase()) return token1;
  return token0;
}

function getMainToken(token0, token1, parentToken) {
  return token0.toLowerCase() === parentToken.toLowerCase() ? token1 : token0;
}

async function fetchTokenInfo(provider, address) {
  const contract = new ethers.Contract(address, ERC20_ABI, provider);
  let symbol = "UNK", totalSupplyReadable = "0";
  const decimals = await safeDecimals(address);

  try { symbol = await contract.symbol(); } catch {}
  try {
    const supply = await contract.totalSupply();
    totalSupplyReadable = ethers.formatUnits(supply, decimals);
  } catch {}
  return { symbol, totalSupplyReadable };
}

async function getTokenBalances(provider, poolAddress, token0, token1) {
  try {
    const tokenContract0 = new ethers.Contract(token0, ERC20_ABI, provider);
    const tokenContract1 = new ethers.Contract(token1, ERC20_ABI, provider);
    const [dec0, dec1, bal0, bal1] = await Promise.all([
      safeDecimals(token0),
      safeDecimals(token1),
      tokenContract0.balanceOf(poolAddress),
      tokenContract1.balanceOf(poolAddress)
    ]);
    return {
      token0_balance: ethers.formatUnits(bal0, dec0),
      token1_balance: ethers.formatUnits(bal1, dec1)
    };
  } catch {
    return { token0_balance: "0", token1_balance: "0" };
  }
}

async function checkV2LP(provider, address) {
  const lpContract = new ethers.Contract(address, V2_LP_ABI, provider);
  try {
    const token0 = await lpContract.token0();
    const token1 = await lpContract.token1();
    return { isLP: true, token0, token1 };
  } catch {
    return { isLP: false };
  }
}

async function getV3Tokens(lpAddress) {
  try {
    const contract = new ethers.Contract(lpAddress, V3_POOL_ABI, provider);
    const [token0, token1] = await Promise.all([
      contract.token0(),
      contract.token1()
    ]);
    return [token0, token1];
  } catch (err) {
    console.warn(`⚠️ 无法从 ${lpAddress} 获取 V3 token 对: ${err.message}`);
    return [null, null];
  }
}

async function main() {
  console.log("✅ 开始同步 LP 数据...");
  const db = await mysql.createPool({ host: DB_HOST, user: DB_USER, password: DB_PASS, database: DB_NAME });

  const latestBlock = await provider.getBlockNumber();
  console.log(`当前区块高度: ${latestBlock}`);

  let fromBlock = parseInt(START_BLOCK);
  while (fromBlock <= latestBlock) {
    const toBlock = Math.min(fromBlock + BATCH_SIZE - 1, latestBlock);
    console.log(`⛏ 处理区块 ${fromBlock}-${toBlock}`);

    try {
      const v3Logs = await provider.getLogs({ fromBlock, toBlock, address: V3_FACTORY, topics: [POOL_CREATED_TOPIC] });
      const swapAndSyncLogs = await provider.getLogs({ fromBlock, toBlock, topics: [[SWAP_TOPIC, SYNC_TOPIC]] });
      const v3SwapLogs = await provider.getLogs({ fromBlock, toBlock, topics: [V3_SWAP_TOPIC] });

      console.log(`⛏ V3 PoolCreated: ${v3Logs.length}, V3 Swap: ${v3SwapLogs.length}, V2事件: ${swapAndSyncLogs.length}`);

      for (const log of v3Logs) {
        const token0 = ethers.getAddress("0x" + log.topics[1].slice(26));
        const token1 = ethers.getAddress("0x" + log.topics[2].slice(26));
        const [, pool] = ethers.AbiCoder.defaultAbiCoder().decode(["int256", "address"], log.data);
        if (containsMainToken(token0, token1)) await insertV3Pool(db, pool.toLowerCase(), token0, token1);
      }

      for (const log of swapAndSyncLogs) {
        const lpAddress = log.address.toLowerCase();
        await ensureV2Pool(db, lpAddress);
        if (log.topics[0] === SYNC_TOPIC) {
          const [r0, r1] = ethers.AbiCoder.defaultAbiCoder().decode(["uint112", "uint112"], log.data);
          await updateReserves(db, lpAddress, r0, r1);
        }
        if (log.topics[0] === SWAP_TOPIC) await updateVolume(db, lpAddress, log.data);
      }

      for (const log of v3SwapLogs) {
        const lpAddress = log.address.toLowerCase();
        await updateV3Volume(db, lpAddress, log.data);
      }
    } catch (err) {
      console.error(`❌ 区块 ${fromBlock}-${toBlock} 出错: ${err.message}`);
      await new Promise(res => setTimeout(res, 10000));
      continue;
    }

    fromBlock = toBlock + 1;
    updateStartBlock(fromBlock);
  }
}

async function ensureV2Pool(db, lpAddress) {
  const [rows] = await db.query("SELECT id FROM token_stats WHERE contract_address=?", [lpAddress]);
  if (rows.length > 0) return;
  const info = await checkV2LP(provider, lpAddress);
  if (!info.isLP || !containsMainToken(info.token0, info.token1)) return;
  const { token0_balance, token1_balance } = await getTokenBalances(provider, lpAddress, info.token0, info.token1);
  const parentToken = getParentToken(info.token0, info.token1);
  const parentInfo = await fetchTokenInfo(provider, parentToken);
  const mainTokenAddr = getMainToken(info.token0, info.token1, parentToken);
  const mainTokenName = MAIN_TOKENS[mainTokenAddr.toLowerCase()]?.name || 'Unknown';

  await db.query(`
    INSERT INTO token_stats (
      contract_address, parent_token_address, token_symbol, total_supply,
      lp_version, token0_address, token1_address, token0_balance, token1_balance, main_token
    ) VALUES (?, ?, ?, ?, 'V2', ?, ?, ?, ?, ?)
  `, [lpAddress, parentToken, parentInfo.symbol, parentInfo.totalSupplyReadable,
      info.token0, info.token1, token0_balance, token1_balance, mainTokenName]);
  console.log(`✅ 新 V2 LP 插入: ${lpAddress}`);
}

async function insertV3Pool(db, poolAddress, token0, token1) {
  const [rows] = await db.query("SELECT id FROM token_stats WHERE contract_address=?", [poolAddress]);
  if (rows.length > 0) return; // 已存在，不插入

  const { token0_balance, token1_balance } = await getTokenBalances(provider, poolAddress, token0, token1);
  const parentToken = getParentToken(token0, token1);
  const parentInfo = await fetchTokenInfo(provider, parentToken);
  const mainTokenAddr = getMainToken(token0, token1, parentToken);
  const mainTokenName = MAIN_TOKENS[mainTokenAddr.toLowerCase()]?.name || 'Unknown';

  let liquidityValue = 0;
  const t0 = token0.toLowerCase();
  const t1 = token1.toLowerCase();
  
  if (MAIN_TOKENS[t0]) {
    liquidityValue = parseFloat(token0_balance) * MAIN_TOKENS[t0].rate;
  } else if (MAIN_TOKENS[t1]) {
    liquidityValue = parseFloat(token1_balance) * MAIN_TOKENS[t1].rate;
  } else {
    liquidityValue = 0;
  }

  await db.query(`
    INSERT INTO token_stats (
      contract_address, parent_token_address, token_symbol, total_supply,
      lp_version, token0_address, token1_address, token0_balance, token1_balance, main_token, liquidity_value
    ) VALUES (?, ?, ?, ?, 'V3', ?, ?, ?, ?, ?, ?)
  `, [poolAddress, parentToken, parentInfo.symbol, parentInfo.totalSupplyReadable,
      token0, token1, token0_balance, token1_balance, mainTokenName, liquidityValue]);

  console.log(`✅ 新 V3 LP 插入: ${poolAddress}`);
}

async function updateReserves(db, lpAddress, r0, r1) {
  try {
    const [rows] = await db.query("SELECT token0_address, token1_address FROM token_stats WHERE contract_address=?", [lpAddress]);
    if (rows.length === 0) return;
    const { token0_address: t0, token1_address: t1 } = rows[0];
    const [dec0, dec1] = await Promise.all([safeDecimals(t0), safeDecimals(t1)]);
    const b0 = parseFloat(ethers.formatUnits(r0, dec0));
    const b1 = parseFloat(ethers.formatUnits(r1, dec1));
    let liquidityValue = 0;
    
    if (MAIN_TOKENS[t0.toLowerCase()]) {
      liquidityValue = b0 * MAIN_TOKENS[t0.toLowerCase()].rate;
    } else if (MAIN_TOKENS[t1.toLowerCase()]) {
      liquidityValue = b1 * MAIN_TOKENS[t1.toLowerCase()].rate;
    } else {
      liquidityValue = 0; // 如果都不是主流币，就不计价值
    }
    
    await db.query(`UPDATE token_stats SET token0_balance=?, token1_balance=?, liquidity_value=?, last_updated=NOW() WHERE contract_address=?`,
                   [b0, b1, liquidityValue, lpAddress]);
  } catch (err) {
    console.error(`❌ 更新储备失败: ${err.message}`);
  }
}

async function updateVolume(db, lpAddress, data) {
  const [amount0In, amount1In, amount0Out, amount1Out] = ethers.AbiCoder.defaultAbiCoder().decode(["uint256","uint256","uint256","uint256"], data);
  const [rows] = await db.query("SELECT token0_address, token1_address FROM token_stats WHERE contract_address=?", [lpAddress]);
  if (rows.length === 0) return;
  const { token0_address: t0, token1_address: t1 } = rows[0];
  const [dec0, dec1] = await Promise.all([safeDecimals(t0), safeDecimals(t1)]);
  let usdtVolume = 0;
  if (MAIN_TOKENS[t0.toLowerCase()]) usdtVolume += getUSDTValue(t0, BigInt(amount0In)+BigInt(amount0Out), dec0);
  if (MAIN_TOKENS[t1.toLowerCase()]) usdtVolume += getUSDTValue(t1, BigInt(amount1In)+BigInt(amount1Out), dec1);
  await db.query(`UPDATE token_stats SET total_transaction_volume = IFNULL(total_transaction_volume,0)+?, trade_count_24h=trade_count_24h+1, trade_count_12h=trade_count_12h+1 WHERE contract_address=?`,
                 [usdtVolume, lpAddress]);
}

async function updateV3Volume(db, lpAddress, data) {
  let [rows] = await db.query(
    "SELECT token0_address, token1_address FROM token_stats WHERE contract_address=?",
    [lpAddress]
  );

  // 如果数据库中没有该 LP 的记录，尝试获取 token0/token1 判断是否应插入
  if (rows.length === 0) {
    const [token0, token1] = await getV3Tokens(lpAddress);
    if (!token0 || !token1) return;

    // 先用正确的 token 对判断是否主流币
    if (!containsMainToken(token0, token1)) {
      return;
    }

    // 确保插入时 token0/token1 是准确的
    await insertV3Pool(db, lpAddress, token0, token1);

    // 插入完再次查询数据库确认
    [rows] = await db.query(
      "SELECT token0_address, token1_address FROM token_stats WHERE contract_address=?",
      [lpAddress]
    );
    if (rows.length === 0) return;
  }

  const { token0_address: t0, token1_address: t1 } = rows[0];

  try {
    const [dec0, dec1] = await Promise.all([safeDecimals(t0), safeDecimals(t1)]);

    const [amount0, amount1] = ethers.AbiCoder.defaultAbiCoder().decode(
      ["int256", "int256", "uint160", "uint128", "int24", "uint128", "uint128"],
      data
    );

    let usdtVolume = 0;
    if (MAIN_TOKENS[t0.toLowerCase()]) usdtVolume += getUSDTValue(t0, BigInt(amount0), dec0);
    if (MAIN_TOKENS[t1.toLowerCase()]) usdtVolume += getUSDTValue(t1, BigInt(amount1), dec1);

    await db.query(`
      UPDATE token_stats
      SET total_transaction_volume = IFNULL(total_transaction_volume, 0) + ?,
          trade_count_24h = trade_count_24h + 1,
          trade_count_12h = trade_count_12h + 1
      WHERE contract_address = ?
    `, [Math.abs(usdtVolume), lpAddress]);

  } catch (err) {
    console.error(`❌ V3 交易更新失败: ${err.message}`);
  }
}

main().catch(console.error);
