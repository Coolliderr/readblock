require('dotenv').config();
const fs = require('fs');
const { ethers } = require('ethers');
const mysql = require('mysql2/promise');
const Redis = require('ioredis');

const {
  RPC_URL,
  DB_HOST,
  DB_USER,
  DB_PASS,
  DB_NAME,
  REDIS_URL,
  REDIS_BLOCK_KEY,
  RATE_WBNB,
  RATE_ETH,
  RATE_BTCB,
} = process.env;

const ENV_PATH = './.env';
const BATCH_SIZE = 100;

const POOL_CREATED_TOPIC = ethers.id("PoolCreated(address,address,uint24,int24,address)");
const SWAP_TOPIC = ethers.id("Swap(address,uint256,uint256,uint256,uint256,address)");
const SYNC_TOPIC = ethers.id("Sync(uint112,uint112)");
const V3_SWAP_TOPIC = ethers.id("Swap(address,address,int256,int256,uint160,uint128,int24,uint128,uint128)");

const provider = new ethers.JsonRpcProvider(RPC_URL);
const redis = new Redis(REDIS_URL);

// ✅ 主流币配置（包含名称 + 汇率）
const MAIN_TOKENS = {
  "0x55d398326f99059ff775485246999027b3197955": { name: "USDT", rate: 1 },
  "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d": { name: "USDC", rate: 1 },
  "0xe9e7cea3dedca5984780bafc599bd69add087d56": { name: "BUSD", rate: 1 },
  "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3": { name: "DAI", rate: 1 },
  "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d": { name: "USD1", rate: 1 },
  "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c": { name: "WBNB", rate: parseFloat(RATE_WBNB || "700") },
  "0x2170ed0880ac9a755fd29b2688956bd959f933f8": { name: "ETH", rate: parseFloat(RATE_ETH || "3500") },
  "0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c": { name: "BTCB", rate: parseFloat(RATE_BTCB || "118000") },
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

// 多进程并发抢占区块段
async function acquireBlockRange(batchSize) {
  const latest = await provider.getBlockNumber();
  const blockKey = REDIS_BLOCK_KEY;
  let fromBlock;

  while (true) {
    fromBlock = parseInt(await redis.get(blockKey)) || (latest - 1000000);
    const toBlock = fromBlock + parseInt(batchSize) - 1;
    if (toBlock > latest) {
      await new Promise(r => setTimeout(r, 5000));
      continue;
    }

    const script = `
      local current = redis.call("GET", KEYS[1])
      if not current or tonumber(ARGV[1]) > tonumber(current) then
        redis.call("SET", KEYS[1], ARGV[1])
        return 1
      else
        return 0
      end
    `;
    const success = await redis.eval(script, 1, blockKey, toBlock + 1);
    
    if (success === 1) {
      console.log(`🟢 进程 ${process.pid} 抢到了区块段: ${fromBlock} - ${toBlock}`);
      return { fromBlock, toBlock };
    }

    await new Promise(r => setTimeout(r, 200));
  }
}

// 获取代币基础信息，包含缓存
async function getTokenMeta(address) {
  const key = `token_meta:${address.toLowerCase()}`;
  const cached = await redis.get(key);
  if (cached) return JSON.parse(cached);

  const contract = new ethers.Contract(address, ERC20_ABI, provider);
  let symbol = "UNK", totalSupplyReadable = "0";
  let decimals = 18;
  try {
    decimals = await contract.decimals();
    decimals = Number(decimals);
  } catch {}
  try {
    symbol = await contract.symbol();
  } catch {}
  try {
    const supply = await contract.totalSupply();
    totalSupplyReadable = ethers.formatUnits(supply, decimals);
  } catch {}

  const result = { symbol, decimals, totalSupplyReadable };
  await redis.set(key, JSON.stringify(result));
  
  return result;
}

// 区分主流币和母币
function analyzePair(token0, token1) {
  const t0 = token0.toLowerCase();
  const t1 = token1.toLowerCase();
  const t0Main = MAIN_TOKENS[t0];
  const t1Main = MAIN_TOKENS[t1];

  const hasMainToken = !!(t0Main || t1Main);

  let mainToken = null;
  let mainTokenName = null;
  let parentToken = null;
  let otherToken = null;

  if (!t0Main && !t1Main) {
    parentToken = token0; // 随便取一个当 parent
    otherToken = token1;
  } else if (t0Main && !t1Main) {
    parentToken = token1;
    mainToken = token0;
    otherToken = token0;
    mainTokenName = t0Main.name;
  } else if (!t0Main && t1Main) {
    parentToken = token0;
    mainToken = token1;
    otherToken = token1;
    mainTokenName = t1Main.name;
  } else {
    // 两个都是主流币：默认 USDT 为次币，另一个为母币
    if (t0 === "0x55d398326f99059fF775485246999027B3197955".toLowerCase()) {
      parentToken = token1;
      mainToken = token0;
      mainTokenName = t0Main.name;
    } else {
      parentToken = token0;
      mainToken = token1;
      mainTokenName = t1Main.name;
    }
  }

  return {
    hasMainToken,
    mainToken,
    mainTokenName,
    parentToken,
    otherToken
  };
}

// 计算资金池价值
function getUSDTValue(tokenAddress, amount, decimals) {
  const info = MAIN_TOKENS[tokenAddress.toLowerCase()];
  if (!info) return 0;
  return parseFloat(ethers.formatUnits(amount, decimals)) * info.rate;
}

// 获取V2池交易对
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

// 获取V3池交易对
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

// 检测到某个V2池发生交易时，更新该池子在数据库中的储备余额和资金池价值
async function updateReserves(db, lpAddress, r0, r1) {
  try {
    const [rows] = await db.query("SELECT token0_address, token1_address FROM token_stats WHERE contract_address=?", [lpAddress]);
    if (rows.length === 0) return;
    const { token0_address: t0, token1_address: t1 } = rows[0];
    const [dec0, dec1] = await Promise.all([getTokenMeta(t0), getTokenMeta(t1)]);
    const b0 = parseFloat(ethers.formatUnits(r0, dec0.decimals));
    const b1 = parseFloat(ethers.formatUnits(r1, dec1.decimals));
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

// 获取代币余额，用于获取V3池储备余额和资金池价值
async function getTokenBalances(provider, poolAddress, token0, token1) {
  try {
    const tokenContract0 = new ethers.Contract(token0, ERC20_ABI, provider);
    const tokenContract1 = new ethers.Contract(token1, ERC20_ABI, provider);
    const [dec0, dec1, bal0, bal1] = await Promise.all([
      getTokenMeta(token0),
      getTokenMeta(token1),
      tokenContract0.balanceOf(poolAddress),
      tokenContract1.balanceOf(poolAddress)
    ]);
    return {
      token0_balance: ethers.formatUnits(bal0, dec0.decimals),
      token1_balance: ethers.formatUnits(bal1, dec1.decimals)
    };
  } catch {
    return { token0_balance: "0", token1_balance: "0" };
  }
}

// 当发现新的V2池并且池子中包含主流币时，向数据库插入一条新记录
async function ensureV2Pool(db, lpAddress) {
  const [rows] = await db.query("SELECT id FROM token_stats WHERE contract_address=?", [lpAddress]);
  if (rows.length > 0) return;

  const info = await checkV2LP(provider, lpAddress);
  if (!info.isLP) return;

  const { hasMainToken, mainToken, mainTokenName, parentToken } = analyzePair(info.token0, info.token1);
  if (!hasMainToken) return;

  const { token0_balance, token1_balance } = await getTokenBalances(provider, lpAddress, info.token0, info.token1);
  const parentInfo = await getTokenMeta(parentToken);

  try {
    await db.query(`
      INSERT INTO token_stats (
        contract_address, parent_token_address, token_symbol, total_supply,
        lp_version, token0_address, token1_address, token0_balance, token1_balance, main_token
      ) VALUES (?, ?, ?, ?, 'V2', ?, ?, ?, ?, ?)
    `, [
      lpAddress,
      parentToken,
      parentInfo.symbol,
      parentInfo.totalSupplyReadable,
      info.token0,
      info.token1,
      token0_balance,
      token1_balance,
      mainTokenName
    ]);
    console.log(`✅ 新 V2 LP 插入: ${lpAddress}`);
    } catch (err) {
    if (err.code === 'ER_DUP_ENTRY') {
      console.warn(`⚠️ 忽略重复插入: ${lpAddress}`);
    } else {
      throw err;
    }
  }
}

// 当发现新的V3池并且池子中包含主流币时，向数据库插入一条新记录
async function insertV3Pool(db, poolAddress, token0, token1) {
  const [rows] = await db.query("SELECT id FROM token_stats WHERE contract_address=?", [poolAddress]);
  if (rows.length > 0) return; // 已存在，不插入

  const { hasMainToken, mainToken, mainTokenName, parentToken } = analyzePair(token0, token1);
  if (!hasMainToken) return;

  const { token0_balance, token1_balance } = await getTokenBalances(provider, poolAddress, token0, token1);
  const parentInfo = await getTokenMeta(parentToken);

  let liquidityValue = 0;
  if (mainToken?.toLowerCase() === token0.toLowerCase()) {
    liquidityValue = parseFloat(token0_balance) * MAIN_TOKENS[mainToken.toLowerCase()].rate;
  } else if (mainToken?.toLowerCase() === token1.toLowerCase()) {
    liquidityValue = parseFloat(token1_balance) * MAIN_TOKENS[mainToken.toLowerCase()].rate;
  }

  try {
    await db.query(`
      INSERT INTO token_stats (
        contract_address, parent_token_address, token_symbol, total_supply,
        lp_version, token0_address, token1_address, token0_balance, token1_balance, main_token, liquidity_value
      ) VALUES (?, ?, ?, ?, 'V3', ?, ?, ?, ?, ?, ?)
    `, [
      poolAddress,
      parentToken,
      parentInfo.symbol,
      parentInfo.totalSupplyReadable,
      token0,
      token1,
      token0_balance,
      token1_balance,
      mainTokenName,
      liquidityValue
    ]);

    console.log(`✅ 新 V3 LP 插入: ${poolAddress}`);
  } catch (err) {
    if (err.code === 'ER_DUP_ENTRY') {
      console.warn(`⚠️ 忽略重复插入: ${poolAddress}`);
    } else {
      throw err;
    }
  }
}

// 更新 V2 LP 的交易量数据
async function updateVolume(db, lpAddress, data) {
  const [amount0In, amount1In, amount0Out, amount1Out] = ethers.AbiCoder.defaultAbiCoder().decode(
    ["uint256", "uint256", "uint256", "uint256"],
    data
  );

  const [rows] = await db.query(
    "SELECT token0_address, token1_address FROM token_stats WHERE contract_address=?",
    [lpAddress]
  );
  if (rows.length === 0) return;

  const { token0_address: token0, token1_address: token1 } = rows[0];
  const { hasMainToken, mainToken } = analyzePair(token0, token1);
  if (!hasMainToken) return;

  const meta = await getTokenMeta(mainToken);
  let usdtVolume = 0n;

  // 只计算主流币一侧的总量（买入+卖出）
  if (mainToken.toLowerCase() === token0.toLowerCase()) {
    usdtVolume = BigInt(amount0In) + BigInt(amount0Out);
  } else if (mainToken.toLowerCase() === token1.toLowerCase()) {
    usdtVolume = BigInt(amount1In) + BigInt(amount1Out);
  }

  const usd = getUSDTValue(mainToken, usdtVolume, meta.decimals);

  await db.query(`
    UPDATE token_stats
    SET total_transaction_volume = IFNULL(total_transaction_volume, 0) + ?,
        trade_count_24h = trade_count_24h + 1,
        trade_count_12h = trade_count_12h + 1
    WHERE contract_address = ?
  `, [usd, lpAddress]);
}

// 更新 V3 LP 的交易量数据
async function updateV3Volume(db, lpAddress, data) {
  let [rows] = await db.query(
    "SELECT token0_address, token1_address FROM token_stats WHERE contract_address=?",
    [lpAddress]
  );

  let token0, token1, analysis;

  if (rows.length === 0) {
    [token0, token1] = await getV3Tokens(lpAddress);
    if (!token0 || !token1) return;

    analysis = analyzePair(token0, token1);
    if (!analysis.hasMainToken) return;

    await insertV3Pool(db, lpAddress, token0, token1);

    [rows] = await db.query(
      "SELECT token0_address, token1_address FROM token_stats WHERE contract_address=?",
      [lpAddress]
    );
    if (rows.length === 0) return;
  }

  token0 = rows[0].token0_address;
  token1 = rows[0].token1_address;
  analysis = analyzePair(token0, token1);
  if (!analysis.hasMainToken) return;

  try {
    const meta = await getTokenMeta(analysis.mainToken);

    const [amount0, amount1] = ethers.AbiCoder.defaultAbiCoder().decode(
      ["int256", "int256", "uint160", "uint128", "int24", "uint128", "uint128"],
      data
    );

    let amount = 0n;
    if (analysis.mainToken.toLowerCase() === token0.toLowerCase()) {
      amount = BigInt(amount0);
    } else if (analysis.mainToken.toLowerCase() === token1.toLowerCase()) {
      amount = BigInt(amount1);
    }

    const usd = Math.abs(getUSDTValue(analysis.mainToken, amount, meta.decimals));

    await db.query(`
      UPDATE token_stats
      SET total_transaction_volume = IFNULL(total_transaction_volume, 0) + ?,
          trade_count_24h = trade_count_24h + 1,
          trade_count_12h = trade_count_12h + 1
      WHERE contract_address = ?
    `, [usd, lpAddress]);

  } catch (err) {
    console.error(`❌ V3 交易更新失败: ${err.message}`);
  }
}

// 主程序
async function main() {
  const db = await mysql.createPool({ host: DB_HOST, user: DB_USER, password: DB_PASS, database: DB_NAME });
  console.log("🚀 LP 同步进程启动，等待分配区块...");

  while (true) {
    // 设置超时处理（60 秒内必须完成处理，否则自动退出）
    const timeout = setTimeout(() => {
      console.error(`🛑 进程 ${process.pid} 处理超时，自动退出防止卡死`);
      process.exit(1);  // 交给 PM2 或脚本管理器重启
    }, 60000); // 60 秒

    try {
      const { fromBlock, toBlock } = await acquireBlockRange(BATCH_SIZE);
      console.log(`📦 正在处理区块 ${fromBlock}-${toBlock}`);

      const v3Logs = await provider.getLogs({ fromBlock, toBlock, address: V3_FACTORY, topics: [POOL_CREATED_TOPIC] });
      const swapAndSyncLogs = await provider.getLogs({ fromBlock, toBlock, topics: [[SWAP_TOPIC, SYNC_TOPIC]] });
      const v3SwapLogs = await provider.getLogs({ fromBlock, toBlock, topics: [V3_SWAP_TOPIC] });

      console.log(`⛏ V3 PoolCreated: ${v3Logs.length}, V3 Swap: ${v3SwapLogs.length}, V2事件: ${swapAndSyncLogs.length}`);

      for (const log of v3Logs) {
        const token0 = ethers.getAddress("0x" + log.topics[1].slice(26));
        const token1 = ethers.getAddress("0x" + log.topics[2].slice(26));
        const [, pool] = ethers.AbiCoder.defaultAbiCoder().decode(["int256", "address"], log.data);
        const { hasMainToken } = analyzePair(token0, token1);
        if (hasMainToken) {
          await insertV3Pool(db, pool.toLowerCase(), token0, token1);
        }
      }

      for (const log of swapAndSyncLogs) {
        const lpAddress = log.address.toLowerCase();
        await ensureV2Pool(db, lpAddress);
        if (log.topics[0] === SYNC_TOPIC) {
          const [r0, r1] = ethers.AbiCoder.defaultAbiCoder().decode(["uint112", "uint112"], log.data);
          await updateReserves(db, lpAddress, r0, r1);
        }
        if (log.topics[0] === SWAP_TOPIC) {
          await updateVolume(db, lpAddress, log.data);
        }
      }

      for (const log of v3SwapLogs) {
        const lpAddress = log.address.toLowerCase();
        await updateV3Volume(db, lpAddress, log.data);
      }

    } catch (err) {
      console.error("❌ 执行出错:", err);
      await new Promise(r => setTimeout(r, 3000));  // 错误等待
    } finally {
      clearTimeout(timeout);  // 只要成功完成这轮，就清除定时器
    }
  }
}

main().catch(console.error);
