require('dotenv').config();
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

const TRANSFER_TOPIC = ethers.id("Transfer(address,address,uint256)");
const POOL_CREATED_TOPIC = ethers.id("PoolCreated(address,address,uint24,int24,address)");
const SWAP_TOPIC = ethers.id("Swap(address,uint256,uint256,uint256,uint256,address)");
const SYNC_TOPIC = ethers.id("Sync(uint112,uint112)");

const provider = new ethers.JsonRpcProvider(RPC_URL);

const MAIN_TOKENS = {
  "0x55d398326f99059fF775485246999027B3197955": 1,        // USDT
  "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d": 1,        // USDC
  "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c": 700,      // WBNB
  "0xe9e7cea3dedca5984780bafc599bd69add087d56": 1,        // BUSD
  "0x2170ed0880ac9a755fd29b2688956bd959f933f8": 3500,     // ETH
  "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3": 1         // DAI
};

// PancakeSwap V3 Factory 地址（BSC）
const V3_FACTORY = "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865".toLowerCase();

const ERC20_ABI = [
  "function name() view returns (string)",
  "function symbol() view returns (string)",
  "function decimals() view returns (uint8)",
  "function totalSupply() view returns (uint256)",
  "function balanceOf(address) view returns (uint256)"
];

const V2_LP_ABI = [
  "function token0() view returns (address)",
  "function token1() view returns (address)"
];

// ✅ 更新 .env 中的 START_BLOCK
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

    if (!updated) {
      newEnvData.push(`START_BLOCK=${newBlock}`);
    }

    fs.writeFileSync(ENV_PATH, newEnvData.join('\n'));
    console.log(`✅ 已更新 .env 中的 START_BLOCK=${newBlock}`);
  } catch (err) {
    console.error('❌ 更新 .env 文件失败:', err.message);
  }
}

// ✅ 判断是否包含主流币
function containsMainToken(token0, token1) {
  if (!token0 || !token1) return false;
  return MAIN_TOKENS[token0.toLowerCase()] || MAIN_TOKENS[token1.toLowerCase()];
}

// ✅ 根据 token 地址和数量换算为 USDT
function getUSDTValue(tokenAddress, amount, decimals) {
  const rate = MAIN_TOKENS[tokenAddress.toLowerCase()] || 0;
  const humanReadableAmount = parseFloat(ethers.formatUnits(amount.toString(), decimals));
  return humanReadableAmount * rate;
}

// ✅ 母币逻辑函数
function getParentToken(token0, token1) {
    const t0Main = MAIN_TOKENS[token0.toLowerCase()];
    const t1Main = MAIN_TOKENS[token1.toLowerCase()];

    // 如果有非主流币，直接返回那个
    if (!t0Main) return token0;
    if (!t1Main) return token1;

    // 两个都是主流币，优先取非USDT
    const usdtAddr = "0x55d398326f99059fF775485246999027B3197955".toLowerCase();
    if (token0.toLowerCase() === usdtAddr) return token1;
    if (token1.toLowerCase() === usdtAddr) return token0;

    // 如果都不是USDT（例如 WBNB-ETH），默认取 token0
    return token0;
}

// 判断主流币
function getMainToken(token0, token1, parentToken) {
    return (token0.toLowerCase() === parentToken.toLowerCase()) ? token1 : token0;
}

async function main() {
  console.log("✅ 开始批量同步数据...");
  const provider = new ethers.JsonRpcProvider(RPC_URL);
  const db = await mysql.createPool({
    host: DB_HOST,
    user: DB_USER,
    password: DB_PASS,
    database: DB_NAME,
    connectionLimit: 10
  });

  const latestBlock = await provider.getBlockNumber();
  console.log(`当前区块高度: ${latestBlock}`);

  let fromBlock = parseInt(START_BLOCK);

  while (fromBlock <= latestBlock) {
    const toBlock = Math.min(fromBlock + BATCH_SIZE - 1, latestBlock);
    console.log(`\n⛏ 正在处理区块范围: ${fromBlock} - ${toBlock}`);

    try {
      // 1. Transfer 事件
      const transferLogs = await provider.getLogs({
        fromBlock,
        toBlock,
        topics: [TRANSFER_TOPIC]
      });

      // 2. V3 PoolCreated 事件
      const v3Logs = await provider.getLogs({
        fromBlock,
        toBlock,
        address: V3_FACTORY,
        topics: [POOL_CREATED_TOPIC]
      });

      // 3. V2 LP Swap 和 Sync 事件
      const swapAndSyncLogs = await provider.getLogs({
        fromBlock,
        toBlock,
        topics: [[SWAP_TOPIC, SYNC_TOPIC]] // OR 条件
      });

      console.log(`Transfer: ${transferLogs.length}, V3: ${v3Logs.length}, V2事件: ${swapAndSyncLogs.length}`);

      // ✅ 处理 Transfer
      for (const log of transferLogs) {
        const contractAddress = log.address.toLowerCase();
        await updateTokenStats(db, provider, contractAddress);
      }

      // ✅ 处理 V3 PoolCreated
      for (const log of v3Logs) {
        const token0 = ethers.getAddress("0x" + log.topics[1].slice(26));
        const token1 = ethers.getAddress("0x" + log.topics[2].slice(26));
        const fee = parseInt(log.topics[3], 16);
        const [tickSpacingBig, pool] = ethers.AbiCoder.defaultAbiCoder().decode(
          ["int256", "address"],
          log.data
        );
        const poolAddress = pool.toLowerCase();
        console.log(`V3 PoolCreated: token0=${token0}, token1=${token1}, pool=${poolAddress}`);
        if (containsMainToken(token0, token1)) {
          await insertV3Pool(db, provider, poolAddress, token0, token1);
        } else {
          console.log(`⏭ 跳过非主流币V3池: ${poolAddress}`);
        }
      }

      // ✅ 处理 V2 Swap 和 Sync
      for (const log of swapAndSyncLogs) {
        const lpAddress = log.address.toLowerCase();

        // 如果 LP 不存在数据库，先插入
        await ensureV2Pool(db, provider, lpAddress);

        // Sync：更新储备
        if (log.topics[0] === SYNC_TOPIC) {
          const [reserve0, reserve1] = ethers.AbiCoder.defaultAbiCoder().decode(
            ["uint112", "uint112"],
            log.data
          );
          await updateReserves(db, lpAddress, reserve0, reserve1);
        }

        // Swap：更新交易量和交易次数
        if (log.topics[0] === SWAP_TOPIC) {
          const [rows] = await db.query("SELECT token0_address, token1_address FROM token_stats WHERE contract_address=?", [lpAddress]);
          if (rows.length === 0) continue;

          const token0 = rows[0].token0_address;
          const token1 = rows[0].token1_address;
          if (!containsMainToken(token0, token1)) continue;

          const [amount0In, amount1In, amount0Out, amount1Out] = ethers.AbiCoder.defaultAbiCoder().decode(
            ["uint256", "uint256", "uint256", "uint256"], log.data
          );

          const tokenContract0 = new ethers.Contract(token0, ERC20_ABI, provider);
          const tokenContract1 = new ethers.Contract(token1, ERC20_ABI, provider);
          const [dec0, dec1] = await Promise.all([tokenContract0.decimals(), tokenContract1.decimals()]);

          let usdtVolume = 0;
          if (MAIN_TOKENS[token0.toLowerCase()]) {
            usdtVolume += getUSDTValue(token0, BigInt(amount0In) + BigInt(amount0Out), dec0);
          }
          if (MAIN_TOKENS[token1.toLowerCase()]) {
            usdtVolume += getUSDTValue(token1, BigInt(amount1In) + BigInt(amount1Out), dec1);
          }

          await updateTradeVolume(db, lpAddress, usdtVolume);
        }
      }

    } catch (err) {
      console.error(`❌ 区块 ${fromBlock}-${toBlock} 出错: ${err.message}`);
      console.log(`等待 10 秒重试...`);
      await new Promise(res => setTimeout(res, 10000));
      continue;
    }

    fromBlock = toBlock + 1;
    updateStartBlock(fromBlock);
  }

  console.log("✅ 同步完成");
}

// ✅ 保留 ensureV2Pool，并增加主流币判断
async function ensureV2Pool(db, provider, lpAddress) {
  const [rows] = await db.query("SELECT id FROM token_stats WHERE contract_address = ?", [lpAddress]);
  if (rows.length > 0) return true;

  const v2Info = await checkV2LP(provider, lpAddress);
  
  // 检查是否为LP，且 token0 和 token1 都存在
    if (!v2Info.isLP || !v2Info.token0 || !v2Info.token1) {
      console.warn(`⚠️ LP检查失败或token0/token1为空，跳过: ${lpAddress}`);
      return false;
    }
  
  if (!v2Info.isLP || !containsMainToken(v2Info.token0, v2Info.token1)) return false;

  const { token0_balance, token1_balance } = await getTokenBalances(provider, lpAddress, v2Info.token0, v2Info.token1);
  const parentToken = getParentToken(v2Info.token0, v2Info.token1);
  const parentInfo = await fetchTokenInfo(provider, parentToken);
  
  const mainTokenAddr = getMainToken(v2Info.token0, v2Info.token1, parentToken);
  const mainTokenInfo = await fetchTokenInfo(provider, mainTokenAddr);

  await db.query(`
    INSERT INTO token_stats (
      contract_address, parent_token_address, token_name, token_symbol, decimals, total_supply,
      transfer_count, is_lp, lp_version, token0_address, token1_address, token0_balance, token1_balance, main_token
    ) VALUES (?, ?, ?, ?, ?, ?, 0, 1, 'V2', ?, ?, ?, ?, ?)
  `, [
    lpAddress, parentToken,
    parentInfo.name || 'Unknown', parentInfo.symbol || 'UNK', parentInfo.decimals || 18, parentInfo.totalSupplyReadable || '0',
    v2Info.token0, v2Info.token1, token0_balance, token1_balance, mainTokenInfo.name || 'Unknown'
  ]);
  
  console.log(`✅ 新V2 LP插入: ${lpAddress}`);
  return true; // ✅ 补上这个
}

// 更新普通代币或 V2 LP 基本信息
async function updateTokenStats(db, provider, contractAddress) {
  try {
    const [rows] = await db.query("SELECT id, transfer_count FROM token_stats WHERE contract_address = ?", [contractAddress]);

    if (rows.length === 0) {
      // 原逻辑：新增代币记录
      const { name, symbol, decimals, totalSupplyReadable } = await fetchTokenInfo(provider, contractAddress);

      let isLP = 0, lpVersion = null, token0Addr = null, token1Addr = null, token0Bal = null, token1Bal = null;

      if ((symbol && symbol.toLowerCase().includes('lp')) || (name && name.toLowerCase().includes('lp'))) {
        const v2Result = await checkV2LP(provider, contractAddress);
        if (v2Result.isLP) {
          isLP = 1;
          lpVersion = 'V2';
          token0Addr = v2Result.token0;
          token1Addr = v2Result.token1;

          try {
            const balances = await getTokenBalances(provider, contractAddress, token0Addr, token1Addr);
            token0Bal = balances.token0_balance;
            token1Bal = balances.token1_balance;
          } catch (err) {
            console.warn(`⚠️ 无法获取 LP 余额: ${contractAddress}`);
          }
        }
      }

      await db.query(`
        INSERT INTO token_stats (
          contract_address, token_name, token_symbol, decimals, total_supply, transfer_count,
          is_lp, lp_version, token0_address, token1_address, token0_balance, token1_balance
        ) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?)
      `, [contractAddress, name || 'Unknown', symbol || 'UNK', decimals, totalSupplyReadable,
          isLP, lpVersion, token0Addr, token1Addr, token0Bal, token1Bal]);

      console.log(`✅ 新代币: ${contractAddress} | 名称: ${name || 'Unknown'} | LP: ${isLP ? lpVersion : '否'}`);
    } else {
      await db.query(`
        UPDATE token_stats SET transfer_count = transfer_count + 1 WHERE contract_address = ?
      `, [contractAddress]);

      // ✅ 新增逻辑：如果这个地址也在 parent_token_address 中，直接同步
      const [parentRows] = await db.query(`
        SELECT contract_address FROM token_stats WHERE parent_token_address = ?
      `, [contractAddress]);

      if (parentRows.length > 0) {
        const newTransferCount = rows[0].transfer_count + 1;
        for (const row of parentRows) {
          await db.query(`
            UPDATE token_stats SET transfer_count = ? WHERE contract_address = ?
          `, [newTransferCount, row.contract_address]);
        }
        console.log(`🔄 母币 ${contractAddress} 的 LP 转账次数同步完成`);
      }
    }
  } catch (err) {
    console.error(`❌ 数据库写入失败: ${err.message}`);
  }
}

// 插入 V3 池子（只插入包含主流币的池）
async function insertV3Pool(db, provider, poolAddress, token0, token1) {
  try {
    // 空值安全检查
    if (!token0 || !token1) {
      console.warn(`⚠️ token0 或 token1 为空，跳过插入: ${poolAddress}`);
      return;
    }

    // 检查是否包含主流币
    if (!containsMainToken(token0, token1)) {
      console.log(`⏭ 非主流币V3池，跳过: ${poolAddress}`);
      return;
    }

    // 如果已存在则跳过
    const [rows] = await db.query("SELECT id FROM token_stats WHERE contract_address = ?", [poolAddress]);
    if (rows.length > 0) return;

    // 获取 token0 和 token1 的余额
    const { token0_balance, token1_balance } = await getTokenBalances(provider, poolAddress, token0, token1);
    const parentToken = getParentToken(token0, token1);
    const parentInfo = await fetchTokenInfo(provider, parentToken);
    const mainTokenAddr = getMainToken(token0, token1, parentToken);
    const mainTokenInfo = await fetchTokenInfo(provider, mainTokenAddr);

    // ✅ 插入数据库（用母币信息覆盖 LP 占位信息）
    await db.query(`
      INSERT INTO token_stats (
        contract_address, parent_token_address, token_name, token_symbol, decimals, total_supply,
        transfer_count, is_lp, lp_version, token0_address, token1_address, token0_balance, token1_balance, main_token
      ) VALUES (?, ?, ?, ?, ?, ?, 0, 1, 'V3', ?, ?, ?, ?, ?)
    `, [
      poolAddress, parentToken,
      parentInfo.name || 'Unknown', parentInfo.symbol || 'UNK', parentInfo.decimals || 18, parentInfo.totalSupplyReadable || '0',
      token0, token1, token0_balance, token1_balance, mainTokenInfo.name || 'Unknown'
    ]);

    console.log(`✅ 新V3池: ${poolAddress} | token0: ${token0} | token1: ${token1}`);
  } catch (err) {
    console.error(`❌ V3池插入失败: ${err.message}`);
  }
}

// 更新 V2 储备
async function updateReserves(db, lpAddress, reserve0, reserve1) {
    try {
        // 1. 查询 token0 和 token1 地址
        const [rows] = await db.query(
            "SELECT token0_address, token1_address FROM token_stats WHERE contract_address=?",
            [lpAddress]
        );
        if (rows.length === 0) return;

        const token0 = rows[0].token0_address;
        const token1 = rows[0].token1_address;

        // 2. 获取 token0 和 token1 的 decimals
        const tokenContract0 = new ethers.Contract(token0, ERC20_ABI, provider);
        const tokenContract1 = new ethers.Contract(token1, ERC20_ABI, provider);
        const [dec0, dec1] = await Promise.all([
            tokenContract0.decimals(),
            tokenContract1.decimals()
        ]);

        // 3. 转换为人类可读余额
        const balance0 = parseFloat(ethers.formatUnits(reserve0.toString(), dec0));
        const balance1 = parseFloat(ethers.formatUnits(reserve1.toString(), dec1));

        // 4. 计算 USDT 计价资金池价值
        let liquidityValue = 0;
        if (MAIN_TOKENS[token0.toLowerCase()]) {
            liquidityValue += balance0 * MAIN_TOKENS[token0.toLowerCase()];
        }
        if (MAIN_TOKENS[token1.toLowerCase()]) {
            liquidityValue += balance1 * MAIN_TOKENS[token1.toLowerCase()];
        }

        // 5. 更新数据库
        await db.query(`
            UPDATE token_stats
            SET token0_balance=?, token1_balance=?, liquidity_value=?, last_updated=NOW()
            WHERE contract_address=?
        `, [balance0, balance1, liquidityValue, lpAddress]);

        console.log(`✅ 更新储备 & 池子价值: ${lpAddress} | 价值 ≈ ${liquidityValue} USD`);
    } catch (err) {
        console.error(`❌ 更新储备失败: ${err.message}`);
    }
}

// 更新交易量和交易次数
async function updateTradeVolume(db, lpAddress, usdtVolume) {
  try {
    await db.query(`
      UPDATE token_stats
      SET total_transaction_volume = total_transaction_volume + ?,
          trade_count_24h = trade_count_24h + 1,
          trade_count_12h = trade_count_12h + 1
      WHERE contract_address = ?
    `, [usdtVolume, lpAddress]);
  } catch (err) {
    console.error(`❌ 更新交易量失败: ${err.message}`);
  }
}

// 工具函数
async function fetchTokenInfo(provider, address) {
  const contract = new ethers.Contract(address, ERC20_ABI, provider);
  let name = null, symbol = null, decimals = 18, totalSupplyReadable = "0";

  try { name = await contract.name(); } catch {}
  try { symbol = await contract.symbol(); } catch {}
  try { decimals = await contract.decimals(); } catch {}
  try {
    const rawSupply = await contract.totalSupply();
    totalSupplyReadable = ethers.formatUnits(rawSupply, decimals);
  } catch {}

  return { name, symbol, decimals, totalSupplyReadable };
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

async function getTokenBalances(provider, poolAddress, token0, token1) {
  try {
    const tokenContract0 = new ethers.Contract(token0, ERC20_ABI, provider);
    const tokenContract1 = new ethers.Contract(token1, ERC20_ABI, provider);

    const [dec0, dec1, bal0, bal1] = await Promise.all([
      tokenContract0.decimals(),
      tokenContract1.decimals(),
      tokenContract0.balanceOf(poolAddress),
      tokenContract1.balanceOf(poolAddress)
    ]);

    return {
      token0_balance: ethers.formatUnits(bal0, dec0),
      token1_balance: ethers.formatUnits(bal1, dec1)
    };
  } catch (err) {
    console.warn(`⚠️ 获取余额失败: pool=${poolAddress}, token0=${token0}, token1=${token1}, 错误=${err.message}`);
    return { token0_balance: "0", token1_balance: "0" };
  }
}

main().catch(console.error);
