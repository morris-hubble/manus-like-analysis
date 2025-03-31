/**
 * 代币交易分析与可视化脚本
 * 读取并分析DEX代币交易数据，生成分析报告和交互式图表
 */

// ==== 导入必要的库 ====
const fs = require('fs');
const Papa = require('papaparse');
const _ = require('lodash');
const path = require('path');

// ==== 配置参数 ====
// 指定CSV文件路径 (可以通过命令行参数覆盖)
let csvFilePath = 'dex_token_trade_transaction_202503311957.csv';  // 修改为你实际的CSV文件名
// 输出文件路径
const REPORT_PATH = 'analysis_report.md';
const CHART_PATH = 'charts.html';

// 检查命令行参数，允许用户指定CSV文件路径
if (process.argv.length > 2) {
    csvFilePath = process.argv[2];
}

console.log(`开始分析 ${csvFilePath} 文件...`);

// 阈值设置
const WHALE_THRESHOLD = 10000; // $10,000 巨鲸交易阈值
const MEDIUM_THRESHOLD = 1000;  // $1,000 中等交易阈值
const RETAIL_THRESHOLD = 100;   // $100 散户交易阈值

// ==== 数据读取与预处理 ====
console.log("正在读取并解析CSV数据...");

// 读取CSV文件
const csvFile = fs.readFileSync(csvFilePath, 'utf-8');
const { data } = Papa.parse(csvFile, { 
    header: true, 
    dynamicTyping: true,
    skipEmptyLines: true
});

console.log(`成功读取 ${data.length} 条交易记录`);

// 辅助函数：转换为UTC+8时区
function convertToUTC8(timestamp) {
    const date = new Date(timestamp * 1000);
    const utc8Date = new Date(date.getTime() + 8 * 60 * 60 * 1000);
    return utc8Date.toISOString(); // 使用ISO格式
}

// 辅助函数：验证时间戳是否有效
const isValidTimestamp = (timestamp) => {
    // 检查是否为有效数字
    if (typeof timestamp !== 'number' || isNaN(timestamp)) {
        return false;
    }
    
    // 检查是否在合理的JavaScript日期范围内
    // JavaScript Date支持的最小和最大时间戳 (-8.64e15到8.64e15)
    if (timestamp < -8640000000000000 || timestamp > 8640000000000000) {
        return false;
    }
    
    // 进一步验证：确保可以创建有效的Date对象
    const date = new Date(timestamp);
    return date instanceof Date && !isNaN(date.getTime());
};

// 创建一个安全的日期处理函数
function createSafeDate(timestamp) {
    try {
        if (timestamp === undefined || timestamp === null) {
            console.log("警告: 时间戳为undefined或null");
            return null;
        }
        
        // 支持UNIX秒级时间戳和毫秒级时间戳
        let ts = timestamp;
        if (typeof timestamp === 'number') {
            // 如果时间戳太小，可能是以秒为单位
            if (timestamp < 10000000000) { // 判断是秒还是毫秒
                ts = timestamp * 1000;
            }
        } else if (typeof timestamp === 'string') {
            ts = parseInt(timestamp, 10);
            if (isNaN(ts)) {
                console.log(`警告: 无法解析时间戳字符串: ${timestamp}`);
                return null;
            }
            if (ts < 10000000000) {
                ts = ts * 1000;
            }
        }
        
        const date = new Date(ts);
        if (!(date instanceof Date) || isNaN(date.getTime())) {
            console.log(`警告: 创建的日期对象无效: ${timestamp}`);
            return null;
        }
        
        return date;
    } catch (err) {
        console.log(`警告: 创建日期时出错: ${timestamp}`, err);
        return null;
    }
}

// 预处理数据
const processedData = data.map(row => {
    // 添加人类可读的日期/时间
    if (row.trade_timestamp) {
        row.dateTime = new Date(row.trade_timestamp * 1000);
    }
  
    // 计算交易价值
    if (row.type === 'TOKEN_BUY' && row.buy_price && row.buy_amount) {
        row.transaction_value = row.buy_price * row.buy_amount;
    } else if (row.type === 'TOKEN_SELL' && row.sell_price && row.sell_amount) {
        row.transaction_value = row.sell_price * row.sell_amount;
    } else {
        row.transaction_value = 0;
    }
    
    // 检查数据字段
    if (!row.trade_timestamp || !row.type || (!row.buy_amount && !row.sell_amount)) {
        console.log('发现无效数据行:', row);
        return null;
    }
    
    const price = row.type === 'TOKEN_BUY' ? parseFloat(row.buy_price) : parseFloat(row.sell_price);
    
    // 记录异常价格
    if (price > 1000 || price < 0.00000001) {
        console.log('发现异常价格:', {
            时间: convertToUTC8(row.trade_timestamp),
            价格: price,
            类型: row.type,
            交易签名: row.transaction_signature
        });
    }
    
    return {
        ...row,
        trade_timestamp: parseInt(row.trade_timestamp),
        buy_amount: row.type === 'TOKEN_BUY' ? parseFloat(row.buy_amount) || 0 : 0,
        sell_amount: row.type === 'TOKEN_SELL' ? parseFloat(row.sell_amount) || 0 : 0,
        price: price
    };
}).filter(row => row !== null);

// 按时间戳排序
const sortedData = _.sortBy(processedData, 'trade_timestamp');

// 检查处理后的数据
console.log('处理后记录数:', sortedData.length);
console.log('时间范围:', 
    convertToUTC8(sortedData[0].trade_timestamp),
    '至',
    convertToUTC8(sortedData[sortedData.length - 1].trade_timestamp)
);

// 找出最高和最低价格
const priceStats = sortedData.reduce((stats, row) => {
    if (row.price) {
        if (!stats.max || row.price > stats.max.price) {
            stats.max = {
                price: row.price,
                time: convertToUTC8(row.trade_timestamp)
            };
        }
        if (!stats.min || row.price < stats.min.price) {
            stats.min = {
                price: row.price,
                time: convertToUTC8(row.trade_timestamp)
            };
        }
    }
    return stats;
}, { max: null, min: null });

console.log('价格统计:', {
    最高价: priceStats.max,
    最低价: priceStats.min,
    价格波动倍数: priceStats.max && priceStats.min ? priceStats.max.price / priceStats.min.price : 'N/A'
});

// ====== 第一部分：交易分析 ======
console.log("正在进行交易数据分析...");

// ==== 钱包活动分析 ====
// 基于交易量对钱包进行分类
const walletActivity = {};
sortedData.forEach(row => {
  if (!row.trader_wallet_address) return;
  
  if (!walletActivity[row.trader_wallet_address]) {
    walletActivity[row.trader_wallet_address] = {
      buys: { count: 0, volume: 0, value: 0 },
      sells: { count: 0, volume: 0, value: 0 },
      totalTransactions: 0,
      firstSeen: row.trade_timestamp,
      lastSeen: row.trade_timestamp,
      netSOLChange: 0
    };
  }
  
  const wallet = walletActivity[row.trader_wallet_address];
  
  // 更新首次和最后一次出现时间
  wallet.firstSeen = Math.min(wallet.firstSeen, row.trade_timestamp);
  wallet.lastSeen = Math.max(wallet.lastSeen, row.trade_timestamp);
  
  // 更新交易次数和交易量
  if (row.type === 'TOKEN_BUY') {
    wallet.buys.count++;
    wallet.buys.volume += row.buy_amount || 0;
    wallet.buys.value += row.transaction_value || 0;
  } else if (row.type === 'TOKEN_SELL') {
    wallet.sells.count++;
    wallet.sells.volume += row.sell_amount || 0;
    wallet.sells.value += row.transaction_value || 0;
  }
  
  wallet.totalTransactions++;
  wallet.netSOLChange += row.net_sol_balance_change || 0;
});

// ==== 识别可疑的巨鲸/操控者钱包 ====
const suspectedManipulators = Object.entries(walletActivity)
  .map(([address, activity]) => {
    const totalValue = activity.buys.value + activity.sells.value;
    const buyToSellRatio = activity.buys.count > 0 && activity.sells.count > 0 ? 
      activity.buys.count / activity.sells.count : 
      (activity.buys.count > 0 ? Infinity : 0);
    
    // 计算活跃时长(小时)
    const timeActiveHours = (activity.lastSeen - activity.firstSeen) / 3600;
    
    // 计算交易频率(每小时)
    const transactionFrequency = timeActiveHours > 0 ? 
      activity.totalTransactions / timeActiveHours : 
      activity.totalTransactions;
    
    return {
      address,
      totalValue,
      buyToSellRatio,
      transactionCount: activity.totalTransactions,
      timeActiveHours,
      transactionFrequency,
      netSOLChange: activity.netSOLChange,
      buys: activity.buys,
      sells: activity.sells,
      suspiciousScore: 0 // 后续计算
    };
  });

// 基于可疑行为对钱包进行评分
suspectedManipulators.forEach(wallet => {
  let score = 0;
  
  // 高交易价值
  if (wallet.totalValue > WHALE_THRESHOLD * 10) score += 5;
  else if (wallet.totalValue > WHALE_THRESHOLD) score += 3;
  
  // 买卖比率极度倾斜
  if (wallet.buyToSellRatio > 10 || wallet.buyToSellRatio < 0.1) score += 3;
  
  // 高交易频率
  if (wallet.transactionFrequency > 10) score += 3;
  else if (wallet.transactionFrequency > 5) score += 2;
  
  // 短时间内大量交易
  if (wallet.timeActiveHours < 1 && wallet.transactionCount > 20) score += 4;
  
  // 获利
  if (wallet.netSOLChange > 10) score += 3;
  
  wallet.suspiciousScore = score;
});

// 按可疑分数排序
const topSuspiciousWallets = suspectedManipulators
  .sort((a, b) => b.suspiciousScore - a.suspiciousScore)
  .slice(0, 20);

console.log("识别出前5个可疑钱包:", topSuspiciousWallets.slice(0, 5));

// ==== 价格行为分析 ====
// 按10分钟时间间隔对交易进行分组以更清晰的查看
const timeIntervals = _.groupBy(sortedData, row => 
  Math.floor(row.trade_timestamp / 600) * 600
);

const priceByInterval = Object.entries(timeIntervals).map(([timestamp, transactions]) => {
  const buys = transactions.filter(tx => tx.type === 'TOKEN_BUY');
  const sells = transactions.filter(tx => tx.type === 'TOKEN_SELL');
  
  // 获取区间的平均价格
  const avgBuyPrice = buys.length > 0 ? 
    _.meanBy(buys.filter(tx => tx.buy_price), 'buy_price') : null;
  
  const avgSellPrice = sells.length > 0 ? 
    _.meanBy(sells.filter(tx => tx.sell_price), 'sell_price') : null;
  
  // 计算总交易量
  const buyVolume = _.sumBy(buys, 'buy_amount');
  const sellVolume = _.sumBy(sells, 'sell_amount');
  
  // 计算交易价值
  const buyValue = _.sumBy(buys, 'transaction_value');
  const sellValue = _.sumBy(sells, 'transaction_value');
  
  return {
    timestamp: parseInt(timestamp),
    datetime: new Date(parseInt(timestamp) * 1000).toISOString(),
    avgBuyPrice,
    avgSellPrice, 
    price: avgBuyPrice || avgSellPrice, // 使用可用的价格
    buyCount: buys.length,
    sellCount: sells.length,
    buyVolume,
    sellVolume,
    buyValue,
    sellValue,
    totalTransactions: transactions.length
  };
});

// 按时间戳排序
const sortedPriceIntervals = priceByInterval
  .filter(interval => interval.price) // 只保留有价格数据的区间
  .sort((a, b) => a.timestamp - b.timestamp);

// ==== 识别区间之间的重大价格变化 ====
const priceChanges = [];
for (let i = 1; i < sortedPriceIntervals.length; i++) {
  const prev = sortedPriceIntervals[i-1];
  const curr = sortedPriceIntervals[i];
  
  if (prev.price && curr.price) {
    const percentChange = ((curr.price - prev.price) / prev.price) * 100;
    
    if (Math.abs(percentChange) > 5) { // 只记录重大变化(>5%)
      priceChanges.push({
        startTimestamp: prev.timestamp,
        endTimestamp: curr.timestamp,
        startDatetime: prev.datetime,
        endDatetime: curr.datetime,
        startPrice: prev.price,
        endPrice: curr.price,
        percentChange,
        buyVolume: curr.buyVolume,
        sellVolume: curr.sellVolume,
        isSignificant: Math.abs(percentChange) > 10
      });
    }
  }
}

console.log("重大价格变动:", priceChanges.filter(change => change.isSignificant).slice(0, 5));

// ==== 识别潜在的拉高出货模式 ====
// 寻找: 1) 价格大幅上涨, 2) 高散户买入, 3) 巨鲸卖出
const pumpAndDumpPatterns = [];

// 对于每次价格拉升...
priceChanges
  .filter(change => change.percentChange > 10) // 专注于价格拉升(>10%)
  .forEach(pump => {
    const pumpStart = pump.startTimestamp;
    const pumpEnd = pump.endTimestamp;
    
    // 检查拉升期间的散户活动
    const retailBuysDuringPump = sortedData.filter(tx => 
      tx.type === 'TOKEN_BUY' && 
      tx.trade_timestamp >= pumpStart && 
      tx.trade_timestamp <= pumpEnd &&
      tx.transaction_value > 0 && 
      tx.transaction_value <= RETAIL_THRESHOLD
    );
    
    // 寻找拉升后的巨鲸卖出(30分钟内)
    const whaleSellsAfterPump = sortedData.filter(tx => 
      tx.type === 'TOKEN_SELL' && 
      tx.trade_timestamp > pumpEnd && 
      tx.trade_timestamp <= pumpEnd + 1800 && // 30分钟窗口
      tx.transaction_value >= MEDIUM_THRESHOLD
    );
    
    if (retailBuysDuringPump.length > 5 && whaleSellsAfterPump.length > 0) {
      pumpAndDumpPatterns.push({
        pump,
        retailBuysCount: retailBuysDuringPump.length,
        retailBuysValue: _.sumBy(retailBuysDuringPump, 'transaction_value'),
        whaleSellsCount: whaleSellsAfterPump.length,
        whaleSellsValue: _.sumBy(whaleSellsAfterPump, 'transaction_value'),
        suspiciousWallets: [...new Set(whaleSellsAfterPump.map(tx => tx.trader_wallet_address))]
      });
    }
  });

console.log("潜在拉高出货模式:", pumpAndDumpPatterns.slice(0, 5));

// ==== 分析基于时间的市场操纵行为 - 寻找协调活动 ====
// 按5分钟间隔对交易进行分组
const detailedTimeIntervals = _.groupBy(sortedData, row => 
  Math.floor(row.trade_timestamp / 300) * 300
);

// 寻找具有可疑活动模式的时间间隔
const suspiciousActivityIntervals = Object.entries(detailedTimeIntervals)
  .map(([timestamp, transactions]) => {
    const timestamp_num = parseInt(timestamp);
    const datetime = new Date(timestamp_num * 1000).toISOString();
    
    // 计算独立钱包数
    const uniqueWallets = new Set(transactions.map(tx => tx.trader_wallet_address));
    
    // 统计大额交易
    const largeTransactions = transactions.filter(tx => tx.transaction_value >= MEDIUM_THRESHOLD);
    
    // 检测洗盘交易(同一钱包快速买卖)
    const walletActions = {};
    transactions.forEach(tx => {
      if (!tx.trader_wallet_address) return;
      
      if (!walletActions[tx.trader_wallet_address]) {
        walletActions[tx.trader_wallet_address] = { buys: 0, sells: 0 };
      }
      
      if (tx.type === 'TOKEN_BUY') {
        walletActions[tx.trader_wallet_address].buys++;
      } else if (tx.type === 'TOKEN_SELL') {
        walletActions[tx.trader_wallet_address].sells++;
      }
    });
    
    const potentialWashTraders = Object.entries(walletActions)
      .filter(([_, actions]) => actions.buys > 0 && actions.sells > 0)
      .map(([address]) => address);
    
    // 计算买卖比率
    const buys = transactions.filter(tx => tx.type === 'TOKEN_BUY');
    const sells = transactions.filter(tx => tx.type === 'TOKEN_SELL');
    const buyToSellRatio = sells.length > 0 ? buys.length / sells.length : (buys.length > 0 ? Infinity : 0);
    
    // 检查协调的巨鲸活动
    const whaleTransactions = transactions.filter(tx => tx.transaction_value >= WHALE_THRESHOLD);
    const uniqueWhaleWallets = new Set(whaleTransactions.map(tx => tx.trader_wallet_address));
    
    // 计算可疑评分
    let suspiciousScore = 0;
    
    // 高交易频率但很少的独立钱包
    if (transactions.length > 20 && uniqueWallets.size < 5) {
      suspiciousScore += 3;
    }
    
    // 潜在洗盘交易
    if (potentialWashTraders.length > 0) {
      suspiciousScore += potentialWashTraders.length * 2;
    }
    
    // 买卖比率极度倾斜
    if (buyToSellRatio > 10 || buyToSellRatio < 0.1) {
      suspiciousScore += 2;
    }
    
    // 多笔巨鲸交易
    if (whaleTransactions.length > 3) {
      suspiciousScore += 3;
    }
    
    return {
      timestamp: timestamp_num,
      datetime,
      totalTransactions: transactions.length,
      uniqueWallets: uniqueWallets.size,
      transactionsPerWallet: transactions.length / (uniqueWallets.size || 1),
      largeTransactionsCount: largeTransactions.length,
      buyCount: buys.length,
      sellCount: sells.length,
      buyToSellRatio,
      potentialWashTraders,
      washTradingCount: potentialWashTraders.length,
      whaleTransactionsCount: whaleTransactions.length,
      uniqueWhaleWallets: uniqueWhaleWallets.size,
      suspiciousScore
    };
  })
  .filter(interval => interval.suspiciousScore > 0)
  .sort((a, b) => b.suspiciousScore - a.suspiciousScore);

console.log("可疑活动时间区间:", suspiciousActivityIntervals.slice(0, 5));

// ==== 寻找最高和最低价格 ====
const sortedPrices = sortedData
  .filter(row => row.buy_price || row.sell_price)
  .map(row => ({
    timestamp: row.trade_timestamp,
    datetime: row.dateTime,
    price: row.buy_price || row.sell_price
  }))
  .sort((a, b) => b.price - a.price);

const highestPrice = sortedPrices[0];
const lowestPrice = sortedPrices[sortedPrices.length - 1];

console.log("最高价格:", highestPrice);
console.log("最低价格:", lowestPrice);

// ==== 计算顶级钱包的市场影响 ====
const totalTransactionValue = _.sumBy(sortedData, 'transaction_value');
const topWalletsImpact = topSuspiciousWallets.map(wallet => {
  const walletTransactions = sortedData.filter(tx => tx.trader_wallet_address === wallet.address);
  const walletValue = _.sumBy(walletTransactions, 'transaction_value');
  const marketImpact = (walletValue / totalTransactionValue) * 100;
  
  return {
    ...wallet,
    marketImpact
  };
});

console.log("顶级钱包市场影响:", topWalletsImpact.slice(0, 5));

// ==== 最终分析: 识别价格行动的关键转折点 ====
// 并关联钱包活动
const sortedTransactions = _.sortBy(sortedData, 'trade_timestamp');
const earliestTimestamp = sortedTransactions[0].trade_timestamp;
const latestTimestamp = sortedTransactions[sortedTransactions.length - 1].trade_timestamp;

// 将时间范围划分为6个周期
const periodDuration = Math.floor((latestTimestamp - earliestTimestamp) / 6);
const periods = [];

for (let i = 0; i < 6; i++) {
  const startTime = earliestTimestamp + (i * periodDuration);
  const endTime = earliestTimestamp + ((i + 1) * periodDuration);
  
  const periodTransactions = sortedData.filter(tx => 
    tx.trade_timestamp >= startTime && tx.trade_timestamp < endTime
  );
  
  const buyTransactions = periodTransactions.filter(tx => tx.type === 'TOKEN_BUY');
  const sellTransactions = periodTransactions.filter(tx => tx.type === 'TOKEN_SELL');
  
  const avgPrice = periodTransactions.length > 0 ? 
    _.meanBy(
      periodTransactions.filter(tx => tx.buy_price || tx.sell_price), 
      tx => tx.buy_price || tx.sell_price
    ) : null;
  
  const priceAtStart = periodTransactions.length > 0 ? 
    (periodTransactions[0].buy_price || periodTransactions[0].sell_price) : null;
  
  const priceAtEnd = periodTransactions.length > 0 ? 
    (periodTransactions[periodTransactions.length - 1].buy_price || 
     periodTransactions[periodTransactions.length - 1].sell_price) : null;
  
  // 计算活跃钱包
  const activeWallets = new Set(periodTransactions.map(tx => tx.trader_wallet_address));
  
  // 计算巨鲸活动
  const whaleTransactions = periodTransactions.filter(tx => tx.transaction_value >= WHALE_THRESHOLD);
  const whaleWallets = new Set(whaleTransactions.map(tx => tx.trader_wallet_address));
  
  periods.push({
    periodNumber: i + 1,
    startTime,
    endTime,
    startTimeFormatted: new Date(startTime * 1000).toISOString(),
    endTimeFormatted: new Date(endTime * 1000).toISOString(),
    transactionCount: periodTransactions.length,
    buyCount: buyTransactions.length,
    sellCount: sellTransactions.length,
    buyToSellRatio: sellTransactions.length > 0 ? 
      buyTransactions.length / sellTransactions.length : 
      (buyTransactions.length > 0 ? Infinity : 0),
    avgPrice,
    priceAtStart,
    priceAtEnd,
    priceChange: priceAtStart && priceAtEnd ? 
      ((priceAtEnd - priceAtStart) / priceAtStart) * 100 : null,
    activeWalletsCount: activeWallets.size,
    whaleTransactionsCount: whaleTransactions.length,
    whaleWalletsCount: whaleWallets.size
  });
}

console.log("周期分析:", periods);

// ==== 识别市场周期阶段 ====
// (积累 -> 上涨 -> 分配 -> 下跌)
const marketCycles = [];

for (let i = 0; i < periods.length - 1; i++) {
  const current = periods[i];
  const next = periods[i + 1];
  
  if (current.priceChange && next.priceChange) {
    // 潜在的积累->上涨
    if (current.priceChange < 5 && next.priceChange > 10) {
      marketCycles.push({
        startPeriod: current.periodNumber,
        endPeriod: next.periodNumber,
        type: 'accumulation_to_markup',
        startTime: current.startTime,
        endTime: next.endTime,
        startTimeFormatted: current.startTimeFormatted,
        endTimeFormatted: next.endTimeFormatted,
        description: '潜在积累到上涨阶段',
        priceChange: next.priceChange
      });
    }
    
    // 潜在的上涨->分配
    if (current.priceChange > 10 && next.priceChange < 5 && next.buyToSellRatio < 1) {
      marketCycles.push({
        startPeriod: current.periodNumber,
        endPeriod: next.periodNumber,
        type: 'markup_to_distribution',
        startTime: current.startTime,
        endTime: next.endTime,
        startTimeFormatted: current.startTimeFormatted,
        endTimeFormatted: next.endTimeFormatted,
        description: '潜在上涨到分销阶段',
        priceChange: current.priceChange
      });
    }
    
    // 潜在的分配->下跌
    if (current.priceChange < 5 && current.buyToSellRatio < 1 && next.priceChange < -10) {
      marketCycles.push({
        startPeriod: current.periodNumber,
        endPeriod: next.periodNumber,
        type: 'distribution_to_markdown',
        startTime: current.startTime,
        endTime: next.endTime,
        startTimeFormatted: current.startTimeFormatted,
        endTimeFormatted: next.endTimeFormatted,
        description: '潜在分销到下跌阶段',
        priceChange: next.priceChange
      });
    }
  }
}

console.log("已识别的市场周期:", marketCycles);

// ==== 识别协调活动的具体时间 ====
const coordinatedActivities = suspiciousActivityIntervals
  .filter(interval => interval.suspiciousScore >= 5 && interval.whaleTransactionsCount >= 2)
  .map(interval => ({
    timestamp: interval.timestamp,
    datetime: interval.datetime,
    suspiciousScore: interval.suspiciousScore,
    whaleCount: interval.uniqueWhaleWallets,
    description: '检测到密集交易活动，可能为协同性操作',
    transactionCount: interval.totalTransactions,
    buyToSellRatio: interval.buyToSellRatio
  }));

console.log("协调活动:", coordinatedActivities.slice(0, 5));

// ==== 计算数据集中的代币总量 ====
const totalTokenVolume = _.sumBy(sortedData, tx => {
  if (tx.type === 'TOKEN_BUY') return tx.buy_amount || 0;
  if (tx.type === 'TOKEN_SELL') return tx.sell_amount || 0;
  return 0;
});

// ==== 识别大额巨鲸进入 ====
const whaleEntries = [];
const timeWindows = _.groupBy(sortedTransactions, tx => 
  Math.floor(tx.trade_timestamp / 3600) // 按小时分组
);

Object.entries(timeWindows).forEach(([timestamp, transactions]) => {
  const buyTransactions = transactions.filter(tx => 
    tx.type === 'TOKEN_BUY' && tx.transaction_value >= WHALE_THRESHOLD
  );
  
  if (buyTransactions.length >= 2) {
    const uniqueWhales = [...new Set(buyTransactions.map(tx => tx.trader_wallet_address))];
    const totalBuyVolume = _.sumBy(buyTransactions, 'buy_amount');
    const percentOfTotal = (totalBuyVolume / totalTokenVolume) * 100;
    
    whaleEntries.push({
      timestamp: parseInt(timestamp),
      datetime: new Date(parseInt(timestamp) * 1000).toISOString(),
      whaleCount: uniqueWhales.length,
      totalBuyVolume,
      percentOfTotalSupply: percentOfTotal,
      transactions: buyTransactions.length
    });
  }
});

console.log("巨鲸进入:", whaleEntries.slice(0, 5));

// ==== 第二部分：生成分析报告 ======
console.log("正在生成分析报告...");

// 基于所有收集的数据生成最终分析报告
function generateAnalysisReport() {
  // 1. 确定选定的时间范围
  const selectedTimeRange = `${new Date(earliestTimestamp * 1000).toLocaleString()} 至 ${new Date(latestTimestamp * 1000).toLocaleString()}`;
  
  // 2. 计算可疑地址数量
  const suspiciousAddressesCount = topSuspiciousWallets.filter(w => w.suspiciousScore >= 3).length;
  
  // 3. 计算市场影响
  const suspiciousWalletsImpact = _.sumBy(topWalletsImpact.filter(w => w.suspiciousScore >= 3), 'marketImpact');
  
  // 4. 查找协调活动的时间段
  const coordinatedTimeframes = coordinatedActivities.length > 0 ? 
    coordinatedActivities.map(activity => activity.datetime).join(', ') : 
    '未检测到明显的协同性操作';
  
  // 5. 查找巨鲸进入
  const topWhaleEntries = _.sortBy(whaleEntries, 'totalBuyVolume').reverse().slice(0, 5);
  const whaleEntriesDescription = topWhaleEntries.length > 0 ? 
    topWhaleEntries.map(entry => `${entry.whaleCount}个巨鲸地址在${entry.datetime}进入，持有${entry.percentOfTotalSupply.toFixed(2)}%的代币`).join('\n') : 
    '未检测到显著的巨鲸进入';
  
  // 6. 查找重大价格影响
  const priceImpacts = [];
  
  // 寻找导致价格上涨的巨鲸买入
  priceChanges.filter(change => change.percentChange > 5).forEach(priceUp => {
    const whaleBuysBeforeIncrease = sortedData.filter(tx => 
      tx.type === 'TOKEN_BUY' && 
      tx.transaction_value >= WHALE_THRESHOLD &&
      tx.trade_timestamp >= priceUp.startTimestamp - 1800 && // 30分钟前
      tx.trade_timestamp <= priceUp.startTimestamp
    );
    
    if (whaleBuysBeforeIncrease.length > 0) {
      priceImpacts.push({
        timestamp: priceUp.startTimestamp,
        datetime: priceUp.startDatetime,
        event: '巨鲸买入',
        priceChange: `+${priceUp.percentChange.toFixed(2)}%`,
        description: `巨鲸买入 ➝ 价格 +${priceUp.percentChange.toFixed(2)}%`
      });
    }
  });
  
  // 寻找导致二次价格上涨的散户买入
  priceChanges.filter(change => change.percentChange > 5).forEach(priceUp => {
    const retailBuysBeforeIncrease = sortedData.filter(tx => 
      tx.type === 'TOKEN_BUY' && 
      tx.transaction_value <= RETAIL_THRESHOLD &&
      tx.transaction_value > 0 &&
      tx.trade_timestamp >= priceUp.startTimestamp - 1800 && // 30分钟前
      tx.trade_timestamp <= priceUp.startTimestamp
    );
    
    if (retailBuysBeforeIncrease.length > 10) { // 多笔散户买入
      priceImpacts.push({
        timestamp: priceUp.startTimestamp,
        datetime: priceUp.startDatetime,
        event: '散户跟风',
        priceChange: `+${priceUp.percentChange.toFixed(2)}%`,
        description: `散户跟风 ➝ 价格 +${priceUp.percentChange.toFixed(2)}%`
      });
    }
  });
  
  // 寻找导致价格下跌的巨鲸卖出
  priceChanges.filter(change => change.percentChange < -5).forEach(priceDown => {
    const whaleSellsBeforeDecrease = sortedData.filter(tx => 
      tx.type === 'TOKEN_SELL' && 
      tx.transaction_value >= WHALE_THRESHOLD &&
      tx.trade_timestamp >= priceDown.startTimestamp - 1800 && // 30分钟前
      tx.trade_timestamp <= priceDown.startTimestamp
    );
    
    if (whaleSellsBeforeDecrease.length > 0) {
      priceImpacts.push({
        timestamp: priceDown.startTimestamp,
        datetime: priceDown.startDatetime,
        event: '巨鲸卖出',
        priceChange: `${priceDown.percentChange.toFixed(2)}%`,
        description: `巨鲸卖出 ➝ 价格 ${priceDown.percentChange.toFixed(2)}%`
      });
    }
  });
  
  // 7. 识别拉高出货模式
  let pumpAndDumpEvidence = '';
  if (pumpAndDumpPatterns.length > 0) {
    const pattern = pumpAndDumpPatterns[0]; // 获取最显著的模式
    
    // 检查低价积累
    const lowPriceAccumulation = sortedData.filter(tx =>
      tx.type === 'TOKEN_BUY' &&
      tx.transaction_value >= WHALE_THRESHOLD &&
      tx.trade_timestamp < pattern.pump.startTimestamp &&
      tx.buy_price < pattern.pump.startPrice * 0.8 // 买入价格显著低于拉升开始
    );
    
    // 检查通过小额买入拉高价格
    const smallBuysDuringPump = sortedData.filter(tx =>
      tx.type === 'TOKEN_BUY' &&
      tx.transaction_value < MEDIUM_THRESHOLD &&
      tx.transaction_value > 0 &&
      tx.trade_timestamp >= pattern.pump.startTimestamp &&
      tx.trade_timestamp <= pattern.pump.endTimestamp
    );
    
    // 计算散户活动增加情况
    const beforePumpRetailBuys = sortedData.filter(tx =>
      tx.type === 'TOKEN_BUY' &&
      tx.transaction_value <= RETAIL_THRESHOLD &&
      tx.transaction_value > 0 &&
      tx.trade_timestamp < pattern.pump.startTimestamp &&
      tx.trade_timestamp >= pattern.pump.startTimestamp - (pattern.pump.endTimestamp - pattern.pump.startTimestamp)
    );
    
    const retailActivityIncrease = beforePumpRetailBuys.length > 0 ?
      pattern.retailBuysCount / beforePumpRetailBuys.length :
      pattern.retailBuysCount;
    
    // 找到最高价格时间戳
    const highPriceTimestamp = pattern.pump.endTimestamp;
    
    // 计算巨鲸卖出金额
    const totalWhaleSelling = pattern.whaleSellsValue;
    
    if (lowPriceAccumulation.length > 0 && smallBuysDuringPump.length > 5) {
      pumpAndDumpEvidence = `
1. 巨鲸地址在低位吸筹。
2. 通过小额买单拉高价格。
3. 价格上涨期间，散户买入量增加 ${retailActivityIncrease.toFixed(1)}倍。
4. 在价格达到高点后（${new Date(highPriceTimestamp * 1000).toLocaleString()}），巨鲸地址开始分批出货。
   - 出货金额：约为 $${totalWhaleSelling.toFixed(2)}。
`;
    } else {
      pumpAndDumpEvidence = '未检测到典型的拉高出货模式';
    }
  } else {
    pumpAndDumpEvidence = '未检测到典型的拉高出货模式';
  }

  // 8. 价格异常分析
  let priceAnomalySection = '';
  if (priceStats.max && priceStats.min) {
    const fluctuationMultiplier = priceStats.max.price / priceStats.min.price;
    if (fluctuationMultiplier > 1000) {
      priceAnomalySection = `
### 5.1 价格操纵风险
- 证据：在 ${priceStats.max.time} 至 ${priceStats.min.time} 期间价格波动超过 ${fluctuationMultiplier.toExponential(2)} 倍
- 最高价：${priceStats.max.price.toFixed(6)} 出现于 ${priceStats.max.time}
- 最低价：${priceStats.min.price.toExponential(6)} 出现于 ${priceStats.min.time}
- 影响：导致市场信心严重受损，典型的价格操纵行为

### 5.2 流动性风险
- 证据：`
      // 寻找买卖比例失衡的时间区间
      const imbalancedIntervals = Object.entries(detailedTimeIntervals).map(([timestamp, txs]) => {
        const buys = txs.filter(tx => tx.type === 'TOKEN_BUY');
        const sells = txs.filter(tx => tx.type === 'TOKEN_SELL');
        const buyVolume = _.sumBy(buys, 'buy_amount') || 0;
        const sellVolume = _.sumBy(sells, 'sell_amount') || 0;
        const ratio = buyVolume > 0 && sellVolume > 0 ? sellVolume / buyVolume : 0;
        return { timestamp: parseInt(timestamp), ratio, buyVolume, sellVolume };
      }).filter(int => int.ratio > 2);
      
      if (imbalancedIntervals.length > 0) {
        const worstInterval = _.maxBy(imbalancedIntervals, 'ratio');
        priceAnomalySection += `${new Date(worstInterval.timestamp * 1000).toLocaleString()}期间卖出量是买入量的${worstInterval.ratio.toFixed(1)}倍\n`;
        priceAnomalySection += `- 影响：造成价格快速下跌\n\n`;
      } else {
        priceAnomalySection += `未发现显著的流动性失衡\n`;
      }
      
      priceAnomalySection += `
### 5.3 市场操纵风险
- 证据：`;
      
      // 寻找交易密集且钱包高度集中的时间段
      const manipulationIntervals = suspiciousActivityIntervals.filter(int => 
        int.totalTransactions > 20 && int.uniqueWallets < 10 && int.suspiciousScore > 5
      );
      
      if (manipulationIntervals.length > 0) {
        const worstInterval = manipulationIntervals[0];
        priceAnomalySection += `${new Date(worstInterval.timestamp * 1000).toLocaleString()}期间${worstInterval.uniqueWallets}个钱包完成${worstInterval.totalTransactions}笔交易\n`;
        priceAnomalySection += `- 影响：市场秩序混乱，交易高度集中于少数钱包\n`;
      } else {
        priceAnomalySection += `未发现明显的交易集中现象\n`;
      }
    }
  }
  
  // 9. 编译最终报告
  const report = `# 代币交易分析报告

## 1. 概述
分析时间范围: ${selectedTimeRange}
数据记录总数: ${sortedData.length}
价格波动范围: ${priceStats.min ? priceStats.min.price.toExponential(6) : 'N/A'} - ${priceStats.max ? priceStats.max.price.toFixed(6) : 'N/A'}
价格波动倍数: ${priceStats.max && priceStats.min ? (priceStats.max.price / priceStats.min.price).toExponential(2) : 'N/A'}

## 2. 主要发现

### 2.1 可疑钱包活动
- 识别出 ${suspiciousAddressesCount} 个高度可疑的钱包地址
- 这些地址累计交易额占总交易额的 ${suspiciousWalletsImpact.toFixed(2)}%
- 最活跃的可疑钱包: ${topSuspiciousWallets.length > 0 ? topSuspiciousWallets[0].address : 'N/A'}，可疑评分: ${topSuspiciousWallets.length > 0 ? topSuspiciousWallets[0].suspiciousScore : 'N/A'}

### 2.2 价格波动分析
- 检测到 ${priceChanges.filter(change => change.isSignificant).length} 次显著价格变动 (>10%)
- 最大单次涨幅: ${_.maxBy(priceChanges, 'percentChange') ? '+' + _.maxBy(priceChanges, 'percentChange').percentChange.toFixed(2) + '%' : 'N/A'}
- 最大单次跌幅: ${_.minBy(priceChanges, 'percentChange') ? _.minBy(priceChanges, 'percentChange').percentChange.toFixed(2) + '%' : 'N/A'}

### 2.3 交易模式
- 拉高出货模式: ${pumpAndDumpPatterns.length > 0 ? '已检测到' : '未检测到'}
- 洗盘交易: ${suspiciousActivityIntervals.some(i => i.washTradingCount > 0) ? '已检测到' : '未检测到'}
- 协同操作: ${coordinatedActivities.length > 0 ? '已检测到' : '未检测到'}

## 3. 详细分析

### 3.1 钱包行为
- 交易最频繁的钱包: ${suspectedManipulators.length > 0 ? _.maxBy(suspectedManipulators, 'transactionCount').address : 'N/A'} (${suspectedManipulators.length > 0 ? _.maxBy(suspectedManipulators, 'transactionCount').transactionCount : 'N/A'}笔交易)
- 获利最多的钱包: ${suspectedManipulators.length > 0 ? _.maxBy(suspectedManipulators, 'netSOLChange').address : 'N/A'} (净获利${suspectedManipulators.length > 0 ? _.maxBy(suspectedManipulators, 'netSOLChange').netSOLChange.toFixed(2) : 'N/A'} SOL)

### 3.2 市场周期
${marketCycles.map(cycle => `- ${cycle.description}: ${new Date(cycle.startTime * 1000).toLocaleString()} 至 ${new Date(cycle.endTime * 1000).toLocaleString()}`).join('\n')}

### 3.3 巨鲸活动
${whaleEntriesDescription}

## 4. 价格影响因素
${priceImpacts.map(impact => `- ${new Date(impact.timestamp * 1000).toLocaleString()}: ${impact.description}`).join('\n')}

## 5. 风险警示

${priceAnomalySection ? priceAnomalySection : `### 5.1 交易风险
未检测到明显的交易风险`}

## 6. 结论与建议

基于以上分析，我们得出以下结论：

${pumpAndDumpPatterns.length > 0 || suspiciousActivityIntervals.some(i => i.suspiciousScore > 5) ?
  '- 该代币交易存在明显的操纵痕迹，投资者应保持高度警惕。\n- 建议监管部门对相关可疑钱包进行进一步调查。' :
  '- 该代币交易未显示明显操纵迹象，但市场波动仍然较大。\n- 建议投资者在投资前充分了解代币基本面。'}
- ${highestPrice && lowestPrice ? `价格波动幅度异常，从${lowestPrice.price.toExponential(6)}到${highestPrice.price.toFixed(6)}，投资者需注意风险。` : '价格数据不足，无法提供完整评估。'}
- ${coordinatedActivities.length > 0 ? '检测到协同操作行为，可能存在市场操纵。' : '未检测到明显的协同操作行为。'}

---
*本报告由自动分析系统生成，仅供参考，不构成投资建议。*
*生成时间: ${new Date().toLocaleString()}*
`;

  return report;
}

const analysisReport = generateAnalysisReport();
console.log("分析报告生成完成。");

// 将分析报告保存到文件
fs.writeFileSync(REPORT_PATH, analysisReport);
console.log(`分析报告已保存到 ${REPORT_PATH}`);

// ====== 第三部分：生成可视化图表 ======
console.log("正在生成可视化图表...");

// ==== 通用图表配置 ====
const chartDefaults = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: {
        intersect: false,
        mode: 'index'
    },
    plugins: {
        legend: {
            position: 'top',
            labels: {
                padding: 20,
                usePointStyle: true,
                pointStyle: 'circle'
            }
        },
        tooltip: {
            backgroundColor: 'rgba(255, 255, 255, 0.95)',
            titleColor: '#1a1a1a',
            bodyColor: '#666',
            bodyFont: {
                size: 13
            },
            borderColor: '#f0f0f0',
            borderWidth: 1,
            padding: 12,
            displayColors: true,
            callbacks: {
                label: function(context) {
                    let label = context.dataset.label || '';
                    if (label) {
                        label += ': ';
                    }
                    if (context.parsed.y !== null) {
                        label += context.parsed.y.toFixed(6);
                    }
                    return label;
                }
            }
        }
    }
};

// ==== 生成价格走势图配置 ====
// 在价格图表配置中使用安全日期函数
const createPriceChartConfig = () => {
    try {
        // 数据降采样 - 为了更好的性能和可视化
        const MAX_POINTS = 3000; // 限制最大点数
        let sampledData = [];
        let allData = [];
        
        console.log(`原始数据点: ${sortedData.length}`);
        
        if (sortedData.length > MAX_POINTS) {
            // 计算抽样间隔
            const interval = Math.ceil(sortedData.length / MAX_POINTS);
            
            // 按时间区间分组数据
            const timeGroups = {};
            sortedData.forEach(tx => {
                // 使用安全日期函数
                const date = createSafeDate(tx.trade_timestamp);
                if (!date) return; // 跳过无效日期
                
                // 向下取整到小时
                const hourTs = Math.floor(date.getTime() / 3600000) * 3600;
                if (!timeGroups[hourTs]) {
                    timeGroups[hourTs] = [];
                }
                timeGroups[hourTs].push({
                    ...tx,
                    timestamp: date.getTime()
                });
            });
            
            // 对每个时间区间进行处理
            Object.entries(timeGroups).forEach(([hourTs, transactions]) => {
                if (transactions.length === 1) {
                    // 如果只有一个数据点，直接添加
                    try {
                        sampledData.push({
                            x: new Date(transactions[0].timestamp).toISOString(),
                            y: transactions[0].price,
                            type: transactions[0].type
                        });
                    } catch (err) {
                        console.log(`处理单个时间点时出错:`, err.message);
                    }
                } else {
                    // 添加该小时的第一个点
                    try {
                        sampledData.push({
                            x: new Date(transactions[0].timestamp).toISOString(),
                            y: transactions[0].price,
                            type: transactions[0].type
                        });
                    } catch (err) {
                        console.log(`处理时间组首个点时出错:`, err.message);
                    }
                    
                    // 添加该小时的最后一个点
                    try {
                        sampledData.push({
                            x: new Date(transactions[transactions.length-1].timestamp).toISOString(),
                            y: transactions[transactions.length-1].price,
                            type: transactions[transactions.length-1].type
                        });
                    } catch (err) {
                        console.log(`处理时间组最后点时出错:`, err.message);
                    }
                    
                    // 找出最高和最低价格点
                    try {
                        const maxPrice = _.maxBy(transactions, 'price');
                        const minPrice = _.minBy(transactions, 'price');
                        
                        // 添加最高价格点（如果不是第一个或最后一个点）
                        if (maxPrice && maxPrice !== transactions[0] && maxPrice !== transactions[transactions.length-1]) {
                            sampledData.push({
                                x: new Date(maxPrice.timestamp).toISOString(),
                                y: maxPrice.price,
                                type: maxPrice.type,
                                isExtremeValue: true
                            });
                        }
                        
                        // 添加最低价格点（如果不是第一个或最后一个点）
                        if (minPrice && minPrice !== transactions[0] && minPrice !== transactions[transactions.length-1]) {
                            sampledData.push({
                                x: new Date(minPrice.timestamp).toISOString(),
                                y: minPrice.price,
                                type: minPrice.type,
                                isExtremeValue: true
                            });
                        }
                    } catch (err) {
                        console.log(`处理极值点时出错:`, err.message);
                    }
                }
            });
            
            // 添加所有异常价格点
            const abnormalPoints = sortedData.filter(d => d.price > 1000 || d.price < 0.0000001);
            abnormalPoints.forEach(point => {
                const date = createSafeDate(point.trade_timestamp);
                if (!date) return; // 跳过无效日期
                
                sampledData.push({
                    x: date.toISOString(),
                    y: point.price,
                    type: point.type,
                    isAbnormal: true
                });
            });
            
            // 确保按时间排序
            sampledData = _.sortBy(sampledData, 'x');
            
            console.log(`采样后数据点: ${sampledData.length}`);
            allData = sampledData;
        } else {
            // 数据量不大，直接使用所有数据点
            sortedData.forEach(tx => {
                const date = createSafeDate(tx.trade_timestamp);
                if (!date) return; // 跳过无效日期
                
                allData.push({
                    x: date.toISOString(),
                    y: tx.price,
                    type: tx.type,
                    abnormal: tx.isHighPriceOutlier || tx.isLowPriceOutlier
                });
            });
            console.log(`采样后数据点: ${allData.length}`);
        }
        
        // 生成价格图表
        // 添加日期分隔线
        const dayMarkers = [];
        if (allData.length > 0) {
            let currentDay = '';
            allData.forEach(dataPoint => {
                const date = new Date(dataPoint.x);
                const day = date.toISOString().split('T')[0];
                if (day !== currentDay) {
                    currentDay = day;
                    dayMarkers.push({
                        x: dataPoint.x,
                        y: 0,
                        day: day
                    });
                }
            });
        }
        
        // 找出异常高价格点
        const highAbnormals = allData.filter(d => d.abnormal && d.y > 1000).map(d => ({
            x: d.x,
            y: d.y,
            type: 'high'
        }));
        
        return {
            id: 'priceChart',
            title: '代币价格趋势',
            description: '显示代币价格随时间的变化趋势，支持缩放查看详情。',
            config: {
                type: 'line',
                data: {
                    datasets: [
                        // 日期分隔线
                        {
                            label: '日期分隔',
                            data: dayMarkers,
                            pointRadius: 0,
                            showLine: true,
                            borderColor: 'rgba(200, 200, 200, 0.3)',
                            borderDash: [5, 5],
                            borderWidth: 1,
                            fill: false,
                            pointHoverRadius: 0,
                            pointHitRadius: 0
                        },
                        // 价格线
                        {
                            label: '代币价格',
                            data: allData,
                            borderColor: 'rgba(59, 130, 246, 1)',
                            backgroundColor: 'rgba(59, 130, 246, 0.1)',
                            borderWidth: 2,
                            pointRadius: 0,
                            pointHoverRadius: 5,
                            pointHitRadius: 10,
                            pointHoverBackgroundColor: 'rgba(59, 130, 246, 1)',
                            fill: false,
                            tension: 0.1
                        },
                        // 异常高价格点
                        {
                            label: '异常高价格',
                            data: highAbnormals,
                            borderColor: 'rgba(239, 68, 68, 1)',
                            backgroundColor: 'rgba(239, 68, 68, 1)',
                            borderWidth: 0,
                            pointRadius: 4,
                            pointHoverRadius: 7,
                            showLine: false
                        }
                    ]
                },
                options: {
                    ...chartDefaults,
                    animation: false,
                    interaction: {
                        mode: 'nearest',
                        axis: 'x',
                        intersect: false
                    },
                    scales: {
                        y: {
                            type: 'logarithmic',
                            title: {
                                display: true,
                                text: '价格 (对数刻度)',
                                padding: 10
                            }
                        },
                        x: {
                            type: 'time',
                            time: {
                                unit: 'day',
                                displayFormats: {
                                    day: 'MM-dd'
                                },
                                tooltipFormat: 'yyyy-MM-dd HH:mm'
                            },
                            title: {
                                display: true,
                                text: '时间 (UTC+8)',
                                padding: 10
                            }
                        }
                    },
                    plugins: {
                        tooltip: {
                            callbacks: {
                                label: function(context) {
                                    if (context.dataset.label === '日期分隔') {
                                        return `日期: ${context.raw.day}`;
                                    }
                                    
                                    const value = context.raw.y;
                                    let typeLabel = '';
                                    
                                    if (context.raw.type === 'TOKEN_BUY') {
                                        typeLabel = '(买入)';
                                    } else if (context.raw.type === 'TOKEN_SELL') {
                                        typeLabel = '(卖出)';
                                    }
                                    
                                    if (value > 1000) {
                                        return `价格: ${value.toFixed(2)} ${typeLabel} (异常高)`;
                                    } else if (value < 0.0000001) {
                                        return `价格: ${value.toExponential(2)} ${typeLabel} (异常低)`;
                                    }
                                    
                                    return `价格: ${value < 0.01 ? value.toExponential(4) : value.toFixed(6)} ${typeLabel}`;
                                }
                            }
                        },
                        zoom: {
                            pan: {
                                enabled: true,
                                mode: 'x'
                            },
                            zoom: {
                                wheel: {
                                    enabled: true
                                },
                                pinch: {
                                    enabled: true
                                },
                                mode: 'x'
                            }
                        }
                    }
                }
            }
        };
    } catch (err) {
        console.log("生成价格图表时发生严重错误:", err);
        return {
            id: 'priceChart',
            title: '代币价格趋势',
            description: '生成图表时发生错误，请检查数据。',
            config: {
                type: 'line',
                data: { datasets: [] },
                options: { ...chartDefaults }
            }
        };
    }
};

// ==== 生成交易量图表配置 ====
const createVolumeChartConfig = () => {
    try {
        // 准备数据
        const intervalData = {};
        const timeLabels = [];
        
        // 按小时间隔聚合数据
        sortedData.forEach(tx => {
            try {
                // 使用安全日期函数处理时间戳
                const date = createSafeDate(tx.trade_timestamp);
                if (!date) return; // 跳过无效日期
                
                // 向下取整到小时
                date.setMinutes(0, 0, 0);
                const hourTimestamp = Math.floor(date.getTime() / 1000);
                
                if (!intervalData[hourTimestamp]) {
                    intervalData[hourTimestamp] = {
                        buys: { volume: 0, count: 0 },
                        sells: { volume: 0, count: 0 }
                    };
                    
                    // 记录时间标签
                    timeLabels.push({
                        ts: hourTimestamp,
                        isoString: date.toISOString()
                    });
                }
                
                if (tx.type === 'TOKEN_BUY') {
                    intervalData[hourTimestamp].buys.volume += tx.buy_amount || 0;
                    intervalData[hourTimestamp].buys.count++;
                } else if (tx.type === 'TOKEN_SELL') {
                    intervalData[hourTimestamp].sells.volume += tx.sell_amount || 0;
                    intervalData[hourTimestamp].sells.count++;
                }
            } catch (err) {
                console.log(`交易量聚合数据错误:`, err.message);
            }
        });
        
        // 如果没有有效的时间标签，返回空图表
        if (timeLabels.length === 0) {
            console.log("无有效时间标签，返回空的交易量图表");
            return {
                id: 'volumeChart',
                title: '交易量分布',
                description: '无法生成交易量图表：没有有效的时间数据。',
                config: {
                    type: 'bar',
                    data: { datasets: [] },
                    options: { ...chartDefaults }
                }
            };
        }
        
        // 按时间戳排序时间标签
        timeLabels.sort((a, b) => a.ts - b.ts);
        
        // 找出最小和最大时间戳
        const allTimestamps = timeLabels.map(item => item.ts);
        const minTimestamp = Math.min(...allTimestamps);
        const maxTimestamp = Math.max(...allTimestamps);
        
        // 填充可能缺失的时间区间
        const completeIntervalData = {};
        for (let ts = minTimestamp; ts <= maxTimestamp; ts += 3600) {
            completeIntervalData[ts] = intervalData[ts] || {
                buys: { volume: 0, count: 0 },
                sells: { volume: 0, count: 0 }
            };
        }
        
        // 安全地重新生成完整的时间标签和数据
        const buyData = [];
        const sellData = [];
        
        for (const ts of Object.keys(completeIntervalData).sort().map(Number)) {
            try {
                const date = createSafeDate(ts);
                if (!date) continue; // 跳过无效日期
                
                const isoString = date.toISOString();
                
                buyData.push({
                    x: isoString,
                    y: completeIntervalData[ts].buys.volume
                });
                
                sellData.push({
                    x: isoString,
                    y: completeIntervalData[ts].sells.volume
                });
            } catch (err) {
                console.log(`生成数据点错误: ${ts}`, err.message);
            }
        }
        
        // 计算总时间区间数和有交易的区间数
        const totalIntervals = Object.keys(completeIntervalData).length;
        const intervalsWithTrades = Object.values(completeIntervalData).filter(
            interval => interval.buys.count > 0 || interval.sells.count > 0
        ).length;
        
        return {
            id: 'volumeChart',
            title: '交易量分布',
            description: `显示买入和卖出的交易量分布情况。总时间区间数: ${totalIntervals}, 有交易的区间数: ${intervalsWithTrades}。`,
            config: {
                type: 'bar',
                data: {
                    datasets: [
                        {
                            label: '买入量',
                            data: buyData,
                            backgroundColor: 'rgba(34, 197, 94, 0.85)', // 提高不透明度
                            borderColor: 'rgba(34, 197, 94, 1)',
                            borderWidth: 1
                        },
                        {
                            label: '卖出量',
                            data: sellData,
                            backgroundColor: 'rgba(239, 68, 68, 0.85)', // 提高不透明度
                            borderColor: 'rgba(239, 68, 68, 1)',
                            borderWidth: 1
                        }
                    ]
                },
                options: {
                    ...chartDefaults,
                    scales: {
                        y: {
                            type: 'logarithmic',
                            title: {
                                display: true,
                                text: '交易量 (对数刻度)',
                                padding: 10
                            }
                        },
                        x: {
                            type: 'time',
                            time: {
                                unit: 'hour',
                                displayFormats: {
                                    hour: 'MM-dd HH:mm'
                                },
                                tooltipFormat: 'yyyy-MM-dd HH:mm'
                            },
                            title: {
                                display: true,
                                text: '时间 (UTC+8)',
                                padding: 10
                            }
                        }
                    }
                }
            }
        };
    } catch (err) {
        console.log("生成交易量图表时发生严重错误:", err);
        // 返回空的图表配置
        return {
            id: 'volumeChart',
            title: '交易量分布',
            description: '生成图表时发生错误，请检查数据。',
            config: {
                type: 'bar',
                data: { datasets: [] },
                options: { ...chartDefaults }
            }
        };
    }
};

// ==== 生成买卖比率图表配置 ====
const createBuySellRatioChartConfig = () => {
    try {
        // 使用与交易量图表相同的时间间隔
        const intervalData = {};
        
        // 按小时间隔聚合数据
        sortedData.forEach(tx => {
            try {
                // 使用安全日期函数处理时间戳
                const date = createSafeDate(tx.trade_timestamp);
                if (!date) return; // 跳过无效日期
                
                // 向下取整到小时
                date.setMinutes(0, 0, 0);
                const hourTimestamp = Math.floor(date.getTime() / 1000);
                
                if (!intervalData[hourTimestamp]) {
                    intervalData[hourTimestamp] = {
                        buys: 0,
                        sells: 0
                    };
                }
                
                if (tx.type === 'TOKEN_BUY') {
                    intervalData[hourTimestamp].buys++;
                } else if (tx.type === 'TOKEN_SELL') {
                    intervalData[hourTimestamp].sells++;
                }
            } catch (err) {
                console.log(`买卖比率 - 聚合数据出错:`, err.message);
            }
        });
        
        // 安全地转换为图表数据
        const timeLabels = [];
        for (const tsStr of Object.keys(intervalData).sort()) {
            try {
                const ts = parseInt(tsStr, 10);
                if (!isNaN(ts)) {
                    const date = createSafeDate(ts);
                    if (date) {
                        timeLabels.push({
                            ts: ts,
                            isoString: date.toISOString()
                        });
                    }
                }
            } catch (err) {
                console.log(`买卖比率 - 时间标签转换错误:`, err.message, tsStr);
            }
        }
        
        // 如果没有有效的时间标签，返回空图表
        if (timeLabels.length === 0) {
            console.log("无有效时间标签，返回空的买卖比率图表");
            return {
                id: 'buySellRatioChart',
                title: '买卖比率变化',
                description: '无法生成买卖比率图表：没有有效的时间数据。',
                config: {
                    type: 'bar',
                    data: { datasets: [] },
                    options: { ...chartDefaults }
                }
            };
        }
        
        // 计算每个时间区间的买卖比率
        const ratioData = [];
        const validLabels = [];
        
        timeLabels.forEach(item => {
            try {
                const { ts, isoString } = item;
                const { buys, sells } = intervalData[ts];
                let ratio;
                
                // 避免除以零
                if (sells === 0) {
                    ratio = buys > 0 ? 5 : 0;  // 如果有买入但没有卖出，限制比率为5
                } else {
                    ratio = Math.min(buys / sells, 5);  // 限制最大比率为5以便更好地可视化
                }
                
                ratioData.push(ratio);
                validLabels.push(isoString);
            } catch (err) {
                console.log(`买卖比率 - 计算比率错误:`, err.message);
            }
        });
        
        // 如果没有有效数据，返回空图表
        if (ratioData.length === 0) {
            return {
                id: 'buySellRatioChart',
                title: '买卖比率变化',
                description: '无法生成买卖比率图表：没有有效的比率数据。',
                config: {
                    type: 'bar',
                    data: { datasets: [] },
                    options: { ...chartDefaults }
                }
            };
        }
        
        // 找出异常值（特别高或低的比率）
        const validRatios = ratioData.filter(r => !isNaN(r) && isFinite(r));
        const avgRatio = validRatios.length > 0 ? _.mean(validRatios) : 1;
        const stdDev = validRatios.length > 0 
            ? Math.sqrt(_.mean(validRatios.map(r => Math.pow(r - avgRatio, 2))))
            : 0;
        
        const abnormalThreshold = avgRatio + 2 * stdDev;
        const isAbnormal = ratioData.map(r => r > abnormalThreshold);
        
        return {
            id: 'buySellRatioChart',
            title: '买卖比率变化',
            description: '显示每个时间区间内买入交易与卖出交易的比率，反映市场情绪变化。比率>1表示买入多于卖出。',
            config: {
                type: 'bar',  // 改为柱状图
                data: {
                    labels: validLabels,
                    datasets: [{
                        label: '买卖比率',
                        data: ratioData,
                        backgroundColor: ratioData.map(value => 
                            value > 1 ? (value > 3 ? 'rgba(22, 163, 74, 0.85)' : 'rgba(34, 197, 94, 0.85)') : 
                                      'rgba(239, 68, 68, 0.85)'
                        ),
                        borderColor: ratioData.map(value => 
                            value > 1 ? (value > 3 ? 'rgba(22, 163, 74, 1)' : 'rgba(34, 197, 94, 1)') : 
                                      'rgba(239, 68, 68, 1)'
                        ),
                        borderWidth: 1
                    }]
                },
                options: {
                    ...chartDefaults,
                    scales: {
                        y: {
                            beginAtZero: true,
                            title: {
                                display: true,
                                text: '买入/卖出比率',
                                padding: 10
                            },
                            suggestedMax: 3,  // 设置合理的Y轴最大值以便更好地观察大多数数据
                        },
                        x: {
                            type: 'time',
                            time: {
                                unit: 'hour',
                                displayFormats: {
                                    hour: 'MM-dd HH:mm'
                                },
                                tooltipFormat: 'yyyy-MM-dd HH:mm'
                            },
                            title: {
                                display: true,
                                text: '时间 (UTC+8)',
                                padding: 10
                            }
                        }
                    },
                    plugins: {
                        tooltip: {
                            callbacks: {
                                label: function(context) {
                                    const value = context.raw;
                                    const label = `买卖比率: ${value.toFixed(2)}`;
                                    
                                    if (value > 3) {
                                        return `${label} (异常高)`;
                                    } else if (value > 1) {
                                        return `${label} (买入多于卖出)`;
                                    } else if (value === 1) {
                                        return `${label} (买卖平衡)`;
                                    } else {
                                        return `${label} (卖出多于买入)`;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };
    } catch (err) {
        console.log("生成买卖比率图表时发生严重错误:", err);
        // 返回空的图表配置
        return {
            id: 'buySellRatioChart',
            title: '买卖比率变化',
            description: '生成图表时发生错误，请检查数据。',
            config: {
                type: 'bar',
                data: { datasets: [] },
                options: { ...chartDefaults }
            }
        };
    }
};

// ==== 生成钱包活动图表配置 ====
const createWalletActivityChartConfig = () => {
    try {
        // 确保钱包活动数据存在且有效
        if (!walletActivity || Object.keys(walletActivity).length === 0) {
            console.log("警告: 钱包活动数据为空，返回默认图表配置");
            return {
                id: 'walletActivityChart',
                title: '机器人交易活动',
                description: '当前没有足够的钱包活动数据可供分析。',
                config: {
                    type: 'bar',
                    data: {
                        labels: ['无数据'],
                        datasets: [{
                            label: '无数据',
                            data: [0],
                            backgroundColor: 'rgba(200, 200, 200, 0.5)'
                        }]
                    },
                    options: { ...chartDefaults }
                }
            };
        }
    
        // 展示最活跃的前10个钱包
        const topActiveWallets = Object.entries(walletActivity)
            .map(([address, activity]) => ({
                address,
                buys: activity.buys?.count || 0,
                sells: activity.sells?.count || 0,
                totalTransactions: (activity.buys?.count || 0) + (activity.sells?.count || 0)
            }))
            .sort((a, b) => b.totalTransactions - a.totalTransactions)
            .slice(0, 10);
        
        // 确保数据格式正确
        const data = topActiveWallets.map(wallet => ({
            address: wallet.address || 'unknown',
            buys: wallet.buys || 0,
            sells: wallet.sells || 0,
            suspicious: suspectedManipulators.some(m => m.address === wallet.address && m.suspiciousScore >= 3)
        }));
        
        return {
            id: 'walletActivityChart',
            title: '钱包交易活动',
            description: '展示最活跃的10个钱包的买入和卖出交易次数，⚠️标记表示被系统识别为可疑的机器人钱包。',
            config: {
                type: 'bar',
                data: {
                    labels: data.map(w => {
                        const shortAddress = w.address ? (w.address.slice(0, 6) + '...' + w.address.slice(-4)) : 'unknown';
                        return w.suspicious ? '⚠️ ' + shortAddress : shortAddress;
                    }),
                    datasets: [
                        {
                            label: '买入次数',
                            data: data.map(w => w.buys),
                            backgroundColor: data.map(w => w.suspicious ? 'rgba(234, 179, 8, 0.8)' : 'rgba(34, 197, 94, 0.8)'),
                            borderColor: data.map(w => w.suspicious ? 'rgba(234, 179, 8, 0.9)' : 'rgba(34, 197, 94, 0.9)'),
                            borderWidth: 1
                        },
                        {
                            label: '卖出次数',
                            data: data.map(w => w.sells),
                            backgroundColor: data.map(w => w.suspicious ? 'rgba(249, 115, 22, 0.8)' : 'rgba(239, 68, 68, 0.8)'),
                            borderColor: data.map(w => w.suspicious ? 'rgba(249, 115, 22, 0.9)' : 'rgba(239, 68, 68, 0.9)'),
                            borderWidth: 1
                        }
                    ]
                },
                options: {
                    ...chartDefaults,
                    indexAxis: 'y',
                    scales: {
                        y: {
                            grid: {
                                display: false
                            }
                        },
                        x: {
                            grid: {
                                color: '#f0f0f0'
                            },
                            title: {
                                display: true,
                                text: '交易次数',
                                padding: 10
                            }
                        }
                    }
                }
            }
        };
    } catch (error) {
        console.error("创建钱包活动图表时出错:", error);
        // 返回一个默认图表配置，避免整个HTML生成失败
        return {
            id: 'walletActivityChart',
            title: '钱包交易活动',
            description: '生成图表时发生错误，请检查控制台输出。',
            config: {
                type: 'bar',
                data: {
                    labels: ['错误'],
                    datasets: [{
                        label: '错误',
                        data: [0],
                        backgroundColor: 'rgba(239, 68, 68, 0.8)'
                    }]
                },
                options: { ...chartDefaults }
            }
        };
    }
};

// 生成HTML页面
function generateHTML() {
    const priceChart = createPriceChartConfig();
    const volumeChart = createVolumeChartConfig();
    const ratioChart = createBuySellRatioChartConfig();
    const walletActivityChart = createWalletActivityChartConfig();
    
    const charts = [priceChart, volumeChart, ratioChart, walletActivityChart];

    // 计算关键统计数据
    const suspiciousAddressesCount = topSuspiciousWallets.filter(w => w.suspiciousScore >= 3).length;
    const suspiciousWalletsImpact = _.sumBy(topWalletsImpact.filter(w => w.suspiciousScore >= 3), 'marketImpact');
    const significantPriceChanges = priceChanges.filter(change => change.isSignificant).length;
    const maxPriceIncrease = _.maxBy(priceChanges, 'percentChange');
    const maxPriceDecrease = _.minBy(priceChanges, 'percentChange');
    const detectPumpAndDump = pumpAndDumpPatterns.length > 0 ? '已检测到' : '未检测到';
    const detectWashTrading = suspiciousActivityIntervals.some(i => i.washTradingCount > 0) ? '已检测到' : '未检测到';
    const detectCoordination = coordinatedActivities.length > 0 ? '已检测到' : '未检测到';
    
    return `
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>代币交易分析报告</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/moment"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-moment"></script>
    <script src="https://cdn.jsdelivr.net/npm/hammerjs@2.0.8"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-zoom@2.0.0"></script>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css">
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 0;
            background-color: #f5f7fa;
            color: #333;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 2rem;
        }
        .header {
            margin-bottom: 2rem;
            text-align: center;
            padding: 1.5rem 0;
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }
        .header h1 {
            font-size: 2rem;
            font-weight: 600;
            color: #1f2937;
            margin-bottom: 0.5rem;
        }
        .header p {
            color: #6b7280;
            max-width: 800px;
            margin: 0 auto;
        }
        .stats-container {
            display: flex;
            flex-wrap: wrap;
            justify-content: center;
            gap: 1.5rem;
            margin: 1.5rem 0;
        }
        .stat-card {
            background: white;
            border-radius: 8px;
            padding: 1rem;
            flex: 1 1 200px;
            text-align: center;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            transition: transform 0.2s;
            max-width: 300px;
        }
        .stat-card:hover {
            transform: translateY(-3px);
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        .stat-number {
            font-size: 1.8rem;
            font-weight: 600;
            color: #1f2937;
            margin: 0.5rem 0;
        }
        .stat-label {
            font-size: 0.9rem;
            color: #6b7280;
        }
        .chart-container {
            height: 400px;
            position: relative;
            margin-bottom: 2rem;
        }
        .tab-content {
            background-color: white;
            border-radius: 0 0 8px 8px;
            padding: 2rem;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }
        .nav-tabs {
            margin-bottom: 0;
            border-bottom: none;
        }
        .nav-tabs .nav-link {
            border-radius: 8px 8px 0 0;
            font-weight: 500;
            color: #6b7280;
            padding: 0.75rem 1.5rem;
            margin-right: 0.25rem;
        }
        .nav-tabs .nav-link.active {
            color: #1f2937;
            border-bottom: none;
            background-color: white;
        }
        .markdown-content {
            line-height: 1.6;
        }
        .markdown-content h2 {
            border-bottom: 1px solid #eaeaea;
            padding-bottom: 0.5rem;
            margin-top: 2rem;
            margin-bottom: 1rem;
            font-size: 1.5rem;
        }
        .markdown-content h3 {
            margin-top: 1.5rem;
            font-size: 1.25rem;
        }
        .alert-warning {
            background-color: #fffbeb;
            border-color: #fef3c7;
            color: #92400e;
        }
        .alert-danger {
            background-color: #fef2f2;
            border-color: #fee2e2;
            color: #b91c1c;
        }
        .wallet-table {
            width: 100%;
            border-collapse: collapse;
            margin: 1.5rem 0;
        }
        .wallet-table th, .wallet-table td {
            padding: 0.75rem;
            border-bottom: 1px solid #eaeaea;
            text-align: left;
        }
        .wallet-table th {
            background-color: #f9fafb;
            font-weight: 600;
        }
        .wallet-table tr:hover {
            background-color: #f9fafb;
        }
        .suspicious-score {
            display: inline-block;
            padding: 0.25rem 0.5rem;
            border-radius: 4px;
            font-weight: 600;
        }
        .score-high {
            background-color: #fee2e2;
            color: #b91c1c;
        }
        .score-medium {
            background-color: #fef3c7;
            color: #92400e;
        }
        .score-low {
            background-color: #ecfdf5;
            color: #047857;
        }
        .footer {
            text-align: center;
            padding: 1rem 0;
            color: #6b7280;
            font-size: 0.875rem;
            margin-top: 2rem;
        }
        .tab-pane {
            animation: fadeIn 0.3s ease-in-out;
        }
        @keyframes fadeIn {
            from { opacity: 0; }
            to { opacity: 1; }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>代币交易分析报告</h1>
            <p>分析时间范围: ${new Date(earliestTimestamp * 1000).toLocaleString()} 至 ${new Date(latestTimestamp * 1000).toLocaleString()}</p>
            
            <div class="stats-container">
                <div class="stat-card">
                    <div class="stat-label">总交易记录</div>
                    <div class="stat-number">${sortedData.length}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">可疑钱包数量</div>
                    <div class="stat-number">${suspiciousAddressesCount}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">最高价格</div>
                    <div class="stat-number">${priceStats.max ? priceStats.max.price.toFixed(4) : 'N/A'}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">最低价格</div>
                    <div class="stat-number">${priceStats.min ? priceStats.min.price.toExponential(2) : 'N/A'}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">价格波动倍数</div>
                    <div class="stat-number">${priceStats.max && priceStats.min ? (priceStats.max.price / priceStats.min.price).toExponential(2) : 'N/A'}</div>
                </div>
            </div>
        </div>
        
        <ul class="nav nav-tabs" id="analysisTab" role="tablist">
            <li class="nav-item" role="presentation">
                <button class="nav-link active" id="charts-tab" data-bs-toggle="tab" data-bs-target="#charts" type="button" role="tab" aria-controls="charts" aria-selected="true">数据图表</button>
            </li>
            <li class="nav-item" role="presentation">
                <button class="nav-link" id="report-tab" data-bs-toggle="tab" data-bs-target="#report" type="button" role="tab" aria-controls="report" aria-selected="false">分析报告</button>
            </li>
            <li class="nav-item" role="presentation">
                <button class="nav-link" id="wallets-tab" data-bs-toggle="tab" data-bs-target="#wallets" type="button" role="tab" aria-controls="wallets" aria-selected="false">可疑钱包</button>
            </li>
            <li class="nav-item" role="presentation">
                <button class="nav-link" id="risks-tab" data-bs-toggle="tab" data-bs-target="#risks" type="button" role="tab" aria-controls="risks" aria-selected="false">风险警示</button>
            </li>
        </ul>
        
        <div class="tab-content" id="analysisTabContent">
            <!-- 图表标签页 -->
            <div class="tab-pane fade show active" id="charts" role="tabpanel" aria-labelledby="charts-tab">
                <div class="row">
                    ${charts.map(chart => `
                    <div class="col-md-6 mb-4">
                        <h3>${chart.title}</h3>
                        <p class="text-muted">${chart.description}</p>
                        <div class="chart-container">
                            <canvas id="${chart.id}"></canvas>
                        </div>
                    </div>
                    `).join('')}
                </div>
            </div>
            
            <!-- 分析报告标签页 -->
            <div class="tab-pane fade" id="report" role="tabpanel" aria-labelledby="report-tab">
                <div class="markdown-content">
                    <h2>1. 概述</h2>
                    <p><strong>分析时间范围:</strong> ${new Date(earliestTimestamp * 1000).toLocaleString()} 至 ${new Date(latestTimestamp * 1000).toLocaleString()}</p>
                    <p><strong>数据记录总数:</strong> ${sortedData.length}</p>
                    <p><strong>价格波动范围:</strong> ${priceStats.min ? priceStats.min.price.toExponential(6) : 'N/A'} - ${priceStats.max ? priceStats.max.price.toFixed(6) : 'N/A'}</p>
                    <p><strong>价格波动倍数:</strong> ${priceStats.max && priceStats.min ? (priceStats.max.price / priceStats.min.price).toExponential(2) : 'N/A'}</p>
                    
                    <h2>2. 主要发现</h2>
                    
                    <h3>2.1 可疑钱包活动</h3>
                    <ul>
                        <li>识别出 ${suspiciousAddressesCount} 个高度可疑的钱包地址</li>
                        <li>这些地址累计交易额占总交易额的 ${suspiciousWalletsImpact.toFixed(2)}%</li>
                        <li>最活跃的可疑钱包: ${topSuspiciousWallets.length > 0 ? topSuspiciousWallets[0].address : 'N/A'}，可疑评分: ${topSuspiciousWallets.length > 0 ? topSuspiciousWallets[0].suspiciousScore : 'N/A'}</li>
                    </ul>
                    
                    <h3>2.2 价格波动分析</h3>
                    <ul>
                        <li>检测到 ${significantPriceChanges} 次显著价格变动 (>10%)</li>
                        <li>最大单次涨幅: ${maxPriceIncrease ? '+' + maxPriceIncrease.percentChange.toFixed(2) + '%' : 'N/A'}</li>
                        <li>最大单次跌幅: ${maxPriceDecrease ? maxPriceDecrease.percentChange.toFixed(2) + '%' : 'N/A'}</li>
                    </ul>
                    
                    <h3>2.3 交易模式</h3>
                    <ul>
                        <li>拉高出货模式: ${detectPumpAndDump}</li>
                        <li>洗盘交易: ${detectWashTrading}</li>
                        <li>协同操作: ${detectCoordination}</li>
                    </ul>
                    
                    <h2>3. 详细分析</h2>
                    
                    <h3>3.1 钱包行为</h3>
                    <ul>
                        <li>交易最频繁的钱包: ${suspectedManipulators.length > 0 ? _.maxBy(suspectedManipulators, 'transactionCount').address : 'N/A'} (${suspectedManipulators.length > 0 ? _.maxBy(suspectedManipulators, 'transactionCount').transactionCount : 'N/A'}笔交易)</li>
                        <li>获利最多的钱包: ${suspectedManipulators.length > 0 ? _.maxBy(suspectedManipulators, 'netSOLChange').address : 'N/A'} (净获利${suspectedManipulators.length > 0 ? _.maxBy(suspectedManipulators, 'netSOLChange').netSOLChange.toFixed(2) : 'N/A'} SOL)</li>
                    </ul>
                    
                    <h3>3.2 市场周期</h3>
                    <ul>
                        ${marketCycles.map(cycle => `<li>${cycle.description}: ${new Date(cycle.startTime * 1000).toLocaleString()} 至 ${new Date(cycle.endTime * 1000).toLocaleString()}</li>`).join('')}
                    </ul>
                    
                    <h3>3.3 巨鲸活动</h3>
                    <p>${whaleEntries.length > 0 ? 
                        whaleEntries.slice(0, 5).map(entry => 
                            `${entry.whaleCount}个巨鲸地址在${new Date(entry.timestamp * 1000).toLocaleString()}进入，持有${entry.percentOfTotalSupply.toFixed(2)}%的代币`
                        ).join('<br>') : 
                        '未检测到显著的巨鲸进入'}</p>
                    
                    <h2>4. 价格影响因素</h2>
                    <ul>
                        ${priceChanges.filter(change => Math.abs(change.percentChange) > 10).map(impact => 
                            `<li>${new Date(impact.startTimestamp * 1000).toLocaleString()}: ${impact.percentChange > 0 ? '上涨' : '下跌'} ${Math.abs(impact.percentChange).toFixed(2)}%</li>`
                        ).join('')}
                    </ul>
                    
                    <h2>6. 结论与建议</h2>
                    <p>${pumpAndDumpPatterns.length > 0 || suspiciousActivityIntervals.some(i => i.suspiciousScore > 5) ?
                      '该代币交易存在明显的操纵痕迹，投资者应保持高度警惕。' :
                      '虽然市场波动较大，但未发现明显操纵迹象，投资者在深入了解代币基本面后可以考虑适量参与。'}</p>
                    <p>${highestPrice && lowestPrice ? `价格波动幅度异常，从${lowestPrice.price.toExponential(6)}到${highestPrice.price.toFixed(6)}，投资者需注意风险。` : '价格数据不足，无法提供完整评估。'}</p>
                    <p>${coordinatedActivities.length > 0 ? '检测到协同操作行为，可能存在市场操纵。' : '未检测到明显的协同操作行为。'}</p>
                </div>
            </div>
            
            <!-- 可疑钱包标签页 -->
            <div class="tab-pane fade" id="wallets" role="tabpanel" aria-labelledby="wallets-tab">
                <h2>可疑钱包分析</h2>
                <p class="alert alert-warning">以下钱包根据交易行为和模式被系统识别为可能的市场操纵者。高可疑度评分(≥5)表示钱包行为高度异常，可能参与市场操纵。</p>
                
                <table class="wallet-table">
                    <thead>
                        <tr>
                            <th>钱包地址</th>
                            <th>可疑度评分</th>
                            <th>交易次数</th>
                            <th>总交易额</th>
                            <th>买卖比率</th>
                            <th>活跃时长(小时)</th>
                            <th>净SOL变化</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${topSuspiciousWallets.map(wallet => `
                        <tr>
                            <td>${wallet.address}</td>
                            <td>
                                <span class="suspicious-score ${wallet.suspiciousScore >= 5 ? 'score-high' : (wallet.suspiciousScore >= 3 ? 'score-medium' : 'score-low')}">
                                    ${wallet.suspiciousScore}
                                </span>
                            </td>
                            <td>${wallet.transactionCount}</td>
                            <td>$${wallet.totalValue.toFixed(2)}</td>
                            <td>${wallet.buyToSellRatio === Infinity ? '∞' : wallet.buyToSellRatio.toFixed(2)}</td>
                            <td>${wallet.timeActiveHours.toFixed(2)}</td>
                            <td class="${wallet.netSOLChange > 0 ? 'text-success' : 'text-danger'}">${wallet.netSOLChange.toFixed(4)}</td>
                        </tr>
                        `).join('')}
                    </tbody>
                </table>
                
                <h3>钱包活动分析</h3>
                <p>以下是系统检测到的异常钱包活动模式：</p>
                <ul>
                    ${suspectedManipulators.filter(w => w.suspiciousScore >= 4).map(wallet => `
                    <li>
                        <strong>${wallet.address.slice(0, 6)}...${wallet.address.slice(-4)}</strong>: 
                        ${wallet.transactionFrequency > 5 ? `异常高的交易频率(${wallet.transactionFrequency.toFixed(1)}笔/小时)` : ''}
                        ${wallet.buyToSellRatio > 10 ? `买入异常高于卖出(${wallet.buyToSellRatio.toFixed(1)}倍)` : ''}
                        ${wallet.buyToSellRatio < 0.1 ? `卖出异常高于买入(${(1/wallet.buyToSellRatio).toFixed(1)}倍)` : ''}
                        ${wallet.timeActiveHours < 1 && wallet.transactionCount > 10 ? `短时间内大量交易(${wallet.transactionCount}笔)` : ''}
                        ${wallet.netSOLChange > 5 ? `获利显著(${wallet.netSOLChange.toFixed(2)} SOL)` : ''}
                    </li>
                    `).join('')}
                </ul>
            </div>
            
            <!-- 风险警示标签页 -->
            <div class="tab-pane fade" id="risks" role="tabpanel" aria-labelledby="risks-tab">
                <h2>风险警示</h2>
                
                ${priceStats.max && priceStats.min && (priceStats.max.price / priceStats.min.price > 1000) ? `
                <div class="alert alert-danger mb-4" role="alert">
                    <h4 class="alert-heading">严重价格操纵风险</h4>
                    <p><strong>证据：</strong>在 ${priceStats.max.time} 至 ${priceStats.min.time} 期间价格波动超过 ${(priceStats.max.price / priceStats.min.price).toExponential(2)} 倍</p>
                    <hr>
                    <p class="mb-0">最高价: ${priceStats.max.price.toFixed(6)} 出现于 ${priceStats.max.time}<br>
                    最低价: ${priceStats.min.price.toExponential(6)} 出现于 ${priceStats.min.time}</p>
                </div>
                ` : ''}
                
                ${pumpAndDumpPatterns.length > 0 ? `
                <div class="alert alert-danger mb-4" role="alert">
                    <h4 class="alert-heading">拉高出货风险</h4>
                    <p><strong>检测到 ${pumpAndDumpPatterns.length} 个拉高出货模式</strong></p>
                    <hr>
                    <p><strong>最显著模式:</strong> 价格上涨 ${pumpAndDumpPatterns[0].pump.percentChange.toFixed(2)}%，散户买入 ${pumpAndDumpPatterns[0].retailBuysCount} 笔，巨鲸随后卖出 ${pumpAndDumpPatterns[0].whaleSellsValue.toFixed(2)} 美元</p>
                </div>
                ` : ''}
                
                ${suspiciousActivityIntervals.some(i => i.washTradingCount > 3) ? `
                <div class="alert alert-warning mb-4" role="alert">
                    <h4 class="alert-heading">洗盘交易风险</h4>
                    <p><strong>检测到多个洗盘交易行为</strong></p>
                    <hr>
                    <p>同一钱包在短时间内反复买卖，可能试图操纵交易量或价格</p>
                </div>
                ` : ''}
                
                ${coordinatedActivities.length > 0 ? `
                <div class="alert alert-warning mb-4" role="alert">
                    <h4 class="alert-heading">协同操作风险</h4>
                    <p><strong>检测到 ${coordinatedActivities.length} 个可能的协同操作时间段</strong></p>
                    <hr>
                    <p>多个钱包在同一时间段内进行相似操作，可能存在协同市场操纵行为</p>
                </div>
                ` : ''}
                
                ${suspiciousWalletsImpact > 30 ? `
                <div class="alert alert-danger mb-4" role="alert">
                    <h4 class="alert-heading">交易集中度风险</h4>
                    <p><strong>可疑钱包占总交易额的 ${suspiciousWalletsImpact.toFixed(2)}%</strong></p>
                    <hr>
                    <p>交易高度集中于少数可疑钱包，市场可能被少数参与者控制</p>
                </div>
                ` : ''}
                
                <h3>风险评估总结</h3>
                
                <div class="row mt-4">
                    <div class="col-md-6">
                        <h4>市场操纵风险</h4>
                        <div class="progress mb-3">
                            <div class="progress-bar bg-danger" role="progressbar" style="width: ${
                                Math.min(100, ((suspiciousAddressesCount * 10) + (pumpAndDumpPatterns.length * 20) + 
                                (coordinatedActivities.length * 15) + 
                                (priceStats.max && priceStats.min ? Math.min(100, (priceStats.max.price / priceStats.min.price) / 1000 * 25) : 0)))}%"></div>
                        </div>
                    </div>
                    
                    <div class="col-md-6">
                        <h4>流动性风险</h4>
                        <div class="progress mb-3">
                            <div class="progress-bar bg-warning" role="progressbar" style="width: ${
                                Math.min(100, (suspiciousWalletsImpact * 1.5) + 
                                (Object.values(detailedTimeIntervals).some(txs => 
                                    txs.filter(tx => tx.type === 'TOKEN_SELL').length > 
                                    txs.filter(tx => tx.type === 'TOKEN_BUY').length * 3) ? 40 : 0))}%"></div>
                        </div>
                    </div>
                </div>
                
                <div class="row mt-3">
                    <div class="col-md-6">
                        <h4>洗盘交易风险</h4>
                        <div class="progress mb-3">
                            <div class="progress-bar bg-warning" role="progressbar" style="width: ${
                                Math.min(100, (suspiciousActivityIntervals.reduce((sum, i) => sum + i.washTradingCount, 0) * 5))}%"></div>
                        </div>
                    </div>
                    
                    <div class="col-md-6">
                        <h4>价格波动风险</h4>
                        <div class="progress mb-3">
                            <div class="progress-bar bg-danger" role="progressbar" style="width: ${
                                priceStats.max && priceStats.min ? 
                                Math.min(100, Math.log10(priceStats.max.price / priceStats.min.price) * 10) : 0}%"></div>
                        </div>
                    </div>
                </div>
                
                <div class="alert alert-info mt-4">
                    <h4 class="alert-heading">投资建议</h4>
                    <p>${pumpAndDumpPatterns.length > 0 || suspiciousActivityIntervals.some(i => i.suspiciousScore > 5) || (priceStats.max && priceStats.min && (priceStats.max.price / priceStats.min.price > 1000)) ?
                      '该代币交易存在明显的操纵痕迹，投资者应当保持高度警惕，避免在缺乏深入了解的情况下进行投资。' :
                      '虽然市场波动较大，但未发现明显操纵迹象，投资者在深入了解代币基本面后可以考虑适量参与。'}</p>
                </div>
            </div>
        </div>
        
        <div class="footer">
            <p>分析报告生成时间: ${new Date().toLocaleString()}</p>
            <p>本报告数据仅供参考，不构成投资建议</p>
        </div>
    </div>
    
    <script>
        document.addEventListener('DOMContentLoaded', function() {
            // 创建图表
            ${charts.map(chart => `
            (function() {
                try {
                    const ctx = document.getElementById('${chart.id}').getContext('2d');
                    var chartData = ${JSON.stringify(chart.config)};
                    
                    // 将ISO日期字符串转换回Date对象
                    if (chartData.data.datasets) {
                        chartData.data.datasets.forEach(dataset => {
                            if (dataset.data && dataset.data.length > 0 && typeof dataset.data[0].x === 'string') {
                                dataset.data = dataset.data.map(point => ({
                                    ...point,
                                    x: new Date(point.x)
                                }));
                            }
                        });
                    }
                    
                    new Chart(ctx, chartData);
                    console.log("${chart.id} 创建成功");
                } catch (error) {
                    console.error("创建图表 ${chart.id} 时出错:", error);
                }
            })();
            `).join('\n')}
        });
    </script>
</body>
</html>
    `;
}

// 生成图表HTML并保存
try {
    console.log("开始生成HTML...");
    const chartsHTML = generateHTML();
    console.log(`生成的HTML大小: ${chartsHTML.length} 字节`);
    console.log(`尝试保存到: ${CHART_PATH}`);
    fs.writeFileSync(CHART_PATH, chartsHTML);
    console.log(`可视化图表已成功保存到 ${CHART_PATH}`);
} catch (error) {
    console.error("生成或保存HTML时出错:", error);
    console.error(error.stack); // 打印完整错误堆栈以便调试
}

// ====== 主程序执行 ======
console.log("分析与可视化已完成！");
console.log(`- 分析报告: ${REPORT_PATH}`);
console.log(`- 可视化图表: ${CHART_PATH}`);
console.log("请使用浏览器打开可视化图表查看结果。");
