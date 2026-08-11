import express from 'express';
import cors from 'cors';
import { WebSocketServer } from 'ws';
import http from 'http';
import path from 'path';
import { fileURLToPath } from 'url';
import { LAMPORTS_PER_SOL } from '@solana/web3.js';
import { SniperBot, DEFAULTS } from './src/bot.js';
import 'dotenv/config';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT || 4178;

const app = express();
app.use(cors());
app.use(express.json({ limit: '2mb' }));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

const bot = new SniperBot();

const envConfig = {
  useDevnet: process.env.USE_DEVNET ? process.env.USE_DEVNET === 'true' : undefined,
  simulationMode: process.env.SIMULATION_MODE ? process.env.SIMULATION_MODE === 'true' : undefined,
  mode: process.env.AGENT_MODE || undefined,
  allowRealMode: process.env.ALLOW_REAL_MODE !== undefined ? process.env.ALLOW_REAL_MODE === 'true' : undefined,
  rpcUrl: process.env.RPC_URL || undefined,
  mainnetRpcUrl: process.env.MAINNET_RPC_URL || undefined,
  mainnetWsUrl: process.env.MAINNET_WS_URL || undefined,
  pumpFunProgram: process.env.PUMP_FUN_PROGRAM || undefined,
  jupiterApiKey: process.env.JUPITER_API_KEY || undefined,
  commitment: process.env.COMMITMENT || undefined,
  enableLiveTrading: process.env.ENABLE_LIVE_TRADING !== undefined ? process.env.ENABLE_LIVE_TRADING === 'true' : undefined,
  requireLiveConfirmation: process.env.REQUIRE_LIVE_CONFIRMATION !== undefined ? process.env.REQUIRE_LIVE_CONFIRMATION === 'true' : undefined,
  iUnderstandLiveRisk: process.env.I_UNDERSTAND_LIVE_RISK || undefined,
  paperInitialSol: process.env.PAPER_INITIAL_SOL ? parseFloat(process.env.PAPER_INITIAL_SOL) : undefined,
  paperFeeBps: process.env.PAPER_FEE_BPS ? parseInt(process.env.PAPER_FEE_BPS) : undefined,
  paperSlippageBps: process.env.PAPER_SLIPPAGE_BPS ? parseInt(process.env.PAPER_SLIPPAGE_BPS) : undefined,
  paperLatencyMs: process.env.PAPER_LATENCY_MS ? parseInt(process.env.PAPER_LATENCY_MS) : undefined,
  maxSolPerTrade: process.env.MAX_SOL_PER_TRADE ? parseFloat(process.env.MAX_SOL_PER_TRADE) : undefined,
  maxDailyLossSol: process.env.MAX_DAILY_LOSS_SOL ? parseFloat(process.env.MAX_DAILY_LOSS_SOL) : undefined,
  maxOpenPositions: process.env.MAX_OPEN_POSITIONS ? parseInt(process.env.MAX_OPEN_POSITIONS) : undefined,
  minEntryIntervalMs: process.env.MIN_ENTRY_INTERVAL_MS ? parseInt(process.env.MIN_ENTRY_INTERVAL_MS) : undefined,
  reserveSol: process.env.RESERVE_SOL ? parseFloat(process.env.RESERVE_SOL) : undefined,
  dailyTargetPct: process.env.DAILY_TARGET_PCT ? parseFloat(process.env.DAILY_TARGET_PCT) : undefined,
  dailyLossPct: process.env.DAILY_LOSS_PCT ? parseFloat(process.env.DAILY_LOSS_PCT) : undefined,
  buyAmountSol: process.env.BUY_AMOUNT_SOL ? parseFloat(process.env.BUY_AMOUNT_SOL) : undefined,
  sellTriggerPct: process.env.SELL_TRIGGER_PCT ? parseFloat(process.env.SELL_TRIGGER_PCT) : undefined,
  stopLossPct: process.env.STOP_LOSS_PCT ? parseFloat(process.env.STOP_LOSS_PCT) : undefined,
  slippageBps: process.env.SLIPPAGE_BPS ? parseInt(process.env.SLIPPAGE_BPS) : undefined,
  priorityFeeLamports: process.env.PRIORITY_FEE_LAMPORTS ? parseInt(process.env.PRIORITY_FEE_LAMPORTS) : undefined,
  maxBondingCurve: process.env.MAX_BONDING_CURVE ? parseInt(process.env.MAX_BONDING_CURVE) : undefined,
  autoSellOnBuy: process.env.AUTO_SELL_ON_BUY !== undefined ? process.env.AUTO_SELL_ON_BUY === 'true' : undefined,
  autoSimulation: process.env.AUTO_SIMULATION !== undefined ? process.env.AUTO_SIMULATION === 'true' : undefined,
  minEntryScore: process.env.MIN_ENTRY_SCORE ? parseInt(process.env.MIN_ENTRY_SCORE) : undefined,
  targetProfitPct: process.env.TARGET_PROFIT_PCT ? parseFloat(process.env.TARGET_PROFIT_PCT) : undefined,
  maxPositionTimeSeconds: process.env.MAX_POSITION_TIME_SECONDS ? parseInt(process.env.MAX_POSITION_TIME_SECONDS) : undefined,
  minLiquiditySol: process.env.MIN_LIQUIDITY_SOL ? parseFloat(process.env.MIN_LIQUIDITY_SOL) : undefined,
  minVirtualSolReserves: process.env.MIN_VIRTUAL_SOL_RESERVES ? parseFloat(process.env.MIN_VIRTUAL_SOL_RESERVES) : undefined,
  firstBuyWindowSeconds: process.env.FIRST_BUY_WINDOW_SECONDS ? parseInt(process.env.FIRST_BUY_WINDOW_SECONDS) : undefined,
  firstBuyCount: process.env.FIRST_BUY_COUNT ? parseInt(process.env.FIRST_BUY_COUNT) : undefined,
  minUniqueBuyers: process.env.MIN_UNIQUE_BUYERS ? parseInt(process.env.MIN_UNIQUE_BUYERS) : undefined,
  forceEntryOnNewLaunch: process.env.FORCE_ENTRY_ON_NEW_LAUNCH ? process.env.FORCE_ENTRY_ON_NEW_LAUNCH === 'true' : undefined,
  minBuySellRatio: process.env.MIN_BUY_SELL_RATIO ? parseFloat(process.env.MIN_BUY_SELL_RATIO) : undefined,
  maxBuyerConcentration: process.env.MAX_BUYER_CONCENTRATION ? parseFloat(process.env.MAX_BUYER_CONCENTRATION) : undefined,
  maxImpactPct: process.env.MAX_IMPACT_PCT ? parseFloat(process.env.MAX_IMPACT_PCT) : undefined,
  trailingActivatePct: process.env.TRAILING_ACTIVATE_PCT ? parseFloat(process.env.TRAILING_ACTIVATE_PCT) : undefined,
  trailingRetainPct: process.env.TRAILING_RETAIN_PCT ? parseFloat(process.env.TRAILING_RETAIN_PCT) : undefined,
  holdUntilProfit: process.env.HOLD_UNTIL_PROFIT === 'true',
};
const cleanEnvConfig = Object.fromEntries(Object.entries(envConfig).filter(([, v]) => v !== undefined));
bot.updateConfig(cleanEnvConfig);

const clients = new Set();
wss.on('connection', (ws) => {
  clients.add(ws);
  ws.on('close', () => clients.delete(ws));
  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.type === 'hello') {
        ws.send(JSON.stringify({ type: 'config', data: bot.config }));
        ws.send(JSON.stringify({ type: 'wallet', data: bot.wallet }));
        ws.send(JSON.stringify({ type: 'status', data: statusPayload() }));
      }
    } catch (e) {}
  });
});

const broadcast = (type, data) => {
  const payload = JSON.stringify({ type, data });
  for (const ws of clients) {
    if (ws.readyState === ws.OPEN) ws.send(payload);
  }
};

const statusPayload = () => ({
  state: bot.state, running: bot.running, tradeCount: bot.tradeCount,
  profitLoss: bot.profitLoss, equity: bot.equity, wsConnected: bot.wsConnected, monitored: bot.monitoredTokens.size,
  history: bot.tradeHistory,
  network: bot.config.useDevnet ? 'DEVNET' : 'MAINNET',
  mode: bot.config.mode,
  executionMode: bot.executionMode,
  sendTransactions: bot.sendTransactions,
  metrics: bot.getMetricsSummary(),
  walletBalance: bot.sendTransactions ? (bot.wallet.balanceSOL || 0) : bot.paperCash,
  paperCash: bot.paperCash,
  haltNewEntries: bot.haltNewEntries,
  dayStartEquity: bot.dayStartEquity,
  profitLoss: bot.profitLoss
});

bot.emit = (type, data) => {
  broadcast(type, data);
  if (type === 'wallet' || type === 'config') {}
  if (type === 'status') broadcast('status', data);
};

app.post('/api/wallet/connect', (req, res) => {
  const { secret, viewOnly, publicKey } = req.body || {};
  try {
    if (viewOnly && publicKey) {
      bot.setViewOnlyWallet(publicKey);
      return res.json({ ok: true, publicKey: bot.wallet.publicKey, viewOnly: true });
    }
    if (!secret) return res.status(400).json({ ok: false, error: 'secret é obrigatório' });
    const ok = bot.loadWalletFromSecret(secret);
    if (!ok) return res.status(400).json({ ok: false, error: 'Falha ao carregar carteira' });
    res.json({ ok: true, publicKey: bot.wallet.publicKey });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post('/api/wallet/disconnect', (req, res) => {
  bot.wallet = { provided: false, publicKey: null, balanceSOL: 0, tokens: [] };
  bot.keypair = null;
  bot.emitWallet();
  res.json({ ok: true });
});

app.post('/api/wallet/refresh', async (req, res) => {
  if (!bot.wallet.publicKey) return res.status(400).json({ ok: false, error: 'sem carteira' });
  if (!bot.connection) {
    try { await bot.connect(); } catch (e) { return res.status(500).json({ ok: false, error: e.message }); }
  }
  await bot.refreshWallet();
  res.json({ ok: true, wallet: bot.wallet });
});

app.get('/api/config', (req, res) => res.json(bot.config));

app.post('/api/config', (req, res) => {
  bot.updateConfig(req.body || {});
  res.json({ ok: true, config: bot.config });
});

app.post('/api/bot/start', async (req, res) => {
  const { mode, confirmation } = req.body || {};
  try {
    if (!bot.wallet.publicKey) return res.status(400).json({ ok: false, error: 'Conecte a carteira primeiro' });

    if (mode === 'simulator') {
      await bot.start({ mode: 'simulator' });
      return res.json({ ok: true, executionMode: 'paper_mainnet', sendTransactions: false });
    }

    if (mode === 'real') {
      if (process.env.ALLOW_REAL_MODE !== 'true') {
        return res.status(400).json({ ok: false, error: 'Modo real não permitido pelo servidor (ALLOW_REAL_MODE != true).' });
      }
      if (process.env.ENABLE_LIVE_TRADING !== 'true') {
        return res.status(400).json({ ok: false, error: 'ENABLE_LIVE_TRADING não está true.' });
      }
      if ((process.env.I_UNDERSTAND_LIVE_RISK || '').toUpperCase() !== 'YES') {
        return res.status(400).json({ ok: false, error: 'Você precisa confirmar que entende o risco (I_UNDERSTAND_LIVE_RISK=YES).' });
      }
      if (confirmation !== 'REAL') {
        return res.status(400).json({ ok: false, error: 'Confirmação inválida. Digite REAL para operar.' });
      }
      if (!process.env.MAINNET_RPC_URL) {
        return res.status(400).json({ ok: false, error: 'RPC mainnet não configurado (MAINNET_RPC_URL).' });
      }
      if (!process.env.PUMP_FUN_PROGRAM) {
        return res.status(400).json({ ok: false, error: 'PUMP_FUN_PROGRAM não configurado.' });
      }
      // Usa a MESMA config do simulador - só muda executionMode
      await bot.start({ mode: 'real' });
      return res.json({ ok: true, executionMode: 'live_mainnet', sendTransactions: true });
    }

    return res.status(400).json({ ok: false, error: 'Modo inválido. Use "simulator" ou "real".' });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post('/api/bot/stop', (req, res) => {
  bot.stop();
  res.json({ ok: true });
});

app.get('/api/status', (req, res) => res.json(statusPayload()));
app.get('/api/history', (req, res) => res.json(bot.tradeHistory));
app.get('/api/metrics', (req, res) => res.json(bot.getMetricsSummary()));
app.get('/api/positions', (req, res) => {
  const positions = {};
  for (const [mint, pos] of bot.positions) {
    positions[mint] = pos;
  }
  res.json(positions);
});

app.post('/api/validate', async (req, res) => {
  try {
    await bot.validateRpc();
    await bot.validatePumpProgram();
    res.json({ ok: true, message: 'RPC e PUMP_FUN_PROGRAM válidos', pumpFunProgram: bot.config.pumpFunProgram });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Test endpoint to simulate a token detection and entry
app.post('/api/test/entry', async (req, res) => {
  try {
    const { mint } = req.body || {};
    if (!mint) return res.status(400).json({ ok: false, error: 'mint é obrigatório' });

    // Trava central de entrada (máx posições + cooldown + saldo reservado)
    if (!bot.canEnterTrade()) {
      return res.status(400).json({ ok: false, error: 'Bot em cooldown / máximo de posições / saldo insuficiente' });
    }
    
    // Simulate a new token detection
    const quote = await bot.getBuyQuote(mint, bot.config.buyAmountSol);
    if (!quote?.outAmount) {
      return res.json({ ok: false, error: 'sem quote válido', mint });
    }
    
    const entryPrice = (bot.config.buyAmountSol * LAMPORTS_PER_SOL) / parseInt(quote.outAmount);
    const expectedTokens = parseInt(quote.outAmount) / LAMPORTS_PER_SOL;
    
    const sig = await bot.buyToken(mint, bot.config.buyAmountSol, entryPrice, expectedTokens);
    
    if (sig) {
      bot.lastEntryAt = Date.now();
      const positionId = `POS-TEST-${String(++bot.positionCounter).padStart(6, '0')}`;
      const position = {
        positionId,
        mint,
        status: 'OPEN',
        entryPrice,
        entrySol: bot.config.buyAmountSol,
        tokenAmount: expectedTokens,
        openedAt: new Date().toISOString(),
        targetProfitPct: bot.config.targetProfitPct,
        stopLossPct: bot.config.stopLossPct,
        maxTimeSeconds: bot.config.maxPositionTimeSeconds,
        buySignature: sig,
        buyTime: Date.now(),
        firstBuyAt: null,
        firstLiquidityAt: Date.now(),
        entryAt: Date.now(),
        pnlPct: 0,
        entryScore: 50
      };
      bot.positions.set(mint, position);
      bot.monitoredTokens.set(mint, position);
      if (!bot.sendTransactions) {
        bot.paperCash = Math.max(0, bot.paperCash - bot.config.buyAmountSol);
        bot.equity = bot.paperCash;
      }
      
      bot.logDecision({
        mint, positionId, side: 'BUY', signal: 'test-entry',
        entryPrice, entrySol: bot.config.buyAmountSol,
        expectedTokens, estimatedPriceImpact: 0,
        slippage: bot.config.slippageBps, fee: 0,
        timestamp: new Date().toISOString(), slot: 0
      });
      
      bot.emitStatus();
      bot.monitorPosition(mint);
      
      res.json({ ok: true, positionId, mint, entryPrice, expectedTokens, sig });
    } else {
      res.json({ ok: false, error: 'buyToken retornou null', mint });
    }
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Simula lançamento completo: detecta -> entra -> monitora -> sai (profit)
app.post('/api/test/full-cycle', async (req, res) => {
  try {
    const mint = 'TEST' + Date.now().toString(36) + '1111111111111111111111111111';

    if (!bot.canEnterTrade()) {
      return res.status(400).json({ ok: false, error: 'Bot em cooldown / máximo de posições / saldo insuficiente' });
    }
    
    // 1. Simula quote de compra
    const quote = await bot.getBuyQuote(mint, bot.config.buyAmountSol);
    if (!quote?.outAmount) return res.json({ ok: false, error: 'sem quote' });
    
    const entryPrice = (bot.config.buyAmountSol * LAMPORTS_PER_SOL) / parseInt(quote.outAmount);
    const expectedTokens = parseInt(quote.outAmount) / LAMPORTS_PER_SOL;
    
    // 2. Executa compra
    const sig = await bot.buyToken(mint, bot.config.buyAmountSol, entryPrice, expectedTokens);
    if (!sig) return res.json({ ok: false, error: 'buy falhou' });
    bot.lastEntryAt = Date.now();
    
    const positionId = `POS-TEST-${String(++bot.positionCounter).padStart(6, '0')}`;
    const position = {
      positionId, mint, status: 'OPEN', entryPrice, entrySol: bot.config.buyAmountSol,
      tokenAmount: expectedTokens, openedAt: new Date().toISOString(),
      targetProfitPct: bot.config.targetProfitPct, stopLossPct: bot.config.stopLossPct,
      maxTimeSeconds: bot.config.maxPositionTimeSeconds, buySignature: sig, buyTime: Date.now(),
      firstBuyAt: Date.now(), firstLiquidityAt: Date.now(), entryAt: Date.now(),
      pnlPct: 0, entryScore: 50
    };
    bot.positions.set(mint, position);
    bot.monitoredTokens.set(mint, position);
    if (!bot.sendTransactions) {
      bot.paperCash = Math.max(0, bot.paperCash - bot.config.buyAmountSol);
      bot.equity = bot.paperCash;
    }
    
    bot.logDecision({ mint, positionId, side: 'BUY', signal: 'test-full-cycle', entryPrice, entrySol: bot.config.buyAmountSol, expectedTokens, slippage: bot.config.slippageBps, fee: 0, timestamp: new Date().toISOString(), slot: 0 });
    bot.emitStatus();
    
    // 3. Simula preço subindo (+25%)
    setTimeout(async () => {
      const exitPrice = entryPrice * 1.25;
      const grossPnlPct = 25;
      const feePct = bot.config.paperFeeBps / 100;
      const slippagePct = bot.config.paperSlippageBps / 100;
      const netPnlPct = grossPnlPct - feePct - slippagePct;
      const pnlSOL = bot.config.buyAmountSol * (netPnlPct / 100);
      
      position.status = 'CLOSED';
      position.exitPrice = exitPrice;
      position.exitReason = 'TARGET_PROFIT';
      position.closedAt = new Date().toISOString();
      position.grossPnlPct = grossPnlPct;
      position.netPnlPct = netPnlPct;
      position.pnlSOL = pnlSOL;
      
      if (!bot.sendTransactions) bot.paperCash += bot.config.buyAmountSol + pnlSOL;
      
      bot.recordTrade(mint, netPnlPct, pnlSOL, 'target-profit');
      
      bot.logDecision({ mint, positionId, side: 'SELL', signal: 'TARGET_PROFIT', entryPrice, exitPrice, grossPnl: grossPnlPct, fees: feePct, slippage: slippagePct, netPnl: netPnlPct, profitPct: netPnlPct });
      bot.emitStatus();
      
      setTimeout(() => { bot.positions.delete(mint); bot.monitoredTokens.delete(mint); }, 1000);
    }, 2000);
    
    res.json({ ok: true, positionId, mint, entryPrice, expectedTokens, sig, message: 'Ciclo completo simulado - venda em 2s com +25%' });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post('/api/history/clear', (req, res) => {
  bot.tradeHistory = [];
  bot.profitLoss = 0;
  bot.equity = 0;
  bot.startOfDayEquity = 0;
  bot.tradeCount = 0;
  bot.paperCash = bot.config.paperInitialSol || 1.0;
  bot.equity = bot.paperCash;
  bot.dayStartEquity = bot.paperCash;
  bot.startOfDayEquity = bot.paperCash;
  bot.haltNewEntries = false;
  bot.metrics = {
    tokensDetected: 0, tokensRejected: 0, tokensBought: 0,
    winningTrades: 0, losingTrades: 0, grossProfit: 0,
    totalFees: 0, totalSlippage: 0, netProfit: 0,
    avgPnlPct: 0, maxGain: 0, maxLoss: 0,
    avgPositionTimeMs: 0, avgLatencyMs: 0,
    detectionLatencies: [], validationLatencies: [],
    quoteLatencies: [], executionLatencies: [], totalLatencies: [],
    equityCurve: []
  };
  bot.emitStatus();
  res.json({ ok: true, history: [] });
});

const pub = path.join(__dirname, 'public');
app.use(express.static(pub));
app.get('*', (req, res) => res.sendFile(path.join(pub, 'index.html')));

process.on('unhandledRejection', (reason) => {
  console.error('[UNHANDLED REJECTION]', reason);
});
process.on('uncaughtException', (err) => {
  console.error('[UNCAUGHT EXCEPTION]', err);
});

server.listen(PORT, () => {
  console.log(`SniperAI Dashboard rodando em http://localhost:${PORT}`);
  console.log(`WebSocket: ws://localhost:${PORT}/ws`);
});