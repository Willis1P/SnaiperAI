import {
  Keypair, Connection, PublicKey, VersionedTransaction, LAMPORTS_PER_SOL
} from '@solana/web3.js';
import { AccountLayout, TOKEN_PROGRAM_ID } from '@solana/spl-token';
import { default as WebSocket } from 'ws';
import axios from 'axios';
import bs58 from 'bs58';
import { createConnection, wsTlsOptions, axiosTlsConfig } from './rpc.js';
import { saveState, applyStateToBot, loadState } from './persistence.js';
import { debounce } from './utils.js';
import { analyzeMarket, classifyFromAnalysis } from './market-analyst.js';

const PUMP_FUN_PROGRAM = new PublicKey('6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P');
const WRAPPED_SOL = 'So11111111111111111111111111111111111111112';
const JUPITER_QUOTE_API = 'https://api.jup.ag/swap/v1/quote';
const JUPITER_SWAP_API = 'https://api.jup.ag/swap/v1/swap';
const UPGRADEABLE_LOADER = 'BPFLoaderUpgradeab1e11111111111111111111111';

const PUMP_FUN_CREATE_DISCRIMINATOR = Buffer.from([24, 30, 200, 40, 5, 119, 111, 167]);

// Mints conhecidos que não são lançamentos Pump.fun (evita falsos positivos no polling)
const KNOWN_SKIP_MINTS = new Set([
  WRAPPED_SOL,
  'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', // USDC
  'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', // USDT
]);

const MODES = ['mock', 'paper_mainnet', 'simulate_rpc', 'live_mainnet'];

const DEFAULTS = {
  allowRealMode: false,
  agentMode: 'paper_mainnet',
  simulationMode: true,
  useDevnet: false,
  enableLiveTrading: false,
  requireLiveConfirmation: true,
  iUnderstandLiveRisk: false,
  commitment: 'confirmed',
  mainnetRpcUrl: '',
  mainnetWsUrl: '',
  devnetRpcUrl: '',
  devnetWsUrl: '',
  mainnetRpcFallbackUrl: 'https://api.mainnet-beta.solana.com',
  pumpFunProgram: PUMP_FUN_PROGRAM.toString(),
  jupiterApiKey: '',
  jupiterBaseUrl: '',
  jupiterQuoteApi: '',
  jupiterSwapApi: '',
  buyAmountSol: 0.015,
  sellTriggerPct: 20,
  stopLossPct: 8,
  slippageBps: 500,
  priorityFeeLamports: 10000,
  maxBondingCurve: 100,
  autoSellOnBuy: true,
  monitorIntervalMs: 5000,
  pollingIntervalMs: 30000,
  rpcThrottleMs: 300,
  paperInitialSol: 1.0,
  paperFeeBps: 100,
  paperSlippageBps: 50,
  paperLatencyMs: 100,
  maxSolPerTrade: 0.05,
  maxDailyLossSol: 0.20,
  maxOpenPositions: 10,
  minEntryIntervalMs: 8000,
  reserveSol: 0.05,
  dailyTargetPct: 10,
  dailyLossPct: 20,
  minEntryScore: 40,
  targetProfitPct: 20,
  stopLossPct: 8,
  maxPositionTimeSeconds: 300,
  minLiquiditySol: 1,
  minVirtualSolReserves: 5,
  firstBuyWindowSeconds: 10,
  firstBuyCount: 1,
  minUniqueBuyers: 1,
  forceEntryOnNewLaunch: false,
  maxNewLaunchAgeSeconds: 30,
  minBuySellRatio: 1.2,
  maxBuyerConcentration: 0.7,
  maxImpactPct: 2.0,
  stopLossGraceSeconds: 12,
  trailingActivatePct: 3,
  trailingRetainPct: 50,
  partialTakeProfitPct: 10,
  partialTakeProfitSharePct: 50,
  breakevenActivatePct: 4,
  breakevenFloorPct: 1.5,
  holdUntilProfit: false,
  timeoutOnlyOnProfit: true,
  rpcUseFallbackOn429: true,
  rpcPreferFallback: false,
  autoSimulation: true,
  autoSimulationIntervalMs: 15000,
  autoSimulationWinRate: 0.65,
  recoveryEnabled: true,
  recoveryMaxBets: 3,
  recoverySizePct: 2.0,
  entryScoreWeights: {
    newToken: 10,
    tradable: 15,
    liquidity: 15,
    firstBuys: 25,
    buyPressure: 20,
    uniqueBuyers: 10,
    lowRisk: 5
  }
};

export class SniperBot {
  constructor(emitter) {
    this.emit = emitter || (() => {});
    this.connection = null;
    this.keypair = null;
    this.ws = null;
    this.config = { ...DEFAULTS };
    this.sendTransactions = false;
    this.executionMode = 'paper_mainnet';
    this.wallet = { provided: false, publicKey: null, balanceSOL: 0, tokens: [] };
    this.state = 'idle';
    this.running = false;
    this.haltNewEntries = false;
    this.lastEntryAt = 0;
    this.tradeCount = 0;
    this.profitLoss = 0;
    this.tradeHistory = [];
    this.decisionLog = [];
    this.equity = 0;
    this.startOfDayEquity = 0;
    this.dayStartEquity = 0;
    // Sistema de recuperação de saldo: acumula perdas e aumenta a próxima entrada até recuperar
    this.recoveryLoss = 0;
    this.recoveryBets = 0;
    this.paperCash = this.config.paperInitialSol || 0;
    this.positions = new Map();
    this.monitoredTokens = new Map();
    this.processedMints = new Set();
    this._positionMonitors = new Map();
    this.wsConnected = false;
    this.startOfDayPnl = 0;
    this.lastSlot = 0;

    this.metrics = {
      tokensDetected: 0,
      tokensRejected: 0,
      tokensBought: 0,
      winningTrades: 0,
      losingTrades: 0,
      grossProfit: 0,
      totalFees: 0,
      totalSlippage: 0,
      netProfit: 0,
      avgPnlPct: 0,
      maxGain: 0,
      maxLoss: 0,
      avgPositionTimeMs: 0,
      avgLatencyMs: 0,
      detectionLatencies: [],
      validationLatencies: [],
      quoteLatencies: [],
      executionLatencies: [],
      totalLatencies: [],
      equityCurve: [],  // Histórico de saldo para gráfico
      rejectionStats: {}
    };

    this.positionCounter = 0;
    this._recheckQueue = new Map();

    // Throttle global de chamadas RPC para respeitar quota do RPC provider
    this._rpcQueue = Promise.resolve();
    this._rpcThrottleMs = this.config.rpcThrottleMs || 300;
    this._rpcPausedUntil = 0;
    this._activeRpcUrl = null;
    this._connectionRpcUrl = null;
    this._rpcValidatedAt = 0;
    this._heliusRateLimitedAt = 0;
    this._starting = false;

    // Throttle de chamadas à Jupiter (quote) — plano grátis é restritivo sem API key
    this._jupQueue = Promise.resolve();
    this._jupThrottleMs = 400;
    this._persist = debounce(() => saveState(this), 1500);
  }

  _recordRejection(reason) {
    this.metrics.tokensRejected++;
    const key = String(reason || 'unknown').split(':')[0].trim().slice(0, 48);
    this.metrics.rejectionStats[key] = (this.metrics.rejectionStats[key] || 0) + 1;
  }

  _scheduleRecheck(mint, delayMs = 12000) {
    if (this._recheckQueue.has(mint) || !this.running) return;
    this._recheckQueue.set(mint, setTimeout(async () => {
      this._recheckQueue.delete(mint);
      if (!this.running || this.positions.has(mint)) return;
      this.processedMints.delete(mint);
      this.log(`🔄 Re-analisando ${mint.slice(0, 16)} (aguardando confirmação de fluxo)...`, 'info');
      await this.onNewToken(mint);
    }, delayMs));
  }

  async loadPersistedState() {
    const state = await loadState();
    if (applyStateToBot(this, state)) {
      this.log(`Estado restaurado: ${this.tradeHistory.length} trades`, 'info');
      this.emitStatus();
    }
  }

  persist() { this._persist(); }

  _markRpcRateLimited() {
    this._heliusRateLimitedAt = Date.now();
    this._rpcValidatedAt = 0;
  }

  _shouldPreferFallbackRpc() {
    if (this.sendTransactions || this.executionMode === 'live_mainnet') return false;
    if (this.config.rpcPreferFallback) return true;
    if (this.config.rpcUseFallbackOn429 && this._heliusRateLimitedAt) {
      return Date.now() - this._heliusRateLimitedAt < 15 * 60 * 1000;
    }
    return false;
  }

  _resetConnection() {
    this.connection = null;
    this._connectionRpcUrl = null;
  }

  _fallbackRpcUrl() {
    return this.config.mainnetRpcFallbackUrl || process.env.MAINNET_RPC_FALLBACK_URL || 'https://api.mainnet-beta.solana.com';
  }

  _switchRpcToFallback() {
    const fallback = this._fallbackRpcUrl();
    if (this._activeRpcUrl === fallback) return false;
    this._markRpcRateLimited();
    this._activeRpcUrl = fallback;
    this._rpcPausedUntil = 0;
    this._resetConnection();
    this.log('↪️ RPC Helius com rate limit — usando fallback público.', 'warn');
    return true;
  }

  _shouldUseLightweightFlow() {
    if (this._rpcPausedUntil && Date.now() < this._rpcPausedUntil) return true;
    if (this._activeRpcUrl === this._fallbackRpcUrl()) return true;
    if (this._heliusRateLimitedAt && Date.now() - this._heliusRateLimitedAt < 15 * 60 * 1000) return true;
    return false;
  }

  // Estima fluxo a partir da bonding curve quando RPC não consegue ler transações (429)
  _estimateFlowFromCurve(realSol, virtualSol) {
    let buyers = 0;
    if (realSol >= 3) buyers = 4;
    else if (realSol >= 1.5) buyers = 3;
    else if (realSol >= 0.8) buyers = 2;
    else if (realSol >= 0.5) buyers = 1;

    return {
      count: buyers,
      sellCount: 0,
      firstBuy: null,
      buys: [],
      buyVolume: Math.max(realSol, 0),
      sellVolume: 0,
      uniqueBuyers: buyers,
      uniqueSellers: 0,
      buySellRatio: buyers > 0 ? 2.5 : 0,
      topBuyerShare: buyers >= 3 ? 0.4 : (buyers === 2 ? 0.55 : (buyers === 1 ? 0.8 : 1)),
      timeSpanSeconds: buyers >= 2 ? 8 : 0,
      avgBuySize: 0,
      avgSellSize: 0,
      rpcDegraded: true,
      estimated: true
    };
  }

  _parseCurveReserves(data) {
    if (!data || data.length < 40) return null;
    const virtualToken = Number(data.readBigUInt64LE(8)) / 1e6;
    const virtualSol = Number(data.readBigUInt64LE(16)) / LAMPORTS_PER_SOL;
    const realToken = Number(data.readBigUInt64LE(24)) / 1e6;
    const realSol = Number(data.readBigUInt64LE(32)) / LAMPORTS_PER_SOL;
    if (virtualToken <= 0 || virtualSol <= 0) return null;
    return { virtualToken, virtualSol, realToken, realSol };
  }

  _simulateCurveBuy(solIn, vSol, vToken) {
    const k = vSol * vToken;
    const newVSol = vSol + solIn;
    const newVToken = k / newVSol;
    const tokensOut = Math.max(0, vToken - newVToken);
    const effectivePrice = tokensOut > 0 ? solIn / tokensOut : 0;
    return { tokensOut, effectivePrice, virtualSol: newVSol, virtualToken: newVToken };
  }

  _simulateCurveSell(tokenIn, vSol, vToken) {
    const k = vSol * vToken;
    const newVToken = vToken + tokenIn;
    const newVSol = k / newVToken;
    const solOut = Math.max(0, vSol - newVSol);
    const effectivePrice = tokenIn > 0 ? solOut / tokenIn : 0;
    return { solOut, effectivePrice };
  }

  async fetchBondingCurveReserves(mint) {
    try {
      const bondingCurve = await this.findBondingCurve(mint);
      if (!bondingCurve) return null;
      const info = await this._throttledRpc(() =>
        this.connection.getAccountInfo(new PublicKey(bondingCurve))
      );
      return this._parseCurveReserves(info?.data);
    } catch (e) {
      return null;
    }
  }

  async _paperPositionPnl(mint, tokenAmount, entrySol, pos = null) {
    const curve = await this.fetchBondingCurveReserves(mint);
    if (!curve || tokenAmount <= 0) return null;

    const spot = curve.virtualSol / curve.virtualToken;
    let grossPnlPct;
    let solOut;

    // Monitoramento por spot price (estável) — sell sim só na saída real
    if (pos?.entrySpotPrice > 0) {
      grossPnlPct = ((spot / pos.entrySpotPrice) - 1) * 100;
      // Anomaly guard: ignora quedas >25% em um tick (RPC instável)
      if (pos.lastMarkPct !== undefined && grossPnlPct < pos.lastMarkPct - 25) {
        const curve2 = await this.fetchBondingCurveReserves(mint);
        if (curve2) {
          const spot2 = curve2.virtualSol / curve2.virtualToken;
          const retryPct = ((spot2 / pos.entrySpotPrice) - 1) * 100;
          if (Math.abs(retryPct - grossPnlPct) > 15) {
            grossPnlPct = pos.lastMarkPct;
          } else {
            grossPnlPct = retryPct;
          }
        } else {
          grossPnlPct = pos.lastMarkPct;
        }
      }
      solOut = entrySol * (1 + grossPnlPct / 100);
    } else {
      const sell = this._simulateCurveSell(tokenAmount, curve.virtualSol, curve.virtualToken);
      solOut = sell.solOut;
      grossPnlPct = entrySol > 0 ? ((solOut - entrySol) / entrySol) * 100 : 0;
    }

    return { solOut, grossPnlPct, curve, spot };
  }

  async _paperExitSolOut(mint, tokenAmount, entrySol) {
    const curve = await this.fetchBondingCurveReserves(mint);
    if (!curve || tokenAmount <= 0) return entrySol;
    const { solOut } = this._simulateCurveSell(tokenAmount, curve.virtualSol, curve.virtualToken);
    return solOut;
  }

  // Espaça chamadas RPC (getTransaction/getSignatures/getAccountInfo) para evitar 429
  _throttledRpc(fn) {
    const run = this._rpcQueue.then(async () => {
      try {
        await this.ensureRpcConnection();
        const res = await fn();
        await new Promise(r => setTimeout(r, this._rpcThrottleMs));
        return res;
      } catch (e) {
        const msg = String(e?.message || e);
        if (msg.includes('429') && !this.sendTransactions && this._switchRpcToFallback()) {
          await this.ensureRpcConnection();
          const res = await fn();
          await new Promise(r => setTimeout(r, this._rpcThrottleMs));
          return res;
        }
        throw e;
      }
    });
    this._rpcQueue = run.catch(() => {});
    return run;
  }

  // Espaça chamadas à Jupiter para evitar 429 do plano grátis
  _throttledJupiter(fn) {
    const run = this._jupQueue.then(async () => {
      const res = await fn();
      await new Promise(r => setTimeout(r, this._jupThrottleMs));
      return res;
    });
    this._jupQueue = run.catch(() => {});
    return run;
  }

  log(message, type = 'info') {
    this.emit('log', { ts: new Date().toISOString(), message, type });
    console.log(`[${type.toUpperCase()}] ${message}`);
  }

  setState(s) { this.state = s; this.emit('state', s); }
  emitWallet() { this.emit('wallet', { ...this.wallet }); }
  emitConfig() { this.emit('config', { ...this.config }); }
  emitStatus() {
    this.emit('status', {
      state: this.state, running: this.running, tradeCount: this.tradeCount,
      profitLoss: this.profitLoss, equity: this.equity,
      network: this.config.useDevnet ? 'DEVNET' : 'MAINNET',
      mode: this.config.mode,
      executionMode: this.executionMode,
      sendTransactions: this.sendTransactions,
      wsConnected: this.wsConnected,
      monitored: this.monitoredTokens.size,
      metrics: this.getMetricsSummary(),
      history: this.tradeHistory,
      paperCash: this.paperCash,
      haltNewEntries: this.haltNewEntries,
      dayStartEquity: this.dayStartEquity,
      profitLoss: this.profitLoss,
      walletBalance: this.sendTransactions ? (this.wallet.balanceSOL || 0) : this.paperCash
    });
  }

getMetricsSummary() {
    const { tokensDetected, tokensRejected, tokensBought, winningTrades, losingTrades,
      grossProfit, totalFees, totalSlippage, netProfit, avgPnlPct, maxGain, maxLoss,
      avgPositionTimeMs, avgLatencyMs, equityCurve } = this.metrics;
    
    const walletBalance = this.sendTransactions && this.executionMode === 'live_mainnet' 
      ? (this.wallet.balanceSOL || 0) 
      : this.paperCash;
    
    return {
      tokensDetected, tokensRejected, tokensBought, winningTrades, losingTrades,
      winRate: tokensBought > 0 ? ((winningTrades / tokensBought) * 100).toFixed(1) : 0,
      grossProfit: parseFloat(grossProfit.toFixed(6)),
      totalFees: parseFloat(totalFees.toFixed(6)),
      totalSlippage: parseFloat(totalSlippage.toFixed(6)),
      netProfit: parseFloat(netProfit.toFixed(6)),
      avgPnlPct: parseFloat(avgPnlPct.toFixed(2)),
      maxGain: parseFloat(maxGain.toFixed(2)),
      maxLoss: parseFloat(maxLoss.toFixed(2)),
      avgPositionTimeMs: Math.round(avgPositionTimeMs),
      avgLatencyMs: Math.round(avgLatencyMs),
      equityCurve: equityCurve.slice(-200),
      currentEquity: parseFloat(this.equity.toFixed(6)),
      paperCash: parseFloat(this.paperCash.toFixed(6)),
      walletBalance: parseFloat(walletBalance.toFixed(6)),
      rejectionStats: { ...(this.metrics.rejectionStats || {}) }
    };
  }

  logDecision(entry) {
    const base = {
      timestamp: new Date().toISOString(),
      mode: this.config.mode,
      mint: entry.mint || null,
      side: entry.side || null,
      signal: entry.signal || null,
      requestId: this.uid()
    };
    const full = { ...base, ...entry };
    this.log(`[DECISION] ${JSON.stringify(full)}`, 'info');
    this.emit('log', { ts: new Date().toISOString(), message: `📊 decision: ${full.side || ''} ${full.mint || ''} signal=${full.signal || ''} price=${full.price ?? ''}`, type: 'info' });
  }

  recordLatency(type, ms) {
    const arr = this.metrics[`${type}Latencies`];
    if (arr) { arr.push(ms); if (arr.length > 1000) arr.shift(); }
    const all = this.metrics.totalLatencies;
    all.push(ms); if (all.length > 1000) all.shift();
    this.metrics.avgLatencyMs = all.reduce((a, b) => a + b, 0) / all.length;
  }

  recordTrade(mint, pnlPct, pnlSOL, side = 'auto', extra = {}) {
    // In paper mode, equity tracks virtual P&L from paperCash
    // In real mode, equity tracks actual wallet balance
    if (this.executionMode === 'paper_mainnet' || !this.sendTransactions) {
      if (this.equity === 0) this.equity = this.config.paperInitialSol;
      this.equity = this.paperCash;
    } else {
      if (this.startOfDayEquity === 0) this.startOfDayEquity = this.wallet.balanceSOL || 0;
      this.equity = (this.wallet.balanceSOL || 0) + (this.metrics.netProfit || 0);
    }
    this.profitLoss += pnlSOL;
    this.tradeCount++;
    
    this.metrics.netProfit += pnlSOL;
    if (pnlPct > 0) { this.metrics.winningTrades++; this.metrics.grossProfit += Math.abs(pnlSOL); }
    else { this.metrics.losingTrades++; this.metrics.grossProfit -= Math.abs(pnlSOL); }

    // Sistema de recuperação de saldo:
    // - Em perda: acumula o prejuízo e incrementa o contador de apostas de recuperação
    // - Em lucro: paga parte da dívida; se zerar/positivar, reseta o ciclo
    if (this.config.recoveryEnabled) {
      if (pnlSOL < 0) {
        this.recoveryLoss = Math.min(this.recoveryLoss + Math.abs(pnlSOL), this.config.maxDailyLossSol || 0.5);
        this.recoveryBets = Math.min(this.recoveryBets + 1, this.config.recoveryMaxBets || 3);
      } else {
        this.recoveryLoss = Math.max(0, this.recoveryLoss - pnlSOL);
        if (this.recoveryLoss <= 0) {
          this.recoveryLoss = 0;
          this.recoveryBets = 0;
        }
      }
    }
    this.metrics.maxGain = Math.max(this.metrics.maxGain, pnlPct);
    this.metrics.maxLoss = Math.min(this.metrics.maxLoss, pnlPct);
    this.metrics.avgPnlPct = this.tradeCount > 0 ? (this.metrics.netProfit / (this.startOfDayEquity || this.config.paperInitialSol) * this.tradeCount) * 100 : 0;
    
    // Equity curve para gráfico de evolução
    this.metrics.equityCurve.push({
      ts: Date.now(),
      equity: parseFloat(this.equity.toFixed(6)),
      pnlSOL: parseFloat(pnlSOL.toFixed(6)),
      pnlPct: parseFloat(pnlPct.toFixed(2)),
      tradeCount: this.tradeCount
    });
    if (this.metrics.equityCurve.length > 1000) this.metrics.equityCurve.shift();

    const entry = {
      ts: Date.now(),
      mint: String(mint || ''),
      pnlPct: parseFloat(pnlPct.toFixed(2)),
      pnlSOL: parseFloat(pnlSOL.toFixed(6)),
      equity: parseFloat(this.equity.toFixed(6)),
      mode: this.executionMode || this.config.mode,
      sentToChain: this.sendTransactions,
      executionMode: this.executionMode || 'paper_mainnet',
      side,
      entrySol: extra.entrySol || 0,
      exitReason: extra.exitReason || side,
      entryScore: extra.entryScore || 0,
      holdTimeMs: extra.holdTimeMs || 0
    };
    this.tradeHistory.push(entry);
    if (this.tradeHistory.length > 500) this.tradeHistory.shift();
    this.emit('trade', entry);
    this.emitStatus();
    this.persist();
    return entry;
  }

  rpcUrl() {
    if (this.config.useDevnet) {
      return this.config.devnetRpcUrl?.trim() || 'https://api.devnet.solana.com';
    }
    if (this._activeRpcUrl) return this._activeRpcUrl;
    if (this.config.mainnetRpcUrl) return this.config.mainnetRpcUrl;
    return this.config.mainnetRpcFallbackUrl || 'https://api.mainnet-beta.solana.com';
  }
  wsUrl() {
    const httpUrl = this.rpcUrl();
    if (this.config.useDevnet && this.config.devnetWsUrl?.trim()) {
      return this.config.devnetWsUrl.trim();
    }
    const onPrimary = this.config.mainnetRpcUrl && httpUrl === this.config.mainnetRpcUrl;
    if (!this.config.useDevnet && this.config.mainnetWsUrl && onPrimary) {
      return this.config.mainnetWsUrl;
    }
    return httpUrl.replace('https://', 'wss://').replace('http://', 'ws://');
  }

  _rpcCandidates() {
    const urls = [];
    const primary = this.config.mainnetRpcUrl?.trim();
    const fallback = this._fallbackRpcUrl();
    if (this._shouldPreferFallbackRpc() && fallback) urls.push(fallback);
    if (primary && !urls.includes(primary)) urls.push(primary);
    if (fallback && !urls.includes(fallback)) urls.push(fallback);
    return urls;
  }

  async _probeRpc(rpcUrl) {
    const conn = createConnection(rpcUrl, { commitment: 'confirmed' });
    const slot = await conn.getSlot();
    return { conn, slot };
  }

  async validateRpc(options = {}) {
    const quick = options.quick === true;
    const force = options.force === true;
    if (this.config.useDevnet) {
      this.log('Validação de RPC: usando devnet (sem exigência de MAINNET_RPC_URL).', 'info');
      return true;
    }

    const cacheMs = quick ? 60 * 1000 : 10 * 60 * 1000;
    if (!force && this._activeRpcUrl && this._rpcValidatedAt && (Date.now() - this._rpcValidatedAt) < cacheMs) {
      if (!quick) this.log(`RPC em cache OK — ${this._activeRpcUrl.split('?')[0]}`, 'info');
      return true;
    }

    const candidates = (this.sendTransactions || this.executionMode === 'live_mainnet')
      ? [this.config.mainnetRpcUrl?.trim()].filter(Boolean)
      : this._rpcCandidates();
    if (!candidates.length) throw new Error('MAINNET_RPC_URL não configurado.');

    let lastErr = null;
    for (const rpcUrl of candidates) {
      if (rpcUrl.includes('api-key=https://') || rpcUrl.includes('api-key=wss://')) {
        throw new Error('MAINNET_RPC_URL malformado — use apenas: https://mainnet.helius-rpc.com/?api-key=SUA_CHAVE');
      }
      try {
        const { slot } = await this._probeRpc(rpcUrl);
        this._activeRpcUrl = rpcUrl;
        this._rpcValidatedAt = Date.now();
        this._resetConnection();
        const label = rpcUrl === this.config.mainnetRpcUrl ? 'primário' : 'fallback';
        this.log(`RPC mainnet OK (${label}) — slot:${slot}`, 'success');
        if (rpcUrl !== this.config.mainnetRpcUrl) {
          this.log('⚠️ Helius indisponível — usando RPC público de fallback.', 'warn');
        }
        return true;
      } catch (e) {
        lastErr = e;
        const msg = e.message || String(e);
        if (msg.includes('429')) {
          this._markRpcRateLimited();
          this.log(`⏳ RPC 429 em ${rpcUrl.split('?')[0]} — tentando próximo...`, 'warn');
          continue;
        }
        if (!quick) throw e;
      }
    }

    const msg = lastErr?.message || 'Nenhum RPC disponível';
    if (msg.includes('429')) {
      throw new Error('Cota RPC esgotada (429). Aguarde 2–5 minutos ou use outra API key Helius.');
    }
    throw new Error(`Falha ao validar RPC mainnet: ${msg}`);
  }

  async ensureRpcConnection() {
    if (!this._activeRpcUrl) {
      this._activeRpcUrl = this._shouldPreferFallbackRpc()
        ? this._fallbackRpcUrl()
        : (this.config.mainnetRpcUrl?.trim() || this._fallbackRpcUrl());
    }
    const url = this.rpcUrl();
    if (!this.connection || this._connectionRpcUrl !== url) {
      this.connection = createConnection(url, { commitment: 'confirmed' });
      this._connectionRpcUrl = url;
    }
    return this.connection;
  }

  loadWalletFromSecret(secret) {
    try {
      let arr;
      if (typeof secret === 'string') {
        const t = secret.trim();
        arr = t.startsWith('[') ? JSON.parse(t) : bs58.decode(t);
      } else if (Array.isArray(secret)) {
        arr = secret;
      } else {
        throw new Error('Formato não suportado. Use base58 ou array JSON.');
      }
      this.keypair = Keypair.fromSecretKey(Uint8Array.from(arr));
      this.wallet.provided = true;
      this.wallet.publicKey = this.keypair.publicKey.toString();
      this.log(`Carteira carregada: ${this.wallet.publicKey}`, 'success');
      this.emitWallet();
      return true;
    } catch (e) {
      this.log(`Falha ao carregar carteira: ${e.message}`, 'error');
      return false;
    }
  }

  setViewOnlyWallet(publicKeyStr) {
    this.wallet.provided = true;
    this.wallet.viewOnly = true;
    this.wallet.publicKey = publicKeyStr;
    this.keypair = null;
    if (this.paperCash <= 0) this.paperCash = this.config.paperInitialSol;
    this.wallet.paperCash = this.paperCash;
    this.log(`Modo somente leitura: ${publicKeyStr}`, 'info');
    this.emitWallet();
  }

  async connect() {
    if (!this.wallet.publicKey) throw new Error('Nenhuma carteira carregada');
    await this.ensureRpcConnection();
    try {
      await this._throttledRpc(() => this.connection.getLatestBlockhash());
    } catch (e) {
      if (String(e.message).includes('429') && this._switchRpcToFallback()) {
        await this.ensureRpcConnection();
        await this._throttledRpc(() => this.connection.getLatestBlockhash());
      } else {
        throw e;
      }
    }
    this.log(`Conectado à rede ${this.config.useDevnet ? 'DEVNET' : 'MAINNET'} (${this.rpcUrl().split('?')[0]})`, 'success');
    await this.refreshWallet();
  }

  async refreshWallet() {
    if (!this.wallet.publicKey) return;
    try {
      await this.ensureRpcConnection();
      const pubkey = new PublicKey(this.wallet.publicKey);
      const bal = await this._throttledRpc(() => this.connection.getBalance(pubkey));
      this.wallet.balanceSOL = bal / LAMPORTS_PER_SOL;

      if (!this.sendTransactions) {
        if (this.paperCash <= 0) this.paperCash = this.config.paperInitialSol;
        this.wallet.paperCash = this.paperCash;
      } else {
        const tokenAccounts = await this._throttledRpc(() =>
          this.connection.getTokenAccountsByOwner(pubkey, { programId: TOKEN_PROGRAM_ID })
        );
        this.wallet.tokens = tokenAccounts.value.map(acc => {
          const d = AccountLayout.decode(acc.account.data);
          return { mint: new PublicKey(d.mint).toString(), amount: Number(d.amount) / 1e6 };
        }).filter(t => t.amount > 0.0001);
      }
      this.emitWallet();
    } catch (e) {
      if (!this.sendTransactions && this.paperCash > 0) {
        this.wallet.paperCash = this.paperCash;
        this.wallet.balanceSOL = this.wallet.balanceSOL || 0;
        this.emitWallet();
      }
      this.log(`Erro ao atualizar carteira: ${e.message}`, 'error');
    }
  }

  updateConfig(newConfig) {
    if (newConfig.mode && !MODES.includes(newConfig.mode)) {
      delete newConfig.mode;
      this.log(`Modo "${newConfig.mode}" inválido. Mantendo "${this.config.mode}".`, 'error');
    }
    if (newConfig.entryScoreWeights) {
      newConfig.entryScoreWeights = { ...this.config.entryScoreWeights, ...newConfig.entryScoreWeights };
    }
    this.config = { ...this.config, ...newConfig };
    this.log('Configurações atualizadas.', 'info');
    this.emitConfig();
    this.emitStatus();
  }

  assertSafeMode() {
    if (this.sendTransactions || this.config.mode === 'live_mainnet') {
      if (!this.config.allowRealMode) throw new Error('ALLOW_REAL_MODE precisa ser true para operar real.');
      if (!this.config.enableLiveTrading) throw new Error('ENABLE_LIVE_TRADING precisa estar true para operar real.');
      if (!this.config.iUnderstandLiveRisk || String(this.config.iUnderstandLiveRisk).toUpperCase() !== 'YES') {
        throw new Error('Para operar real, defina I_UNDERSTAND_LIVE_RISK=YES.');
      }
      if (!this.config.mainnetRpcUrl) throw new Error('MAINNET_RPC_URL é obrigatório em modo live.');
      if (!this.config.pumpFunProgram) throw new Error('PUMP_FUN_PROGRAM é obrigatório em modo live.');
      if (!this.keypair) throw new Error('AMBIENTE REAL: necessária a CHAVE PRIVADA da carteira para assinar transações.');
    }
  }

  async validatePumpProgram() {
    const pidStr = this.config.pumpFunProgram;
    if (!pidStr) throw new Error('PUMP_FUN_PROGRAM não configurado.');
    let pid;
    try { pid = new PublicKey(pidStr); } catch (e) { throw new Error(`PUMP_FUN_PROGRAM inválido: ${pidStr}`); }

    await this.ensureRpcConnection();
    let info;
    try {
      info = await this._throttledRpc(() => this.connection.getAccountInfo(pid));
    } catch (e) {
      const msg = String(e?.message || e);
      if (msg.includes('429') && this._switchRpcToFallback()) {
        await this.ensureRpcConnection();
        info = await this._throttledRpc(() => this.connection.getAccountInfo(pid));
      } else {
        throw e;
      }
    }
    if (!info) throw new Error(`Programa não encontrado na ${this.config.useDevnet ? 'devnet' : 'mainnet'}: ${pidStr}.`);
    this.log(`Validação Pump.fun — executable=${info.executable} owner=${info.owner.toBase58()} dataLen=${info.data.length}`, 'info');
    if (!info.executable) throw new Error('A conta do programa existe, mas NÃO é executable.');
    if (info.owner.toBase58() !== UPGRADEABLE_LOADER) {
      this.log(`⚠️ Aviso: owner inesperado (${info.owner.toBase58()}) para programa.`, 'warn');
    }
    return true;
  }

  _jupiterQuoteUrl() {
    if (this.config.jupiterQuoteApi) return this.config.jupiterQuoteApi;
    const base = this.config.jupiterBaseUrl?.trim();
    if (base) return `${base.replace(/\/$/, '')}/quote`;
    return JUPITER_QUOTE_API;
  }

  _jupiterSwapUrl() {
    if (this.config.jupiterSwapApi) return this.config.jupiterSwapApi;
    const base = this.config.jupiterBaseUrl?.trim();
    if (base) return `${base.replace(/\/$/, '')}/swap`;
    return JUPITER_SWAP_API;
  }

  async getJupitQuote(inputMint, outputMint, amountLamports, slippageBps) {
    const apiKey = this.config.jupiterApiKey;
    const headers = apiKey ? { 'Authorization': `Bearer ${apiKey}` } : {};
    const url = `${this._jupiterQuoteUrl()}?inputMint=${inputMint}&outputMint=${outputMint}&amount=${amountLamports}&slippageBps=${slippageBps}&onlyDirectRoutes=false`;
    
    try {
      const res = await this._throttledJupiter(() => axios.get(url, { timeout: 5000, headers, ...axiosTlsConfig() }));
      return res.data;
    } catch (e) {
      // Mock fallback APENAS em devnet/teste. Em mainnet, NUNCA fabricar preço
      // (preço falso gera PnL irreal). Falha = sem quote disponível.
      if (this.config.useDevnet) {
        const priceImpact = 0.01;
        const fee = Math.floor(amountLamports * slippageBps / 10000);
        const outAmount = Math.floor(amountLamports * 0.98); // ~2% price impact mock
        this.log(`[MOCK QUOTE] ${inputMint.slice(0,8)}→${outputMint.slice(0,8)} out=${outAmount}`, 'debug');
        return {
          inputMint,
          outputMint,
          inAmount: amountLamports.toString(),
          outAmount: outAmount.toString(),
          otherAmountThreshold: Math.floor(outAmount * (1 - slippageBps / 10000)).toString(),
          swapMode: 'ExactIn',
          slippageBps,
          priceImpactPct: (priceImpact * 100).toString(),
          routePlan: [],
          swapTransaction: 'mock'
        };
      }
      this.log(`[QUOTE FALHOU] ${inputMint.slice(0,8)}→${outputMint.slice(0,8)}: ${e.message || e}`, 'warn');
      return null;
    }
  }

  async buildJupiterSwapTx(inputMint, outputMint, amountLamports, slippageBps) {
    const quote = await this.getJupitQuote(inputMint, outputMint, amountLamports, slippageBps);
    if (!quote?.swapTransaction) throw new Error('Jupiter: transação de swap não retornada');
    const apiKey = this.config.jupiterApiKey;
    const headers = apiKey ? { 'Authorization': `Bearer ${apiKey}` } : {};
    const res = await this._throttledJupiter(() => axios.post(this._jupiterSwapUrl(), {
      quoteResponse: quote,
      userPublicKey: this.keypair.publicKey.toString(),
      wrapAndUnwrapSol: true,
      prioritizationFeeLamports: this.config.priorityFeeLamports,
      dynamicComputeUnitLimit: true
    }, { timeout: 15000, headers, ...axiosTlsConfig() }));
    if (!res.data?.swapTransaction) throw new Error('Swap tx inválida');
    const buf = Buffer.from(res.data.swapTransaction, 'base64');
    const tx = VersionedTransaction.deserialize(buf);
    const raw = tx.serialize({ requireAllSignatures: false });
    return { tx, quote, base64: res.data.swapTransaction };
  }

  async executeJupiterSwapLIVE(inputMint, outputMint, amountLamports, slippageBps) {
    if (!this.keypair) throw new Error('Sem keypair para swap LIVE.');
    const { tx } = await this.buildJupiterSwapTx(inputMint, outputMint, amountLamports, slippageBps);
    tx.sign([this.keypair]);
    const sig = await this.connection.sendRawTransaction(tx.serialize(), {
      maxRetries: 3, skipPreflight: false, preflightCommitment: 'confirmed'
    });
    const conf = await this.connection.confirmTransaction(sig, 'confirmed');
    if (conf.value.err) throw new Error(`Tx falhou: ${JSON.stringify(conf.value.err)}`);
    return sig;
  }

  async buyToken(mint, solAmount, entryPrice, expectedTokens) {
    this.log(`>>> [${this.executionMode}] COMPRANDO ${solAmount} SOL de ${mint.slice(0,16)}... price=${entryPrice}`, 'buy');
    if (!this.sendTransactions) {
      const feeSOL = (solAmount * this.config.paperFeeBps) / 10000;
      const slipSOL = (solAmount * this.config.paperSlippageBps) / 10000;
      await sleep(this.config.paperLatencyMs);
      this.log(`[paper] fill virtual: custo ${solAmount.toFixed(6)} + fee ${feeSOL.toFixed(6)} + slip ${slipSOL.toFixed(6)}`, 'sim');
      this.logDecision({
        mint, side: 'buy', signal: 'paper-fill', price: entryPrice,
        expectedOut: expectedTokens, simulatedOut: expectedTokens - feeSOL - slipSOL,
        feeBps: this.config.paperFeeBps, slippageBps: this.config.paperSlippageBps,
        latencyMs: this.config.paperLatencyMs,
        sentToChain: false, executionMode: 'paper_mainnet'
      });
      return `SIM_${Date.now()}`;
    }
    if (!this.enableLiveAssertion()) { this.log('Negando transação real: travas live insuficientes.', 'error'); return null; }
    try {
      const lamports = Math.floor(solAmount * LAMPORTS_PER_SOL);
      const sig = await this.executeJupiterSwapLIVE(WRAPPED_SOL, mint, lamports, this.config.slippageBps);
      this.log(`COMPRA real: ${sig.slice(0,30)}...`, 'buy');
      this.logDecision({ mint, side: 'buy', signal: 'live-exec', expectedOut: solAmount, txSignature: sig, sentToChain: true, executionMode: 'live_mainnet' });
      return sig;
    } catch (e) {
      this.log(`Erro na compra real: ${e.message}`, 'error');
      return null;
    }
  }

  async sellToken(mint, rawAmount, entryPrice, exitPrice) {
    this.log(`<<< [${this.executionMode}] VENDENDO ${mint.slice(0,16)}...`, 'sell');
    if (!this.sendTransactions) {
      await sleep(this.config.paperLatencyMs);
      this.log('[paper] fill virtual de venda.', 'sim');
      this.logDecision({ mint, side: 'sell', signal: 'paper-fill', sentToChain: false, executionMode: 'paper_mainnet' });
      return `SIM_SELL_${Date.now()}`;
    }
    if (!this.enableLiveAssertion()) return null;
    try {
      const sig = await this.executeJupiterSwapLIVE(mint, WRAPPED_SOL, rawAmount, this.config.slippageBps);
      this.log(`VENDA real: ${sig.slice(0,30)}...`, 'sell');
      this.logDecision({ mint, side: 'sell', signal: 'real-exec', txSignature: sig, sentToChain: true, executionMode: 'live_mainnet' });
      return sig;
    } catch (e) { this.log(`Erro na venda real: ${e.message}`, 'error'); return null; }
  }

  enableLiveAssertion() {
    return this.sendTransactions && this.config.allowRealMode && this.config.enableLiveTrading && String(this.config.iUnderstandLiveRisk).toUpperCase() === 'YES';
  }

  async start(options = {}) {
    if (this._starting) { this.log('Inicialização já em andamento...', 'warn'); return; }
    if (this.running) { this.log('Bot já rodando.', 'warn'); return; }
    if (!this.wallet.publicKey) { this.log('Conecte a carteira primeiro.', 'error'); return; }

    this._starting = true;
    try {
    const mode = options.mode || 'simulator';
    const sim = mode === 'simulator';

    if (sim) {
      this.sendTransactions = false;
      this.executionMode = 'paper_mainnet';
      this.config.mode = 'paper_mainnet';
    } else {
      this.sendTransactions = true;
      this.executionMode = 'live_mainnet';
      this.config.mode = 'live_mainnet';
    }

    if (this.sendTransactions) {
      try { this.assertSafeMode(); }
      catch (e) {
        this.log(`❌ Bloqueado: ${e.message}`, 'error');
        this.config.mode = 'paper_mainnet';
        this.executionMode = 'paper_mainnet';
        this.sendTransactions = false;
        this.emitStatus();
        return;
      }
    }

    try {
      await this.validateRpc({ force: true });
      await this.validatePumpProgram();
      this.log(`Validações OK — PROVADOR: ${this.config.pumpFunProgram}`, 'success');
    } catch (e) {
      this.log(`❌ Bloqueado na validação: ${e.message}`, 'error');
      return;
    }

    try {
      if (this.ws) { try { this.ws.close(); } catch (e) {} this.ws = null; }
      await this.connect();
    } catch (e) {
      this.log(`Falha ao conectar: ${e.message}`, 'error');
      return;
    }

    this.running = true;
    this.haltNewEntries = false;
    this.setState('searching');
    this._rpcThrottleMs = this.config.rpcThrottleMs || 300;

    if (this.executionMode === 'paper_mainnet') {
      if (this.paperCash <= 0) this.paperCash = this.config.paperInitialSol;
      if (this.equity <= 0) this.equity = this.paperCash;
      this.profitLoss = 0;
      this.haltNewEntries = false;
      this.recoveryLoss = 0;
      this.recoveryBets = 0;
      this.dayStartEquity = this.paperCash;
      this.startOfDayEquity = this.dayStartEquity;
      this.log('⚠️  MODO SIMULADOR (paper_mainnet): lê dados REAIS da MAINNET, mas NENHUMA transação será enviada.', 'warn');
      this.log(`Caixa virtual: ${this.paperCash} SOL | fee=${this.config.paperFeeBps}bps slip=${this.config.paperSlippageBps}bps lat=${this.config.paperLatencyMs}ms`, 'info');
      this.log(`📊 Seletividade: minEntryScore=${this.config.minEntryScore} | maxPositions=${this.config.maxOpenPositions} | buyAmount=${this.config.buyAmountSol}SOL`, 'info');
      this.log(`🎯 Meta do dia: +${this.config.dailyTargetPct}% | Loss max: -${this.config.dailyLossPct}% | startEquity=${this.dayStartEquity}`, 'info');
    } else {
      // REAL MODE: Initialize equity with actual wallet balance
      await this.refreshWallet();
      this.equity = this.wallet.balanceSOL || 0;
      this.startOfDayEquity = this.equity;
      this.dayStartEquity = this.equity;
      this.log('🚨 MODO REAL (live_mainnet): transações REAIS serão enviadas à mainnet.', 'warn');
      this.log(`Carteira de execução: ${this.wallet.publicKey} | Saldo: ${this.equity.toFixed(4)} SOL`, 'info');
      this.log(`🎯 Meta do dia: +${this.config.dailyTargetPct}% | Loss max: -${this.config.dailyLossPct}%`, 'info');
    }

    this.startTokenMonitor();
    this.log(`Sniper iniciado | Execução: ${this.executionMode} | EnviaTx: ${this.sendTransactions}`, 'info');
    this.emitStatus();

    // Periodic wallet refresh in real mode to keep balance updated
    if (this.executionMode === 'live_mainnet') {
      this.walletRefreshHandle = setInterval(async () => {
        if (this.running) await this.refreshWallet();
      }, 30000); // Refresh every 30 seconds
    }

    // Auto-simulation para paper_mainnet (mostra equity curve evoluindo)
    if (this.executionMode === 'paper_mainnet' && this.config.autoSimulation) {
      this.startAutoSimulation();
    }
    } finally {
      this._starting = false;
    }
  }

  startAutoSimulation() {
    this.log(`🤖 Auto-simulation ativada (intervalo: ${this.config.autoSimulationIntervalMs}ms, winRate: ${this.config.autoSimulationWinRate * 100}%)`, 'info');
    this.log(`📊 Seletividade: apenas tokens com score ≥ ${this.config.minEntryScore} entram`, 'info');
    
    const runSimulation = async () => {
      if (!this.running || this.haltNewEntries || this.executionMode !== 'paper_mainnet' || !this.config.autoSimulation) return;

      // Trava central: máx posições, cooldown e saldo reservado
      if (!this.canEnterTrade()) return;

      try {
        // Simula detecção de token com análise seletiva
        this.metrics.tokensDetected++;
        const mint = 'SIM' + Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
        
        // --- ANÁLISE SELETIVA: simula fatores on-chain seguindo mesma lógica da entrada real ---
        // Distribuição realista: maioria dos lançamentos é fraca, poucos têm fluxo saudável
        const isWeak = Math.random() < 0.55;
        const scoreFactors = {
          liquidity: isWeak ? (0.5 + Math.random() * 4) : (5 + Math.random() * 95),
          firstBuys: isWeak ? Math.floor(Math.random() * 3) : (3 + Math.floor(Math.random() * 15)),
          buyPressure: isWeak ? (0.2 + Math.random() * 0.8) : (0.8 + Math.random() * 2.5),
          uniqueBuyers: isWeak ? Math.floor(Math.random() * 2) : (2 + Math.floor(Math.random() * 8)),
          marketCap: 5 + Math.random() * 200,
          volatility: 0.3 + Math.random() * 0.7,
          isNewLaunch: Math.random() < 0.3,
          topBuyerShare: isWeak ? (0.7 + Math.random() * 0.3) : (0.2 + Math.random() * 0.5)
        };

        const score = this.calculateSimScore(scoreFactors);
        const totalSupply = 1000000000;
        const initialLiq = scoreFactors.liquidity;

        // Gates de fluxo e impacto simulados (mesma lógica da entrada real)
        const simFlow = {
          uniqueBuyers: scoreFactors.uniqueBuyers,
          buySellRatio: scoreFactors.buyPressure,
          topBuyerShare: scoreFactors.topBuyerShare
        };
        const flow = this.evaluateFlow(simFlow);
        const impact = this.evaluateImpact(this.config.buyAmountSol, scoreFactors.liquidity);
        const cls = this.classifyOpportunity(score, flow.grade, impact.ok);

        this.log(`[${cls}] ${mint.slice(0, 12)} score=${score}/100 | ${flow.reason} | ${impact.reason} | liq=${scoreFactors.liquidity.toFixed(2)}SOL buyers=${scoreFactors.uniqueBuyers} b/s=${scoreFactors.buyPressure.toFixed(2)}`, cls.includes('REJEITAR') ? 'warn' : cls.includes('CONVICÇÃO') ? 'success' : 'info');

        // Filtra: rejeita scores baixos, fluxo ruim ou impacto alto
        if (score < this.config.minEntryScore || !flow.ok || !impact.ok) {
          this.metrics.tokensRejected++;
          if (score < this.config.minEntryScore) this.log(`[REJECT] ${mint.slice(0, 12)} score=${score} < min=${this.config.minEntryScore}`, 'warn');
          return;
        }

        // --- QUOTE & ENTRY (auto-sim: quote SIMULADA local — nunca chamar Jupiter com mint fake) ---
        const lamports = Math.floor(this.config.buyAmountSol * LAMPORTS_PER_SOL);
        const simOut = Math.round(lamports * (0.92 + Math.random() * 0.08));
        const quote = {
          outAmount: String(simOut),
          inAmount: String(lamports),
          priceImpactPct: '0.5',
          slippageBps: this.config.slippageBps,
          routePlan: []
        };
        if (!quote?.outAmount) return;
        
        const entryPrice = (this.config.buyAmountSol * LAMPORTS_PER_SOL) / parseInt(quote.outAmount);
        const expectedTokens = parseInt(quote.outAmount) / LAMPORTS_PER_SOL;
        
        // Re-verifica saldo e limite antes de comprar (quote já foi feita)
        if (!this.canEnterTrade()) {
          this.metrics.tokensRejected++;
          return;
        }

        const sig = await this.buyToken(mint, this.config.buyAmountSol, entryPrice, expectedTokens);
        if (!sig) return;
        this.lastEntryAt = Date.now();
        
        const positionId = `POS-AUTO-${String(++this.positionCounter).padStart(6, '0')}`;
        const position = {
          positionId,
          mint,
          status: 'OPEN',
          entryPrice,
          entrySol: this.config.buyAmountSol,
          tokenAmount: expectedTokens,
          openedAt: new Date().toISOString(),
          targetProfitPct: this.config.targetProfitPct,
          stopLossPct: this.config.stopLossPct,
          maxTimeSeconds: this.config.maxPositionTimeSeconds,
          buySignature: sig,
          buyTime: Date.now(),
          firstBuyAt: Date.now(),
          firstLiquidityAt: Date.now(),
          entryAt: Date.now(),
          pnlPct: 0,
          entryScore: score,
          scoreFactors
        };
        this.positions.set(mint, position);
        this.monitoredTokens.set(mint, position);
        // Rejeições/quotes não descontam saldo. Só entrada real desconta.
        this.paperCash = Math.max(0, this.paperCash - this.config.buyAmountSol);
        this.equity = this.paperCash;
        this.metrics.tokensBought++;
        
        this.logDecision({
          mint, positionId, side: 'BUY', signal: 'auto-sim',
          entryPrice, entrySol: this.config.buyAmountSol,
          expectedTokens, slippage: this.config.slippageBps, fee: 0,
          entryScore: score,
          timestamp: new Date().toISOString(), slot: 0
        });
        
        this.log(`✅ ENTRY: ${positionId} ${mint.slice(0,12)} score=${score} entry=${this.config.buyAmountSol} SOL price=${entryPrice.toFixed(8)} saldo=${this.paperCash.toFixed(4)}SOL`, 'buy');
        this.emitStatus();
        
        // --- EXIT: simula preço com base no score (score alto = mais chance de win) ---
        const holdTime = 10000 + Math.random() * 50000;
        const winProb = Math.min(0.85, 0.35 + (score / 100) * 0.5);
        const isWin = Math.random() < winProb;
        const priceMultiplier = isWin 
          ? (1 + (this.config.targetProfitPct + Math.random() * 15) / 100) 
          : (1 - (this.config.stopLossPct + Math.random() * 8) / 100);
        
        setTimeout(async () => {
          // Posição aberta SEMPRE termina (mesmo com bot parado) para liberar o saldo
          if (!this.positions.has(mint)) return;
          
          const pos = this.positions.get(mint);
          if (!pos || pos.status !== 'OPEN') return;
          
          const exitPrice = pos.entryPrice * priceMultiplier;
          const grossPnlPct = (priceMultiplier - 1) * 100;
          const feePct = this.config.paperFeeBps / 100;
          const slippagePct = this.config.paperSlippageBps / 100;
          const netPnlPct = grossPnlPct - feePct - slippagePct;
          const pnlSOL = pos.entrySol * (netPnlPct / 100);
          const holdTimeMs = Date.now() - pos.entryAt;
          
          pos.status = 'CLOSED';
          pos.exitPrice = exitPrice;
          pos.exitReason = isWin ? 'TARGET_PROFIT' : 'STOP_LOSS';
          pos.closedAt = new Date().toISOString();
          pos.grossPnlPct = grossPnlPct;
          pos.netPnlPct = netPnlPct;
          pos.pnlSOL = pnlSOL;
          
          this.paperCash += pos.entrySol + pnlSOL;
          if (this.paperCash < 0) this.paperCash = 0;
          this.equity = this.paperCash;
          
          this.recordTrade(mint, netPnlPct, pnlSOL, isWin ? 'target-profit' : 'stop-loss', {
            entrySol: pos.entrySol,
            exitReason: isWin ? 'TARGET_PROFIT' : 'STOP_LOSS',
            entryScore: pos.entryScore,
            holdTimeMs
          });
          
          this.logDecision({
            mint, positionId: pos.positionId, side: 'SELL', signal: isWin ? 'TARGET_PROFIT' : 'STOP_LOSS',
            entryPrice: pos.entryPrice, exitPrice,
            grossPnl: grossPnlPct, fees: this.config.paperFeeBps / 100,
            slippage: this.config.paperSlippageBps / 100,
            netPnl: netPnlPct, profitPct: netPnlPct, entryScore: pos.entryScore
          });
          
          this.log(`${isWin ? '🎯' : '❌'} EXIT: ${pos.positionId} ${mint.slice(0,12)} ${isWin ? '+' : ''}${netPnlPct.toFixed(2)}% | ${pnlSOL >= 0 ? '+' : ''}${pnlSOL.toFixed(6)} SOL | saldo=${this.paperCash.toFixed(4)}SOL (${isWin ? 'WIN' : 'LOSS'})`, isWin ? 'success' : 'sell');
          this.emitStatus();
          this.checkDailyLimits();
          
          setTimeout(() => {
            this.positions.delete(mint);
            this.monitoredTokens.delete(mint);
          }, 1000);
        }, holdTime);
        
      } catch (e) {
        this.log(`Erro na auto-simulation: ${e.message}`, 'error');
      }
    };
    
    runSimulation();
    this.autoSimulationHandle = setInterval(runSimulation, this.config.autoSimulationIntervalMs);
  }

  calculateSimScore(f) {
    const w = this.config.entryScoreWeights;
    let score = 0;
    score += w.newToken * (f.isNewLaunch ? 1 : 0.5);
    score += w.tradable * (f.liquidity > this.config.minLiquiditySol ? 1 : 0.3);
    score += w.liquidity * Math.min(1, f.liquidity / 50);
    score += Math.min(w.firstBuys, f.firstBuys * 5);
    score += Math.min(w.buyPressure, Math.max(0, (f.buyPressure - 1) * 3));
    score += Math.min(w.uniqueBuyers, f.uniqueBuyers * 8);
    score += w.lowRisk * (f.volatility < 0.6 ? 1 : 0.3);
    return Math.min(100, Math.round(score));
  }

  startTokenMonitor() {
    const pidStr = this.config.pumpFunProgram;
    this.log(`🎯 Monitorando Pump.fun: ${pidStr} | RPC: ${this.rpcUrl()}`, 'sniper');

    const pid = new PublicKey(pidStr);
    this.wsConnected = false;

    const onHelius = this.config.mainnetRpcUrl && this._activeRpcUrl === this.config.mainnetRpcUrl;
    const useWs = !this.config.useDevnet && this.config.mainnetWsUrl && onHelius;
    if (useWs) {
      this.startWebSocketMonitor(pid);
      this.startPollingMonitor(pid, { fallbackOnly: true });
    } else {
      this.log('Monitor via polling HTTP (RPC sem WebSocket dedicado)', 'info');
      this.startPollingMonitor(pid);
    }
  }

  startPollingMonitor(pid, options = {}) {
    const fallbackOnly = options.fallbackOnly === true;
    const intervalMs = this.config.pollingIntervalMs || 30000;
    let lastLog = 0;
    const seen = new Set();
    const sleep2 = (ms) => new Promise(r => setTimeout(r, ms));

    const poll = async () => {
      if (!this.running) return;
      if (this._rpcPausedUntil && Date.now() < this._rpcPausedUntil) return;
      if (fallbackOnly && this.wsConnected && this.ws?.readyState === WebSocket.OPEN) return;

      try {
        const sigs = await this._throttledRpc(() =>
          this.connection.getSignaturesForAddress(pid, { limit: 5 })
        ).then((list) => list.map(s => s.signature));
        if (sigs[0]) {
          if (!fallbackOnly) {
            this.wsConnected = true;
            this.emitStatus();
          }
        }
        let foundAny = false;
        for (const sig of sigs) {
          if (seen.has(sig)) continue;
          seen.add(sig);
          if (seen.size > 500) seen.clear();
          try {
            const mint = await this.isNewPumpToken(sig);
            if (mint && !this.processedMints.has(mint) && !this.monitoredTokens.has(mint)) {
              foundAny = true;
              await this.onNewToken(mint);
              await sleep2(800);
            }
          } catch (e) {
            if (String(e?.message || e)) {
              this.log(`[POLL] erro ao analisar sig: ${String(e.message || e).slice(0, 80)}`, 'warn');
            }
          }
          await sleep2(400);
        }
        if (!foundAny && Date.now() - lastLog > 60000) {
          this.log(`🔍 Monitor ativo — ${this.metrics.tokensDetected} detectados, ${this.metrics.tokensRejected} rejeitados`, 'info');
          lastLog = Date.now();
        }
      } catch (e) {
        const msg = String(e?.message || e);
        if (msg.includes('429')) {
          this._rpcPausedUntil = Date.now() + 90000;
          this.log('⏳ Rate limit no polling — pausa 90s', 'warn');
          return;
        }
        this.log(`Erro no polling: ${msg}`, 'error');
      }
    };

    this.pollingHandle = setInterval(poll, intervalMs);
    poll();
  }

  startWebSocketMonitor(pid) {
    let reconnectDelay = 5000;
    const maxReconnectDelay = 60000;
    let pingInterval = null;

    const connectWs = async () => {
      if (!this.running) return;
      try {
        const wsUrl = this.wsUrl();
        this.log(`Conectando WebSocket...`, 'info');
        this.ws = new WebSocket(wsUrl, wsTlsOptions());
        
        const connectionTimeout = setTimeout(() => {
          if (this.ws && this.ws.readyState === WebSocket.CONNECTING) {
            this.ws.close();
          }
        }, 15000);

        this.ws.on('open', async () => {
          clearTimeout(connectionTimeout);
          this.wsConnected = true;
          reconnectDelay = 5000;
          this.log('WebSocket conectado', 'success');
          this.emitStatus();
          
          const subMsg = {
            jsonrpc: '2.0', id: 1, method: 'logsSubscribe',
            params: [{ mentions: [pid.toString()] }, { commitment: 'confirmed' }]
          };
          this.ws.send(JSON.stringify(subMsg));

          pingInterval = setInterval(() => {
            if (this.ws && this.ws.readyState === WebSocket.OPEN) {
              this.ws.ping();
            }
          }, 30000);
        });

        this.ws.on('message', async (data) => {
          if (!this.running) return;
          try {
            const msg = JSON.parse(data.toString());
            
            if (msg.method === 'logsNotification') {
              const params = msg.params;
              let result = null;
              
              if (Array.isArray(params)) {
                result = params[0];
              } else if (params && typeof params === 'object') {
                result = params.result;
              }
              
              if (!result) return;
              
              const slot = result.slot;
              const logs = result.logs;
              const signature = result.signature;
              
              if (slot === undefined || !logs || !logs.length) return;
              
              await this.processLogNotification({ signature, logs, slot });
            } else if (msg.result !== undefined) {
              this.log('Subscription confirmada', 'info');
            } else if (msg.error) {
              this.log(`Erro RPC: ${JSON.stringify(msg.error)}`, 'error');
            }
          } catch (e) {}
        });

        this.ws.on('close', (code) => {
          clearTimeout(connectionTimeout);
          if (pingInterval) { clearInterval(pingInterval); pingInterval = null; }
          this.wsConnected = false;
          this.emitStatus();
          if (this.running) {
            const baseDelay = (this._rpcPausedUntil && Date.now() < this._rpcPausedUntil) ? 120000 : reconnectDelay;
            this.log(`WebSocket fechado (${code}), reconectando em ${baseDelay / 1000}s...`, 'warn');
            setTimeout(() => {
              reconnectDelay = Math.min(reconnectDelay * 1.5, maxReconnectDelay);
              connectWs();
            }, baseDelay);
          }
        });

        this.ws.on('error', (e) => {
          if (String(e.message).includes('429')) {
            this._rpcPausedUntil = Date.now() + 120000;
            this.log('⏳ WebSocket 429 — aguardando 2min antes de reconectar', 'warn');
          }
          this.log(`WebSocket erro: ${e.message}`, 'error');
        });
      } catch (e) {
        if (this.running) {
          setTimeout(() => {
            reconnectDelay = Math.min(reconnectDelay * 1.5, maxReconnectDelay);
            connectWs();
          }, reconnectDelay);
        }
      }
    };

    connectWs();
  }

  async processLogNotification(logResult) {
    const { signature, logs, slot } = logResult;
    this.lastSlot = slot;

    if (!this.running || this.haltNewEntries) return;
    if (!logs || !logs.length) return;

    const createLog = logs.find(l => l.includes('InitializeMint') || l.includes('create') || l.includes('launch') || l.includes('pump') || l.includes('mint'));
    if (!createLog) return;

    const mint = this.extractMintFromLogs(logs);
    if (!mint || !mint.endsWith('pump') || this.processedMints.has(mint)) return;

    this.processedMints.add(mint);
    this.metrics.tokensDetected++;

    this.log(`🎯 NOVO TOKEN: ${mint}`, 'sniper');
    this.logDecision({ mint, side: 'entry', signal: 'onchain-detect', slot, detectedAt: new Date().toISOString() });

    const validationStart = Date.now();
    await this.analyzeAndEnter(mint, signature, slot, 'ws');
    this.recordLatency('validation', Date.now() - validationStart);
  }

  async isNewPumpToken(signature) {
    const ok = (t) => t && t.uiTokenAmount && Number(t.uiTokenAmount.uiAmount) > 0;
    try {
      const tx = await this._throttledRpc(() =>
        this.connection.getTransaction(signature, { commitment: 'confirmed', maxSupportedTransactionVersion: 0 })
      );
      if (!tx) return null;
      const pre = new Set((tx.meta.preTokenBalances || [])
        .filter(t => t.mint !== WRAPPED_SOL)
        .map(t => t.mint));
      const post = (tx.meta.postTokenBalances || []).filter(t => t.mint !== WRAPPED_SOL);
      if (!post.length) return null;
      for (const b of post) {
        if (!pre.has(b.mint) && ok(b)) {
          if (KNOWN_SKIP_MINTS.has(b.mint)) return null;
          return b.mint;
        }
      }
      const active = post.find(ok);
      if (active && KNOWN_SKIP_MINTS.has(active.mint)) return null;
      return active ? active.mint : null;
    } catch (e) {}
    return null;
  }

  // Trava central de entrada: respeita máx posições, cooldown, saldo e meta diária.
  canEnterTrade(silent = false) {
    if (this.haltNewEntries) return false;

    const balance = !this.sendTransactions ? this.paperCash : (this.wallet.balanceSOL || 0);

    if (this.positions.size >= this.config.maxOpenPositions) {
      if (!silent) this.log(`[ENTRY] Máximo de ${this.config.maxOpenPositions} posições abertas. Aguardando fechamento...`, 'warn');
      return false;
    }

    const sinceLast = Date.now() - (this.lastEntryAt || 0);
    if (sinceLast < this.config.minEntryIntervalMs) {
      if (!silent) {
        const waitS = Math.ceil((this.config.minEntryIntervalMs - sinceLast) / 1000);
        this.log(`[ENTRY] Cooldown: aguardando ${waitS}s antes de nova entrada (${this.positions.size}/${this.config.maxOpenPositions} abertas, saldo ${balance.toFixed(4)}SOL).`, 'info');
      }
      return false;
    }

    if (balance < this.config.buyAmountSol + this.config.reserveSol) {
      if (!silent) this.log(`Saldo insuficiente: ${balance.toFixed(4)} SOL (necessário ${this.config.buyAmountSol} + reserva ${this.config.reserveSol})`, 'error');
      return false;
    }

    return true;
  }

  // Tamanho de entrada com recuperação de saldo perdido:
  // acumula perdas e aumenta progressivamente até recuperar ou atingir recoveryMaxBets.
  entrySizeSol(sizeMultiplier = 1) {
    let base = this.config.buyAmountSol;
    if (this.config.recoveryEnabled && this.recoveryLoss > 0 && this.recoveryBets < this.config.recoveryMaxBets) {
      const boost = this.recoveryLoss / Math.max(1, this.recoveryBets + 1) * (this.config.recoverySizePct || 2.0);
      base = Math.min(this.config.maxSolPerTrade, this.config.buyAmountSol + boost);
      base = Math.max(this.config.buyAmountSol, base);
    }
    const sized = base * Math.max(0.3, Math.min(1, sizeMultiplier));
    return Math.min(this.config.maxSolPerTrade, Math.max(this.config.buyAmountSol * 0.4, sized));
  }

  // Pipeline ÚNICO de entrada analítica: todo caminho (WS, polling, simulação) passa por aqui.
  async analyzeAndEnter(mint, signature = null, slot = 0, source = 'ws') {
    // 1) Travas operacionais (máx posições, cooldown, saldo, meta diária)
    if (!this.canEnterTrade()) {
      this._recordRejection('travas operacionais');
      return false;
    }

    // 2) Contrato
    const tokenInfo = await this.getTokenLaunchInfo(mint, signature, slot);
    if (!tokenInfo) {
      this.log(`[REJECT] ${mint.slice(0,16)}: sem info do contrato`, 'warn');
      this._recordRejection('sem info do contrato');
      return false;
    }

    // 3) Liquidez (sempre obrigatória)
    const liquidityInfo = await this.validateLiquidityAndTradability(tokenInfo);
    if (!liquidityInfo.tradable) {
      this.log(`[REJECT] ${mint.slice(0,16)}: não negociável - ${liquidityInfo.reason}`, 'warn');
      this._recordRejection(liquidityInfo.reason);
      return false;
    }

    // 4) Análise de mercado (fase, fluxo real, risco, convicção)
    const analysis = analyzeMarket({
      tokenInfo,
      liquidityInfo,
      firstBuys: liquidityInfo.firstBuys,
      config: this.config,
      entryScoreWeights: this.config.entryScoreWeights
    });
    const entrySol = this.entrySizeSol(analysis.sizeMultiplier);
    const recoveryNote = entrySol > this.config.buyAmountSol ? ` (recuperação +${this.recoveryLoss.toFixed(4)}SOL)` : '';
    const score = analysis.score;
    const flowOk = analysis.flow;
    const liqForImpact = liquidityInfo.realSolReserves > 0
      ? liquidityInfo.realSolReserves
      : (liquidityInfo.virtualSolReserves || 0);
    const impact = this.evaluateImpact(entrySol, liqForImpact);
    const isNewLaunch = this.config.forceEntryOnNewLaunch && liquidityInfo.isNewLaunch === true;
    const canEnter = isNewLaunch || (analysis.recommendation === 'ENTER' && flowOk.ok && impact.ok);
    const cls = isNewLaunch
      ? '🟢 OPORTUNIDADE'
      : classifyFromAnalysis(analysis, this.config);

    this.log(
      `[${cls}] ${mint.slice(0,16)} ${analysis.summary} | ${flowOk.reason} | ${impact.reason} | ` +
      `liq=${(liquidityInfo.realSolReserves||0).toFixed(2)}SOL buyers=${liquidityInfo.firstBuys?.uniqueBuyers||0} ` +
      `b/s=${(liquidityInfo.firstBuys?.buySellRatio||0).toFixed(2)} size=${entrySol.toFixed(4)}SOL ` +
      `signals=[${analysis.signals.join(',')}] risks=[${analysis.risks.join(',')}]${recoveryNote}`,
      cls.includes('REJEITAR') ? 'warn' : cls.includes('CONVICÇÃO') ? 'success' : 'info'
    );

    if (!canEnter || score < this.config.minEntryScore || !impact.ok) {
      const rejectReason = !impact.ok
        ? impact.reason
        : score < this.config.minEntryScore
        ? `score ${score} < ${this.config.minEntryScore}`
        : (analysis.recommendation === 'WAIT' ? flowOk.reason : `convicção ${analysis.conviction}`);
      this._recordRejection(rejectReason);
      if (analysis.recommendation === 'WAIT' || flowOk.grade === 'AGUARDAR') {
        this._scheduleRecheck(mint, analysis.phase === 'LAUNCH' ? 6000 : 10000);
      }
      return false;
    }

    // 5) Quote — paper usa simulação AMM da bonding curve (consistente com monitoramento)
    let entryPrice;
    let expectedTokens;
    if (!this.sendTransactions) {
      const curve = await this.fetchBondingCurveReserves(mint);
      if (!curve) {
        this._recordRejection('bonding curve indisponível');
        return false;
      }
      const sim = this._simulateCurveBuy(entrySol, curve.virtualSol, curve.virtualToken);
      if (sim.tokensOut <= 0) {
        this._recordRejection('simulação de compra inválida');
        return false;
      }
      expectedTokens = sim.tokensOut;
      entryPrice = sim.effectivePrice;
      liquidityInfo.curveAfterBuy = { virtualSol: sim.virtualSol, virtualToken: sim.virtualToken };
      this.log(`[PAPER QUOTE] ${mint.slice(0, 16)} curve buy → ${expectedTokens.toFixed(2)} tokens @ ${entryPrice.toFixed(10)}`, 'info');
    } else {
      const quote = await this.getBuyQuote(mint, entrySol);
      if (!quote?.outAmount) {
        this._recordRejection('sem quote Jupiter');
        return false;
      }
      entryPrice = (entrySol * LAMPORTS_PER_SOL) / parseInt(quote.outAmount);
      expectedTokens = parseInt(quote.outAmount) / LAMPORTS_PER_SOL;
    }

    // 6) Re-checa travas (quote é lenta; pode ter mudado)
    if (!this.canEnterTrade()) {
      this._recordRejection('travas operacionais pós-quote');
      return false;
    }

    // 7) Executa
    const sig = await this.buyToken(mint, entrySol, entryPrice, expectedTokens);
    if (!sig) return false;
    this.lastEntryAt = Date.now();

    const positionId = `POS-${source.toUpperCase()}-${String(++this.positionCounter).padStart(6, '0')}`;
    const position = {
      positionId,
      mint,
      status: 'OPEN',
      entryPrice,
      entrySol,
      tokenAmount: expectedTokens,
      openedAt: new Date().toISOString(),
      targetProfitPct: analysis.exitProfile.targetProfitPct,
      stopLossPct: analysis.exitProfile.stopLossPct,
      maxTimeSeconds: analysis.exitProfile.maxTimeSeconds,
      partialTakeProfitPct: analysis.exitProfile.partialTakeProfitPct,
      trailingActivatePct: analysis.exitProfile.trailingActivatePct,
      breakevenActivatePct: analysis.exitProfile.breakevenActivatePct,
      marketPhase: analysis.phase,
      conviction: analysis.conviction,
      buySignature: sig,
      buyTime: Date.now(),
      firstBuyAt: null,
      firstLiquidityAt: Date.now(),
      entryAt: Date.now(),
      pnlPct: 0,
      entryScore: score,
      classification: cls,
      partialTaken: false,
      flowMetrics: {
        buyers: liquidityInfo.firstBuys?.uniqueBuyers || 0,
        buySellRatio: liquidityInfo.firstBuys?.buySellRatio || 0,
        impactPct: impact.impactPct || 0
      },
      curveAfterBuy: liquidityInfo.curveAfterBuy || null,
      entrySpotPrice: liquidityInfo.curveAfterBuy
        ? liquidityInfo.curveAfterBuy.virtualSol / liquidityInfo.curveAfterBuy.virtualToken
        : entryPrice,
      lastMarkPct: 0
    };
    this.positions.set(mint, position);
    this.monitoredTokens.set(mint, position);
    if (!this.sendTransactions) {
      this.paperCash = Math.max(0, this.paperCash - entrySol);
      this.equity = this.paperCash;
    }
    this.metrics.tokensBought++;

    this.logDecision({
      mint, positionId, side: 'BUY', signal: source,
      entryPrice, entrySol: this.config.buyAmountSol,
      expectedTokens, estimatedPriceImpact: impact.impactPct || 0,
      slippage: this.config.slippageBps, fee: 0,
      entryScore: score, classification: cls,
      timestamp: new Date().toISOString(), slot
    });

    this.log(`✅ ENTRY: ${positionId} ${mint.slice(0,16)} score=${score} ${cls} entry=${entrySol.toFixed(4)} SOL price=${entryPrice.toFixed(8)} saldo=${this.paperCash.toFixed(4)}SOL${recoveryNote}`, 'buy');
    this.emitStatus();
    this.monitorPosition(mint);
    return true;
  }

  async onNewToken(mint) {
    if (!mint || this.processedMints.has(mint)) return false;
    if (!mint.endsWith('pump')) return false;

    this.processedMints.add(mint);
    this.metrics.tokensDetected++;
    this.log(`🎯 NOVO TOKEN: ${mint}`, 'sniper');

    try {
      return await Promise.race([
        this.analyzeAndEnter(mint, null, 0, 'poll'),
        new Promise((_, reject) => setTimeout(() => reject(new Error('timeout análise 20s')), 20000))
      ]);
    } catch (e) {
      this._recordRejection(String(e.message || e).slice(0, 48));
      this.log(`[ERRO] ${mint.slice(0, 16)}: ${e.message}`, 'warn');
      return false;
    }
  }

  extractMintFromLogs(logs) {
    for (const log of logs) {
      const match = log.match(/Program log: (?:InitializeMint|create).*?([1-9A-HJ-NP-Za-km-z]{32,44})/);
      if (match) return match[1];
      const match2 = log.match(/Program log:.*?([1-9A-HJ-NP-Za-km-z]{44})/);
      if (match2) return match2[1];
    }
    return null;
  }

  async getTokenLaunchInfo(mint, signature, slot) {
    try {
      const mintInfo = await this._throttledRpc(() =>
        this.connection.getParsedAccountInfo(new PublicKey(mint))
      );
      if (!mintInfo.value?.data?.parsed) return null;

      const creator = mintInfo.value.data.parsed.info.mintAuthority || 'unknown';
      const bondingCurve = await this.findBondingCurve(mint);

      return {
        mint,
        creator,
        bondingCurve,
        signature,
        slot,
        detectedAt: new Date().toISOString(),
        mintAuthority: mintInfo.value.data.parsed.info.mintAuthority,
        freezeAuthority: mintInfo.value.data.parsed.info.freezeAuthority,
        supply: mintInfo.value.data.parsed.info.supply,
        decimals: mintInfo.value.data.parsed.info.decimals
      };
    } catch (e) {
      this.log(`Erro ao obter info do token ${mint}: ${e.message}`, 'error');
      return null;
    }
  }

  async findBondingCurve(mint) {
    try {
      const bondingCurvePDA = PublicKey.findProgramAddressSync(
        [Buffer.from('bonding-curve'), new PublicKey(mint).toBuffer()],
        new PublicKey(this.config.pumpFunProgram)
      )[0];
      return bondingCurvePDA.toString();
    } catch (e) {
      return null;
    }
  }

  async validateLiquidityAndTradability(tokenInfo) {
    try {
      const bondingCurveInfo = await this._throttledRpc(() =>
        this.connection.getAccountInfo(new PublicKey(tokenInfo.bondingCurve))
      );
      if (!bondingCurveInfo || bondingCurveInfo.data.length < 40) {
        return { tradable: false, reason: 'bonding curve não encontrada ou inválida' };
      }

      const data = bondingCurveInfo.data;
      // Layout da BondingCurve (Anchor): [0..8) discriminator | [8) virtualTokenReserves | [16) virtualSolReserves | [24) realTokenReserves | [32) realSolReserves | [40) tokenTotalSupply | [48) complete
      const virtualTokenReserves = data.readBigUInt64LE(8) / BigInt(10 ** 6);
      const virtualSolReserves = data.readBigUInt64LE(16) / BigInt(LAMPORTS_PER_SOL);
      const realTokenReserves = data.readBigUInt64LE(24) / BigInt(10 ** 6);
      const realSolReserves = data.readBigUInt64LE(32) / BigInt(LAMPORTS_PER_SOL);

      const virtualSol = Number(virtualSolReserves);
      const realSol = Number(realSolReserves);
      const currentPrice = virtualSol / Number(virtualTokenReserves);

      const isNewLaunch = this.config.forceEntryOnNewLaunch &&
        tokenInfo.detectedAt &&
        (Date.now() - new Date(tokenInfo.detectedAt).getTime()) < this.config.maxNewLaunchAgeSeconds * 1000;

      // Virtual pool é o indicador principal em lançamentos Pump.fun
      if (virtualSol < this.config.minVirtualSolReserves) {
        return { tradable: false, reason: `virtualSolReserves ${virtualSol.toFixed(2)} < ${this.config.minVirtualSolReserves}` };
      }

      // realSol baixo é normal no snipe inicial — só bloqueia se virtual também estiver fraco
      const isFreshLaunch = realSol < this.config.minLiquiditySol;
      if (!isFreshLaunch && realSol < this.config.minLiquiditySol) {
        return { tradable: false, reason: `realSolReserves ${realSol.toFixed(2)} < ${this.config.minLiquiditySol}` };
      }

      let firstBuys = await this.detectFirstBuys(tokenInfo.mint);
      if (!firstBuys || (firstBuys.rpcDegraded && firstBuys.uniqueBuyers === 0)) {
        firstBuys = this._estimateFlowFromCurve(realSol, virtualSol);
      }

      return {
        tradable: true,
        virtualSolReserves: virtualSol,
        virtualTokenReserves: Number(virtualTokenReserves),
        realSolReserves: realSol,
        realTokenReserves: Number(realTokenReserves),
        currentPrice,
        firstBuys,
        bondingCurve: tokenInfo.bondingCurve,
        isNewLaunch,
        isFreshLaunch
      };
    } catch (e) {
      return { tradable: false, reason: `erro ao validar: ${e.message}` };
    }
  }

  async detectFirstBuys(mint) {
    try {
      const sigs = await this._throttledRpc(() =>
        this.connection.getSignaturesForAddress(new PublicKey(mint), { limit: Math.max(6, this.config.firstBuyCount + 4) })
      );
      const now = Date.now() / 1000;
      const windowStart = now - this.config.firstBuyWindowSeconds;
      const buys = [];
      const sells = [];
      const buyerVol = new Map();
      const sellerVol = new Map();
      let buyVolume = 0;
      let sellVolume = 0;

      let rpcErrors = 0;
      const maxTxFetches = 3;
      let txFetches = 0;
      for (const s of sigs) {
        if (txFetches >= maxTxFetches) break;
        if (!s.blockTime || s.blockTime < windowStart) continue;
        try {
          txFetches++;
          const tx = await this._throttledRpc(() =>
            this.connection.getTransaction(s.signature, { commitment: 'confirmed', maxSupportedTransactionVersion: 0 })
          );
          if (!tx) continue;
          const pre = tx.meta.preTokenBalances || [];
          const post = tx.meta.postTokenBalances || [];
          for (let i = 0; i < post.length; i++) {
            const p = post[i];
            if (p.mint === mint) {
              const preBal = pre.find(x => x.accountIndex === p.accountIndex);
              const diff = (p.uiTokenAmount.uiAmount || 0) - (preBal?.uiTokenAmount?.uiAmount || 0);
              if (diff > 0) {
                buys.push({ signature: s.signature, buyer: p.owner, amount: diff, timestamp: s.blockTime });
                buyerVol.set(p.owner, (buyerVol.get(p.owner) || 0) + diff);
                buyVolume += diff;
              } else if (diff < 0) {
                sells.push({ signature: s.signature, seller: p.owner, amount: Math.abs(diff), timestamp: s.blockTime });
                sellerVol.set(p.owner, (sellerVol.get(p.owner) || 0) + Math.abs(diff));
                sellVolume += Math.abs(diff);
              }
            }
          }
          if (buys.length >= this.config.firstBuyCount + 8) break;
        } catch (e) {
          if (String(e?.message || '').includes('429')) {
            rpcErrors++;
            if (rpcErrors >= 2) break;
            this.log('⏳ Rate limit no detectFirstBuys — usando estimativa da curva', 'warn');
            await new Promise(r => setTimeout(r, 500));
          }
        }
      }

      if (rpcErrors > 0 && buys.length === 0) {
        return this._estimateFlowFromCurve(0, 30);
      }

      const uniqueBuyers = new Set(buys.map(b => b.buyer));
      const uniqueSellers = new Set(sells.map(s => s.seller));
      const buySellRatio = sellVolume > 0 ? buyVolume / sellVolume : (buyVolume > 0 ? 3 : 0);

      // Concentração: fração do volume de compra dominada pela maior carteira
      let topBuyerShare = 0;
      if (buyVolume > 0) {
        const top = Math.max(...buyerVol.values());
        topBuyerShare = top / buyVolume;
      }
      // Série temporal: span entre 1ª e última compra na janela
      let timeSpanSeconds = 0;
      if (buys.length >= 2) {
        const times = buys.map(b => b.timestamp).sort((a, b) => a - b);
        timeSpanSeconds = times[times.length - 1] - times[0];
      }

      return {
        count: buys.length,
        sellCount: sells.length,
        firstBuy: buys[0] || null,
        buys,
        buyVolume,
        sellVolume,
        uniqueBuyers: uniqueBuyers.size,
        uniqueSellers: uniqueSellers.size,
        buySellRatio,
        topBuyerShare,
        timeSpanSeconds,
        avgBuySize: buys.length ? buyVolume / buys.length : 0,
        avgSellSize: sells.length ? sellVolume / sells.length : 0,
        rpcDegraded: rpcErrors > 0 && buys.length === 0
      };
    } catch (e) {
      return { count: 0, sellCount: 0, firstBuy: null, buys: [], buyVolume: 0, sellVolume: 0, uniqueBuyers: 0, uniqueSellers: 0, buySellRatio: 0, topBuyerShare: 1, timeSpanSeconds: 0, avgBuySize: 0, avgSellSize: 0, rpcDegraded: true };
    }
  }

  // Gate de fluxo: confirma que há compra real, diversificada e sem concentração anômala.
  evaluateFlow(firstBuys, liquidityInfo = {}) {
    if (!firstBuys) return { ok: false, reason: 'sem dados de fluxo', grade: 'REJEITAR' };
    const minBuyers = this.config.minUniqueBuyers;
    const minRatio = this.config.minBuySellRatio;
    const maxConc = this.config.maxBuyerConcentration;
    const ratioEps = 0.02;

    if (firstBuys.uniqueBuyers < minBuyers) {
      if ((liquidityInfo.realSolReserves || 0) >= 1) {
        return { ok: true, reason: 'liquidez real confirma atividade', grade: 'OPORTUNIDADE' };
      }
      if (firstBuys.estimated || liquidityInfo.isFreshLaunch) {
        return { ok: false, reason: `compradores ${firstBuys.uniqueBuyers} (aguardando fluxo)`, grade: 'AGUARDAR' };
      }
      return { ok: false, reason: `compradores distintos ${firstBuys.uniqueBuyers} < ${minBuyers}`, grade: 'OBSERVAR' };
    }

    // Sem vendas + compradores = momentum puro (típico de snipe Pump.fun)
    const pureBuyMomentum = firstBuys.sellCount === 0 && firstBuys.uniqueBuyers >= minBuyers && firstBuys.buyVolume > 0;
    if (!pureBuyMomentum && firstBuys.buySellRatio + ratioEps < minRatio) {
      return { ok: false, reason: `buy/sell ${firstBuys.buySellRatio.toFixed(2)} < ${minRatio}`, grade: 'AGUARDAR' };
    }
    if (firstBuys.topBuyerShare > maxConc) {
      return { ok: false, reason: `concentração ${(firstBuys.topBuyerShare * 100).toFixed(0)}% > ${(maxConc * 100).toFixed(0)}%`, grade: 'REJEITAR' };
    }
    return { ok: true, reason: pureBuyMomentum ? 'momentum puro (sem vendas)' : 'fluxo confirmado', grade: 'OPORTUNIDADE' };
  }

  // Impacto de ordem e EV líquido estimado antes da entrada.
  evaluateImpact(buyAmountSol, solReserves) {
    if (!solReserves || solReserves <= 0) return { ok: false, reason: 'liquidez indisponível' };
    const impactPct = (buyAmountSol / solReserves) * 100;
    if (impactPct > this.config.maxImpactPct) {
      return { ok: false, reason: `impacto ${impactPct.toFixed(2)}% > máx ${this.config.maxImpactPct}% (liquidez ${solReserves.toFixed(2)} SOL)`, impactPct };
    }
    // EV líquido conceitual: alvo − fee − slippage − impacto
    const feePct = this.config.paperFeeBps / 100;
    const slipPct = this.config.paperSlippageBps / 100;
    const netTarget = this.config.targetProfitPct - feePct - slipPct - impactPct;
    if (netTarget <= 0) {
      return { ok: false, reason: `EV líquido ≤ 0 (alvo ${this.config.targetProfitPct}% − custos ${(feePct + slipPct + impactPct).toFixed(2)}%)`, impactPct };
    }
    return { ok: true, reason: `impacto ${impactPct.toFixed(2)}% | EV líquido +${netTarget.toFixed(2)}%`, impactPct };
  }

  // Classifica a oportunidade para o log/dashboard.
  classifyOpportunity(score, flowGrade, impactOk) {
    if (!impactOk) return '🔴 REJEITAR';
    if (flowGrade === 'REJEITAR') return '🔴 REJEITAR';
    if (score >= 85 && flowGrade === 'OPORTUNIDADE') return '🔵 ALTA CONVICÇÃO';
    if (score >= this.config.minEntryScore && flowGrade === 'OPORTUNIDADE') return '🟢 OPORTUNIDADE';
    if (flowGrade === 'AGUARDAR') return '🟡 AGUARDAR CONFIRMAÇÃO';
    return '🟠 OBSERVAR';
  }

  calculateEntryScore(tokenInfo, liquidityInfo) {
    const w = this.config.entryScoreWeights;
    const fb = liquidityInfo.firstBuys || {};
    let score = 0;

    score += w.newToken;
    score += w.tradable;
    // Liquidez relativa (até 50 SOL de reserva real pontua cheio)
    score += w.liquidity * Math.min(1, (liquidityInfo.realSolReserves || 0) / 50);
    // Compradores DISTINTOS valem mais que contagem bruta (anti wash-trading)
    score += Math.min(w.firstBuys, fb.uniqueBuyers * 4);
    // Continuidade temporal de compra: 1ª→última compra em ≥ 5s indica fluxo, não burst único
    if ((fb.timeSpanSeconds || 0) >= 5) score += 5;
    // Buy/sell pressure
    score += Math.min(w.buyPressure, Math.max(0, ((fb.buySellRatio || 0) - 1) * 4));
    // Buyers únicos vs contagem: penaliza mesma carteira repetindo
    score += Math.min(w.uniqueBuyers, fb.uniqueBuyers * 6);
    // Penaliza concentração (top buyer dominando o volume de compra)
    if ((fb.topBuyerShare || 0) > this.config.maxBuyerConcentration) score -= 20;
    if (liquidityInfo.isFreshLaunch) score += 8;
    score += w.lowRisk;

    return Math.min(100, Math.max(0, Math.round(score)));
  }

  async getBuyQuote(mint, solAmount) {
    const lamports = Math.floor(solAmount * LAMPORTS_PER_SOL);
    return this.getJupitQuote(WRAPPED_SOL, mint, lamports, this.config.slippageBps);
  }

  async getBondingCurvePrice(mint) {
    try {
      const bondingCurve = await this.findBondingCurve(mint);
      if (!bondingCurve) return null;
      const bondingCurveInfo = await this._throttledRpc(() =>
        this.connection.getAccountInfo(new PublicKey(bondingCurve))
      );
      if (!bondingCurveInfo || bondingCurveInfo.data.length < 24) return null;
      const data = bondingCurveInfo.data;
      const virtualTokenReserves = data.readBigUInt64LE(8) / BigInt(10 ** 6);
      const virtualSolReserves = data.readBigUInt64LE(16) / BigInt(LAMPORTS_PER_SOL);
      if (virtualTokenReserves === 0n) return null;
      return Number(virtualSolReserves) / Number(virtualTokenReserves);
    } catch (e) {
      return null;
    }
  }

  async getSellQuote(mint, tokenAmount) {
    const lamports = Math.floor(tokenAmount * LAMPORTS_PER_SOL);
    return this.getJupitQuote(mint, WRAPPED_SOL, lamports, this.config.slippageBps);
  }

  async monitorPosition(mint) {
    const position = this.positions.get(mint);
    if (!position) return;
    if (this._positionMonitors.has(mint)) return;

    this.log(`Monitorando ${position.positionId} ${mint.slice(0,16)}...`, 'info');

    const interval = setInterval(async () => {
      if (!this.positions.has(mint)) { clearInterval(interval); this._positionMonitors.delete(mint); return; }
      const pos = this.positions.get(mint);
      if (!pos || pos.status !== 'OPEN') {
        clearInterval(interval);
        this._positionMonitors.delete(mint);
        return;
      }

      try {
        let pnlPct = 0;
        let exitPrice = null;
        let exitSolOut = null;

        if (!this.sendTransactions) {
          const mark = await this._paperPositionPnl(mint, pos.tokenAmount, pos.entrySol, pos);
          if (!mark) return;
          pnlPct = mark.grossPnlPct;
          exitSolOut = mark.solOut;
          exitPrice = pos.tokenAmount > 0 ? mark.solOut / pos.tokenAmount : pos.entryPrice;
          pos.lastMarkPct = pnlPct;
        } else {
          const quote = await this.getSellQuote(mint, pos.tokenAmount);
          if (!quote?.outAmount) return;
          exitPrice = (parseInt(quote.outAmount) / LAMPORTS_PER_SOL) / pos.tokenAmount;
          pnlPct = ((exitPrice - pos.entryPrice) / pos.entryPrice) * 100;
        }
        pos.pnlPct = pnlPct;

        // Trailing stop: rastreia pico e protege parte do lucro
        if (pos.peakPnlPct === undefined) pos.peakPnlPct = 0;
        if (pnlPct > pos.peakPnlPct) pos.peakPnlPct = pnlPct;

        const elapsedSeconds = (Date.now() - pos.entryAt) / 1000;

        this.log(`[POSITION] ${pos.positionId} mint=${mint.slice(0,16)} pnl=${pnlPct >= 0 ? '+' : ''}${pnlPct.toFixed(2)}% peak=${pos.peakPnlPct.toFixed(2)}% elapsed=${elapsedSeconds.toFixed(0)}s`, 'info');

        let shouldExit = false;
        let exitReason = '';
        let partialSold = false;

        // 1) Take parcial no PICO (não espera recuar) — protege lucro como F9TW +9%
        const partialTp = pos.partialTakeProfitPct ?? this.config.partialTakeProfitPct;
        if (!pos.partialTaken && (pnlPct >= partialTp || pos.peakPnlPct >= partialTp)) {
          const share = this.config.partialTakeProfitSharePct / 100;
          const tokensToSell = pos.tokenAmount * share;
          const proceeds = (exitPrice * tokensToSell) * (1 - (this.config.paperFeeBps + this.config.paperSlippageBps) / 10000);
          this.paperCash += proceeds;
          pos.tokenAmount = Math.max(pos.tokenAmount - tokensToSell, 0);
          pos.entrySol = pos.entrySol * (1 - share);
          if (pos.tokenAmount > 0) {
            pos.partialTaken = true;
            partialSold = true;
            this.log(`[PARTIAL] ${pos.positionId} +${pnlPct.toFixed(2)}% → embolsou ${(share*100).toFixed(0)}% (+${(proceeds).toFixed(4)} SOL) e mantém ${pos.tokenAmount.toFixed(2)} para trailing`, 'success');
            this.emitStatus();
          }
        }

        // 2) Lucro cheio no alvo
        if (!partialSold && pnlPct >= pos.targetProfitPct) {
          shouldExit = true;
          exitReason = 'TARGET_PROFIT';
        } else if (!this.config.holdUntilProfit && pnlPct <= -Math.min(pos.stopLossPct, this.config.stopLossPct)) {
          const grace = this.config.stopLossGraceSeconds || 0;
          const catastrophic = pnlPct <= -50;
          if (elapsedSeconds >= grace || catastrophic) {
            shouldExit = true;
            exitReason = 'STOP_LOSS';
          } else if (!pos._graceLogged) {
            pos._graceLogged = true;
            this.log(`[GRACE] ${pos.positionId} pnl=${pnlPct.toFixed(2)}% — aguardando ${grace}s antes do stop (evita falso negativo de quote)`, 'info');
          }
        } else if (!this.config.holdUntilProfit && elapsedSeconds >= pos.maxTimeSeconds) {
          const feePct = this.config.paperFeeBps / 100;
          const slippagePct = this.config.paperSlippageBps / 100;
          const netPnlPct = pnlPct - feePct - slippagePct;
          if (!this.config.timeoutOnlyOnProfit || netPnlPct >= 0) {
            shouldExit = true;
            exitReason = 'TIMEOUT';
          } else if (!pos._timeoutHoldLogged) {
            pos._timeoutHoldLogged = true;
            this.log(`[TIMEOUT] tempo esgotado (${pos.maxTimeSeconds}s) mas PnL líquido negativo (${netPnlPct.toFixed(2)}%) — aguardando lucro`, 'info');
          }
        } else if (pos.peakPnlPct >= (pos.breakevenActivatePct ?? this.config.breakevenActivatePct) && pnlPct <= this.config.breakevenFloorPct) {
          // Já esteve lucrativo: se voltar ao zero, sai sem prejuízo (protege capital)
          shouldExit = true;
          exitReason = 'BREAKEVEN';
        } else if (pos.peakPnlPct >= (pos.trailingActivatePct ?? this.config.trailingActivatePct)) {
          // Trailing: protege lucro mesmo se virou negativo (ex: +9% → dump)
          const retainedFloor = pos.peakPnlPct * (this.config.trailingRetainPct / 100);
          if (pnlPct < retainedFloor) {
            shouldExit = true;
            exitReason = 'TRAILING_STOP';
          }
        } else if (pnlPct > 0 && elapsedSeconds > 30) {
          // Reversão de fluxo: positivo, mas vendedores passaram a dominar (sinal de momentum morto)
          try {
            const fb = await this.detectFirstBuys(mint);
            if (fb.sellCount > 0 && fb.buySellRatio < 1) {
              shouldExit = true;
              exitReason = 'MOMENTUM_REVERSAL';
            }
          } catch (e) {}
        }

        if (shouldExit) {
          clearInterval(interval);
          this._positionMonitors.delete(mint);
          if (!this.sendTransactions) {
            exitSolOut = await this._paperExitSolOut(mint, pos.tokenAmount, pos.entrySol);
            exitPrice = pos.tokenAmount > 0 ? exitSolOut / pos.tokenAmount : pos.entryPrice;
          }
          await this.closePosition(mint, exitReason, exitPrice, exitSolOut);
        }
      } catch (e) {}
    }, this.config.monitorIntervalMs);
    this._positionMonitors.set(mint, interval);
  }

  async closePosition(mint, reason, exitPrice, exitSolOut = null) {
    const position = this.positions.get(mint);
    if (!position || position.status !== 'OPEN') return;
    position.status = 'CLOSING';

    this.log(`[EXIT] ${position.positionId} mint=${mint.slice(0,16)} reason=${reason} exitPrice=${exitPrice ? exitPrice.toFixed(8) : 'N/A'}`, 'sell');

    let grossPnlPct;
    let pnlSOL;
    const feePct = this.config.paperFeeBps / 100;
    const slippagePct = this.config.paperSlippageBps / 100;

    if (!this.sendTransactions && exitSolOut != null) {
      pnlSOL = exitSolOut - position.entrySol;
      grossPnlPct = position.entrySol > 0 ? (pnlSOL / position.entrySol) * 100 : 0;
    } else {
      const quote = exitPrice > 0 ? null : await this.getSellQuote(mint, position.tokenAmount);
      const finalExitPrice = exitPrice > 0 ? exitPrice : (quote?.outAmount ? (parseInt(quote.outAmount) / LAMPORTS_PER_SOL) / position.tokenAmount : position.entryPrice);
      grossPnlPct = ((finalExitPrice - position.entryPrice) / position.entryPrice) * 100;
      pnlSOL = position.entrySol * (grossPnlPct / 100);
      exitPrice = finalExitPrice;
    }

    const netPnlPct = grossPnlPct - feePct - slippagePct;
    pnlSOL = position.entrySol * (netPnlPct / 100);
    const finalExitPrice = exitPrice || position.entryPrice;

    position.status = 'CLOSED';
    position.exitPrice = finalExitPrice;
    position.exitReason = reason;
    position.closedAt = new Date().toISOString();
    position.grossPnlPct = grossPnlPct;
    position.netPnlPct = netPnlPct;
    position.pnlSOL = pnlSOL;

    if (!this.sendTransactions) {
      this.paperCash += position.entrySol + pnlSOL;
      if (this.paperCash < 0) this.paperCash = 0;
      if (this.sendTransactions) this.equity = this.wallet.balanceSOL || 0;
      else this.equity = this.paperCash;
    } else {
      // Real mode: refresh wallet to get updated balance
      await this.refreshWallet();
      this.equity = this.wallet.balanceSOL || 0;
    }

    this.recordTrade(mint, netPnlPct, pnlSOL, reason.toLowerCase(), {
      entrySol: position.entrySol,
      exitReason: reason,
      entryScore: position.entryScore || 0,
      holdTimeMs: Date.now() - position.entryAt
    });

    this.metrics.avgPositionTimeMs = (this.metrics.avgPositionTimeMs * (this.metrics.winningTrades + this.metrics.losingTrades - 1) + (Date.now() - position.entryAt)) / (this.metrics.winningTrades + this.metrics.losingTrades);

    this.logDecision({
      mint, positionId: position.positionId, side: 'SELL', signal: reason,
      entryPrice: position.entryPrice, exitPrice: finalExitPrice,
      grossPnl: grossPnlPct, fees: feePct, slippage: slippagePct,
      netPnl: netPnlPct, profitPct: netPnlPct
    });

    this.emitStatus();

    // Verifica se atingiu meta/loss do dia após fechar posição
    this.checkDailyLimits();

    setTimeout(() => {
      this.positions.delete(mint);
      this.monitoredTokens.delete(mint);
    }, 1000);
  }

  stop() {
    this.running = false;
    this.setState('idle');
    if (this.ws) { try { this.ws.close(); } catch (e) {} this.ws = null; }
    if (this.monitorHandle) { clearInterval(this.monitorHandle); this.monitorHandle = null; }
    if (this.pollingHandle) { clearInterval(this.pollingHandle); this.pollingHandle = null; }
    if (this.autoSimulationHandle) { clearInterval(this.autoSimulationHandle); this.autoSimulationHandle = null; }
    if (this.walletRefreshHandle) { clearInterval(this.walletRefreshHandle); this.walletRefreshHandle = null; }
    this.wsConnected = false;
    const openCount = this.positions.size;
    if (openCount > 0) {
      this.log(`⏸️  Bot parado (novas entradas). ${openCount} posição(ões) aberta(s) continuam sendo monitoradas até fechar para atualizar o saldo.`, 'warn');
    } else {
      this.log('Bot parado.', 'warn');
    }
    this.emitStatus();
    saveState(this);
  }

  checkDailyLimits() {
    if (!this.running || this.haltNewEntries) return true;
    const base = this.dayStartEquity || this.config.paperInitialSol || 1;
    const dayPnlPct = (this.profitLoss / base) * 100;
    const dayPnlSol = this.profitLoss;

    if (dayPnlPct >= this.config.dailyTargetPct) {
      this.haltNewEntries = true;
      this.log(`🎯 META DO DIA ATINGIDA! Lucro de +${dayPnlPct.toFixed(2)}% (+${dayPnlSol.toFixed(4)} SOL). Novas entradas pausadas, posições abertas serão fechadas.`, 'success');
      this.emit('limit', { kind: 'target', pnlPct: dayPnlPct, pnlSol: dayPnlSol, message: 'META DO DIA ATINGIDA' });
      this.emitStatus();
      return true;
    }
    if (dayPnlPct <= -this.config.dailyLossPct) {
      this.haltNewEntries = true;
      this.log(`🛑 LOSS DO DIA ATINGIDO! Prejuízo de ${dayPnlPct.toFixed(2)}% (${dayPnlSol.toFixed(4)} SOL). Negociação parada, posições abertas serão fechadas.`, 'error');
      this.emit('limit', { kind: 'loss', pnlPct: dayPnlPct, pnlSol: dayPnlSol, message: 'LOSS DO DIA ATINGIDO' });
      this.emitStatus();
      return true;
    }
    return false;
  }

  uid() { return Math.random().toString(36).slice(2, 10); }
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
export { DEFAULTS, MODES, UPGRADEABLE_LOADER };