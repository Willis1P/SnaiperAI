import { PublicKey } from '@solana/web3.js';

export const PUMP_FUN_PROGRAM = new PublicKey('6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P');
export const WRAPPED_SOL = 'So11111111111111111111111111111111111111112';
export const JUPITER_QUOTE_API = 'https://quote-api.jup.ag/v6/quote';
export const JUPITER_SWAP_API = 'https://quote-api.jup.ag/v6/swap';
export const UPGRADEABLE_LOADER = 'BPFLoaderUpgradeab1e11111111111111111111111';

export const MODES = ['mock', 'paper_mainnet', 'simulate_rpc', 'live_mainnet'];

export const DEFAULTS = {
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
  pumpFunProgram: PUMP_FUN_PROGRAM.toString(),
  jupiterApiKey: '',
  buyAmountSol: 0.015,
  sellTriggerPct: 20,
  stopLossPct: 8,
  slippageBps: 500,
  priorityFeeLamports: 10000,
  maxBondingCurve: 100,
  autoSellOnBuy: true,
  monitorIntervalMs: 1000,
  paperInitialSol: 1.0,
  paperFeeBps: 100,
  paperSlippageBps: 50,
  paperLatencyMs: 100,
  maxSolPerTrade: 0.05,
  maxDailyLossSol: 0.20,
  maxOpenPositions: 10,
  minEntryScore: 40,
  targetProfitPct: 20,
  maxPositionTimeSeconds: 300,
  minLiquiditySol: 1,
  minVirtualSolReserves: 5,
  firstBuyWindowSeconds: 10,
  firstBuyCount: 1,
  minUniqueBuyers: 1,
  forceEntryOnNewLaunch: true,
  maxNewLaunchAgeSeconds: 30,
  autoSimulation: true,
  autoSimulationIntervalMs: 15000,
  autoSimulationWinRate: 0.65,
  entryScoreWeights: {
    newToken: 30,
    tradable: 20,
    liquidity: 10,
    firstBuys: 15,
    buyPressure: 10,
    uniqueBuyers: 5,
    lowRisk: 10
  }
};

export function createEmptyMetrics() {
  return {
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
    equityCurve: []
  };
}
