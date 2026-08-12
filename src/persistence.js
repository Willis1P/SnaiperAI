import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { createEmptyMetrics } from './constants.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = path.join(__dirname, '..', 'data');
const STATE_FILE = path.join(DATA_DIR, 'bot-state.json');

export async function loadState() {
  try {
    const raw = await fs.readFile(STATE_FILE, 'utf8');
    return JSON.parse(raw);
  } catch (e) {
    if (e.code === 'ENOENT') return null;
    console.error('[persistence] Erro ao carregar estado:', e.message);
    return null;
  }
}

export async function saveState(bot) {
  try {
    await fs.mkdir(DATA_DIR, { recursive: true });
    const positions = {};
    for (const [mint, pos] of bot.positions) {
      positions[mint] = pos;
    }
    const state = {
      savedAt: new Date().toISOString(),
      tradeHistory: bot.tradeHistory,
      metrics: {
        ...bot.metrics,
        detectionLatencies: bot.metrics.detectionLatencies.slice(-100),
        validationLatencies: bot.metrics.validationLatencies.slice(-100),
        quoteLatencies: bot.metrics.quoteLatencies.slice(-100),
        executionLatencies: bot.metrics.executionLatencies.slice(-100),
        totalLatencies: bot.metrics.totalLatencies.slice(-100),
        equityCurve: bot.metrics.equityCurve.slice(-500)
      },
      positions,
      paperCash: bot.paperCash,
      equity: bot.equity,
      profitLoss: bot.profitLoss,
      tradeCount: bot.tradeCount,
      positionCounter: bot.positionCounter,
      startOfDayEquity: bot.startOfDayEquity
    };
    await fs.writeFile(STATE_FILE, JSON.stringify(state, null, 2), 'utf8');
    return true;
  } catch (e) {
    console.error('[persistence] Erro ao salvar estado:', e.message);
    return false;
  }
}

export function applyStateToBot(bot, state) {
  if (!state) return false;
  if (Array.isArray(state.tradeHistory)) bot.tradeHistory = state.tradeHistory.slice(-500);
  if (state.metrics) bot.metrics = { ...createEmptyMetrics(), ...state.metrics };
  if (state.positions) {
    bot.positions.clear();
    for (const [mint, pos] of Object.entries(state.positions)) {
      bot.positions.set(mint, pos);
    }
  }
  if (state.paperCash !== undefined) bot.paperCash = state.paperCash;
  if (state.equity !== undefined) bot.equity = state.equity;
  if (state.profitLoss !== undefined) bot.profitLoss = state.profitLoss;
  if (state.tradeCount !== undefined) bot.tradeCount = state.tradeCount;
  if (state.positionCounter !== undefined) bot.positionCounter = state.positionCounter;
  if (state.startOfDayEquity !== undefined) bot.startOfDayEquity = state.startOfDayEquity;
  return true;
}

export async function clearPersistedState() {
  try {
    await fs.unlink(STATE_FILE);
    return true;
  } catch (e) {
    if (e.code === 'ENOENT') return true;
    return false;
  }
}
