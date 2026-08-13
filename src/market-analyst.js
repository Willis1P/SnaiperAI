/**
 * Estratégia de análise de mercado estilo analista crypto/forex para Pump.fun.
 * Fases: LAUNCH → ACCUMULATION → EXPANSION → DISTRIBUTION / LATE
 */

export const PHASE = {
  LAUNCH: 'LAUNCH',
  ACCUMULATION: 'ACCUMULATION',
  EXPANSION: 'EXPANSION',
  DISTRIBUTION: 'DISTRIBUTION',
  LATE: 'LATE'
};

const GRADUATION_SOL = 69;

function clamp(n, min, max) {
  return Math.min(max, Math.max(min, n));
}

function curveProgress(realSol, graduationSol = GRADUATION_SOL) {
  return clamp((realSol / graduationSol) * 100, 0, 100);
}

function detectPhase(liquidityInfo, firstBuys, tokenAgeSec) {
  const realSol = liquidityInfo.realSolReserves || 0;
  const progress = curveProgress(realSol);
  const sells = firstBuys?.sellCount || 0;
  const buyers = firstBuys?.uniqueBuyers || 0;
  const ratio = firstBuys?.buySellRatio || 0;

  if (progress >= 85) return PHASE.LATE;
  if (sells > 0 && ratio < 0.7 && buyers >= 2) return PHASE.DISTRIBUTION;
  if (realSol < 2 && tokenAgeSec < 120) return PHASE.LAUNCH;
  if (buyers >= 3 && ratio >= 1.5 && realSol >= 1.5) return PHASE.EXPANSION;
  if (buyers >= 2 || realSol >= 2) return PHASE.ACCUMULATION;
  return PHASE.LAUNCH;
}

function flowQualityScore(firstBuys, liquidityInfo) {
  const fb = firstBuys || {};
  let q = 0;

  // Diversificação de compradores (0-25)
  q += Math.min(25, (fb.uniqueBuyers || 0) * 8);

  // Pressão compradora líquida (0-20)
  const netFlow = (fb.buyVolume || 0) - (fb.sellVolume || 0);
  if (netFlow > 0) q += Math.min(20, netFlow * 2);
  else q -= 10;

  // Ratio buy/sell (0-15) — ignora ratios absurdos (dados corrompidos)
  const ratio = Math.min(fb.buySellRatio || 0, 10);
  if (ratio >= 2) q += 15;
  else if (ratio >= 1.2) q += 10;
  else if (ratio >= 0.9) q += 5;
  else q -= 8;

  // Continuidade temporal — fluxo sustentado, não burst único (0-10)
  if ((fb.timeSpanSeconds || 0) >= 8) q += 10;
  else if ((fb.timeSpanSeconds || 0) >= 4) q += 5;

  // Concentração — penaliza whale dominance (0 a -20)
  const conc = fb.topBuyerShare || 1;
  if (conc <= 0.35) q += 10;
  else if (conc <= 0.55) q += 5;
  else if (conc > 0.75) q -= 15;

  // Liquidez real confirma interesse (0-15)
  const realSol = liquidityInfo.realSolReserves || 0;
  q += Math.min(15, realSol * 5);

  // Dados reais vs estimados
  if (fb.estimated || fb.rpcDegraded) q -= 12;

  return clamp(Math.round(q), 0, 100);
}

function structureScore(liquidityInfo) {
  const realSol = liquidityInfo.realSolReserves || 0;
  const virtualSol = liquidityInfo.virtualSolReserves || 0;
  let s = 0;

  // Sweet spot: 2-25 SOL real — momentum sem estar no fim da curva
  if (realSol >= 2 && realSol <= 25) s += 30;
  else if (realSol >= 1 && realSol < 2) s += 18;
  else if (realSol > 25 && realSol < 50) s += 12;
  else if (realSol >= 50) s += 5;
  else s += 5;

  // Pool virtual saudável
  if (virtualSol >= 30) s += 15;
  else if (virtualSol >= 15) s += 8;

  // Progresso da curva — zona de maior upside (15-60%)
  const progress = curveProgress(realSol);
  if (progress >= 15 && progress <= 60) s += 20;
  else if (progress < 15) s += 12;
  else s += 5;

  return clamp(s, 0, 65);
}

function riskScore(firstBuys, liquidityInfo, tokenInfo) {
  let r = 0;
  const fb = firstBuys || {};

  if (fb.estimated || fb.rpcDegraded) r += 18;
  if ((liquidityInfo.realSolReserves || 0) < 0.5) r += 15;
  if ((fb.topBuyerShare || 0) > 0.7) r += 20;
  if ((fb.sellCount || 0) > 0 && (fb.buySellRatio || 0) < 1) r += 25;
  if ((fb.uniqueBuyers || 0) < 2) r += 12;
  if (tokenInfo?.freezeAuthority && tokenInfo.freezeAuthority !== 'unknown') r += 10;
  if (curveProgress(liquidityInfo.realSolReserves || 0) > 80) r += 15;

  return clamp(r, 0, 100);
}

function convictionFromScore(score, flowQ, risk, phase) {
  if (phase === PHASE.LAUNCH || phase === PHASE.DISTRIBUTION || phase === PHASE.LATE) return 'REJECT';
  if (risk >= 55 || flowQ < 30) return 'REJECT';

  const adjusted = score - risk * 0.3 + flowQ * 0.15;
  if (adjusted >= 82 && flowQ >= 55 && risk <= 25) return 'A+';
  if (adjusted >= 72 && flowQ >= 40 && risk <= 35) return 'A';
  if (adjusted >= 62 && flowQ >= 30 && risk <= 45) return 'B';
  if (adjusted >= 52 && flowQ >= 22) return 'C';
  return 'REJECT';
}

function exitProfileForPhase(phase, config) {
  const base = {
    targetProfitPct: config.targetProfitPct ?? 20,
    stopLossPct: config.stopLossPct ?? 8,
    maxTimeSeconds: config.maxPositionTimeSeconds ?? 180,
    partialTakeProfitPct: config.partialTakeProfitPct ?? 10,
    trailingActivatePct: config.trailingActivatePct ?? 8,
    breakevenActivatePct: config.breakevenActivatePct ?? 4
  };

  switch (phase) {
    case PHASE.LAUNCH:
      return {
        ...base,
        targetProfitPct: Math.min(base.targetProfitPct, 15),
        stopLossPct: 5,
        maxTimeSeconds: 90,
        partialTakeProfitPct: 7,
        trailingActivatePct: 5,
        breakevenActivatePct: 3
      };
    case PHASE.EXPANSION:
      return {
        ...base,
        targetProfitPct: base.targetProfitPct + 3,
        stopLossPct: 7,
        maxTimeSeconds: 200,
        partialTakeProfitPct: 7,
        trailingActivatePct: 6
      };
    case PHASE.ACCUMULATION:
      return { ...base, partialTakeProfitPct: 7, trailingActivatePct: 5 };
    default:
      return { ...base, stopLossPct: 5, maxTimeSeconds: 90 };
  }
}

function sizeMultiplierForConviction(conviction) {
  switch (conviction) {
    case 'A+': return 1.0;
    case 'A': return 0.85;
    case 'B': return 0.65;
    case 'C': return 0.45;
    default: return 0;
  }
}

function flowGrade(flowQ, firstBuys, liquidityInfo) {
  if (!firstBuys) return { ok: false, grade: 'REJEITAR', reason: 'sem dados de fluxo' };
  if (firstBuys.estimated && (liquidityInfo.realSolReserves || 0) < 1.5) {
    return { ok: false, grade: 'AGUARDAR', reason: 'fluxo estimado — aguardando compradores reais' };
  }
  if (flowQ >= 45) return { ok: true, grade: 'OPORTUNIDADE', reason: `fluxo forte (Q=${flowQ})` };
  if (flowQ >= 28) return { ok: true, grade: 'OPORTUNIDADE', reason: `fluxo moderado (Q=${flowQ})` };
  if (flowQ >= 18) return { ok: false, grade: 'AGUARDAR', reason: `fluxo fraco (Q=${flowQ}) — aguardar confirmação` };
  return { ok: false, grade: 'REJEITAR', reason: `fluxo insuficiente (Q=${flowQ})` };
}

/**
 * Análise completa de mercado para decisão de entrada.
 */
export function analyzeMarket({ tokenInfo, liquidityInfo, firstBuys, config, entryScoreWeights }) {
  const w = entryScoreWeights || {};
  const tokenAgeSec = tokenInfo?.detectedAt
    ? (Date.now() - new Date(tokenInfo.detectedAt).getTime()) / 1000
    : 0;

  const phase = detectPhase(liquidityInfo, firstBuys, tokenAgeSec);
  const flowQ = flowQualityScore(firstBuys, liquidityInfo);
  const structure = structureScore(liquidityInfo);
  const risk = riskScore(firstBuys, liquidityInfo, tokenInfo);
  const progress = curveProgress(liquidityInfo.realSolReserves || 0);

  // Score composto estilo analista
  let score = 0;
  score += structure;
  score += flowQ * 0.45;
  score += (w.newToken || 10) * 0.3;
  score += (w.tradable || 15) * 0.3;
  score -= risk * 0.35;
  if (phase === PHASE.EXPANSION) score += 8;
  if (phase === PHASE.DISTRIBUTION || phase === PHASE.LATE) score -= 20;
  score = clamp(Math.round(score), 0, 100);

  const conviction = convictionFromScore(score, flowQ, risk, phase);
  const flow = flowGrade(flowQ, firstBuys, liquidityInfo);
  const exitProfile = exitProfileForPhase(phase, config);
  const sizeMultiplier = sizeMultiplierForConviction(conviction);

  const minScore = config.minEntryScore ?? 70;
  const minRealSol = config.minLiquiditySol ?? 2;
  let recommendation = 'REJECT';
  if ((liquidityInfo.realSolReserves || 0) < minRealSol) {
    recommendation = 'WAIT';
  } else if (conviction !== 'REJECT' && score >= minScore && flow.ok && phase !== PHASE.LAUNCH) {
    recommendation = conviction === 'C' ? 'WAIT' : 'ENTER';
  } else if (score >= minScore - 10 && !flow.ok && flow.grade === 'AGUARDAR') {
    recommendation = 'WAIT';
  }

  const signals = [];
  const risks = [];
  if (flowQ >= 50) signals.push('fluxo_comprador_forte');
  if ((firstBuys?.uniqueBuyers || 0) >= 3) signals.push('diversificacao_compradores');
  if (phase === PHASE.EXPANSION) signals.push('fase_expansao');
  if (progress >= 15 && progress <= 55) signals.push('zona_upside_curva');
  if ((firstBuys?.sellCount || 0) === 0 && (firstBuys?.uniqueBuyers || 0) >= 2) signals.push('momentum_puro');

  if (firstBuys?.estimated) risks.push('dados_fluxo_estimados');
  if (risk >= 40) risks.push('risco_elevado');
  if ((firstBuys?.topBuyerShare || 0) > 0.6) risks.push('concentracao_whale');
  if (phase === PHASE.LAUNCH) risks.push('fase_inicial_alto_risco');

  return {
    phase,
    score,
    flowQuality: flowQ,
    structureScore: structure,
    riskScore: risk,
    curveProgress: progress,
    conviction,
    flow,
    exitProfile,
    sizeMultiplier,
    recommendation,
    signals,
    risks,
    summary: `${phase} | score=${score} flowQ=${flowQ} risk=${risk} → ${conviction}`
  };
}

export function classifyFromAnalysis(analysis, config) {
  if (analysis.recommendation === 'REJECT' || analysis.conviction === 'REJECT') return '🔴 REJEITAR';
  if (analysis.conviction === 'A+' && analysis.flow.ok) return '🔵 ALTA CONVICÇÃO';
  if (analysis.recommendation === 'ENTER' && analysis.flow.ok) return '🟢 OPORTUNIDADE';
  if (analysis.recommendation === 'WAIT' || analysis.flow.grade === 'AGUARDAR') return '🟡 AGUARDAR CONFIRMAÇÃO';
  return '🟠 OBSERVAR';
}
