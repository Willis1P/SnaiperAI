import https from 'https';
import { Connection } from '@solana/web3.js';

export function isInsecureTls() {
  return process.env.RPC_INSECURE_TLS === 'true';
}

export function createConnection(rpcUrl, options = {}) {
  const config = { commitment: 'confirmed', ...options };
  if (isInsecureTls()) {
    config.httpAgent = new https.Agent({ rejectUnauthorized: false });
  }
  return new Connection(rpcUrl, config);
}

export function wsTlsOptions() {
  return isInsecureTls() ? { rejectUnauthorized: false } : undefined;
}

export function axiosTlsConfig() {
  return isInsecureTls()
    ? { httpsAgent: new https.Agent({ rejectUnauthorized: false }) }
    : {};
}
