export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export const uid = () => Math.random().toString(36).slice(2, 10);

export function debounce(fn, ms = 1000) {
  let timer = null;
  return (...args) => {
    if (timer) clearTimeout(timer);
    timer = setTimeout(() => fn(...args), ms);
  };
}
