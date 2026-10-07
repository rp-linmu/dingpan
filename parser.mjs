// parser.mjs — 滚动撮合数据解析器（基于 2026-09-23 广西平台真实接口样本固化）
//
// 数据源：POST /GXJYHD/qctc-pm-trade-zcq-jzjz-facade/jzjz/facade/findBuy5AndSell5
// 页面自身每 ~5.2s 请求一次（自动刷新），我们纯被动解析。
//
// 契约：parseSnapshot({key, body, ts}) 返回
//   {
//     windows: [{id, label, price, volume, extras:{avg, high, low, centralized}}],  // 48个时段
//     selectedCode: 'D20260925_H02',   // 页面当前选中的窗口
//     book: {buys:[{price,qty}], sells:[{price,qty}]},  // 选中窗口买卖五档
//     newTrades: [{code, price, energy, matchTime}],    // 最新成交明细
//   }
//   或 null

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
const ROOT = path.dirname(fileURLToPath(import.meta.url));

const cfg = fs.existsSync(path.join(ROOT, 'config.json'))
  ? JSON.parse(fs.readFileSync(path.join(ROOT, 'config.json'), 'utf8')) : {};

const NUM = v => {
  const n = typeof v === 'number' ? v : (typeof v === 'string' && v !== '' && !isNaN(v) ? parseFloat(v) : null);
  return (n === 0 || n == null) ? null : n; // 0 视为无数据（该市场不存在0价）
};

// jydm: D20260925_H00 → 2026-09-25T00 → 标签 "9/25 00:00-01:00"
function windowFromJydm(jydm) {
  const m = /^D(\d{4})(\d{2})(\d{2})_H(\d{2})$/.exec(String(jydm || ''));
  if (!m) return null;
  const [, y, mo, d, h] = m;
  const id = `${y}-${mo}-${d}T${h}`;
  const hi = +h;
  return { id, label: `${+mo}/${+d} ${h}:00-${String((hi + 1) % 24).padStart(2, '0')}:00` };
}

function parseLive(body) {
  const data = body && body.data;
  if (!data || !Array.isArray(data.findMarketCountInfo)) return null;

  const windows = [];
  for (const r of data.findMarketCountInfo) {
    const w = windowFromJydm(r.jydm);
    if (!w) continue;
    windows.push({
      id: w.id,
      label: w.label,
      price: NUM(r.currentPrice),           // 最新成交价
      volume: NUM(r.totalBidEnergy),        // 累计成交量
      extras: {
        avg: NUM(r.avgBidPrice),            // 成交均价
        high: NUM(r.highBidPrice),          // 最高价
        low: NUM(r.lowBidPrice),            // 最低价
        centralized: NUM(r.centralizedPrice), // 集中竞争价（参考基准）
      },
    });
  }
  if (!windows.length) return null;

  // 选中窗口的买卖五档：type 含"供"为卖方，其余（购买/需求）为买方
  let book = null;
  if (Array.isArray(data.buy5AndSell5) && data.buy5AndSell5.length) {
    const buys = [], sells = [];
    for (const x of data.buy5AndSell5) {
      const item = { price: NUM(x.PRICE), qty: NUM(x.DUMPENERGY) };
      if (!item.price || !item.qty) continue;
      (/供/.test(String(x.type)) ? sells : buys).push(item);
    }
    buys.sort((a, b) => b.price - a.price);
    sells.sort((a, b) => b.price - a.price);
    book = { buys, sells };
  }

  const newTrades = (data.newTradeInfo || []).map(t => ({
    code: t.code, window: windowFromJydm(t.code)?.label || t.code,
    price: NUM(t.price), energy: NUM(t.energy), matchTime: t.matchTime,
  })).filter(t => t.price);

  return { windows, selectedCode: data.buy5AndSell5 ? null : null, book, newTrades };
}

// 备用：config.mapping 显式路径通道（其他平台/接口时使用）
function getByPath(obj, p) {
  return String(p).split('.').reduce((o, k) => (o == null ? undefined : o[k]), obj);
}
function parseByMapping(body) {
  const m = cfg.mapping;
  if (!m || !m.arrayPath) return null;
  const arr = getByPath(body, m.arrayPath);
  if (!Array.isArray(arr)) return null;
  const rows = arr.map((r, i) => ({
    id: m.idField ? String(r[m.idField]) : 'idx-' + i,
    label: m.labelField ? String(r[m.labelField]) : (m.idField ? String(r[m.idField]) : 'idx-' + i),
    price: NUM(m.priceField ? r[m.priceField] : null),
    volume: m.volumeField ? NUM(r[m.volumeField]) : null,
  })).filter(r => r.price != null);
  return rows.length ? { windows: rows } : null;
}

export function parseSnapshot({ key, body }) {
  if (Array.isArray(cfg.endpoints) && cfg.endpoints.length) {
    const hit = cfg.endpoints.some(e => key.includes(e));
    if (!hit) return null;
  }
  return parseLive(body) || parseByMapping(body);
}
