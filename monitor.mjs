#!/usr/bin/env node
// monitor.mjs — 滚动撮合被动监听 + 本地看板服务
//
// 原理：通过 CDP attach 到调试版 Chrome 的所有平台标签页，被动监听页面自己发出的
//       XHR/fetch/WebSocket 数据响应（从 Chrome 缓冲区取已收到的响应体），对平台零额外请求。
//
// 用法：
//   node monitor.mjs --discover [--minutes 3]   发现模式：监听N分钟，接口样本存 data/samples/
//   node monitor.mjs                            监控模式：按 config.json 解析，起 localhost:8787 看板
//
// 防封禁约束（硬性）：
//   - 主模式对平台零请求（被动监听，getResponseBody 不访问服务器）
//   - 兜底轮询（仅 config.poll 显式启用）：页面上下文内 fetch，分钟级 + 随机抖动，失败退避
//   - Cookie/认证头只在页面上下文中使用，不打印、不落盘

import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import crypto from 'node:crypto';
import zlib from 'node:zlib';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { sleep } from './lib/cdp-lib.mjs';

const CDP_HTTP = 'http://127.0.0.1:9222';
// Chrome 154+ 可能把调试端口绑到 IPv6 [::1]，逐个尝试双栈
const CDP_BASES = ['http://127.0.0.1:9222', 'http://[::1]:9222'];
let cdpBaseActive = null;
async function cdpFetch(pathname) {
  let lastErr = null;
  for (const base of CDP_BASES) {
    try {
      const r = await fetch(base + pathname, { signal: AbortSignal.timeout(3000) });
      const j = await r.json();
      cdpBaseActive = base;
      return j;
    } catch (e) { lastErr = e; }
  }
  throw (lastErr instanceof Error ? lastErr : new Error('9222 不可达'));
}
// 把 ws 地址的主机规范为实际可达的那个（127.0.0.1 或 [::1]）
function fixWsUrl(u) {
  if (!u || !cdpBaseActive) return u;
  const host = cdpBaseActive === 'http://[::1]:9222' ? '[::1]' : '127.0.0.1';
  return u.replace(/^ws:\/\/[^/]+\//, `ws://${host}:9222/`);
}
const HOST = 'pm.gx.csg.cn';          // 只捕获该平台的流量
const PORT = +(process.env.DINGPAN_PORT || 8787);   // 本地看板端口（可用环境变量并行跑第二实例）
const ROOT = path.dirname(fileURLToPath(import.meta.url));
const DATA = path.join(ROOT, 'data');
const SAMPLES = path.join(DATA, 'samples');

const args = process.argv.slice(2);
const MODE_DISCOVER = args.includes('--discover');
const MODE_DEMO = args.includes('--demo');
const minutesIdx = args.indexOf('--minutes');
const MINUTES = minutesIdx >= 0 ? Math.max(0.5, parseFloat(args[minutesIdx + 1]) || 3) : 3;

fs.mkdirSync(SAMPLES, { recursive: true });

// ---------------- CDP 连接（浏览器级，flatten 会话） ----------------

class Cdp {
  constructor(ws) {
    this.ws = ws;
    this.id = 0;
    this.pending = new Map();
    this.listeners = [];
    ws.addEventListener('message', ev => {
      const m = JSON.parse(ev.data);
      if (m.id && this.pending.has(m.id)) {
        const p = this.pending.get(m.id); this.pending.delete(m.id);
        m.error ? p.rej(new Error(m.error.message)) : p.res(m.result);
      } else if (m.method) {
        for (const fn of this.listeners) { try { fn(m); } catch (e) { console.error('[listener]', e.message); } }
      }
    });
  }
  cmd(method, params = {}, sessionId) {
    return new Promise((res, rej) => {
      const i = ++this.id; this.pending.set(i, { res, rej });
      const msg = { id: i, method, params };
      if (sessionId) msg.sessionId = sessionId;
      this.ws.send(JSON.stringify(msg));
    });
  }
  on(fn) { this.listeners.push(fn); }
}

async function connectBrowser() {
  let ver;
  try {
    ver = await cdpFetch('/json/version');
  } catch (e) {
    throw new Error('无法连接 Chrome 调试端口 9222 —— 请先运行「启动Chrome调试.bat」并登录平台');
  }
  const ws = new WebSocket(fixWsUrl(ver.webSocketDebuggerUrl));
  await new Promise((res, rej) => {
    ws.addEventListener('open', res);
    ws.addEventListener('error', () => rej(new Error('browser-ws 连接失败')));
  });
  return new Cdp(ws);
}

// ---------------- 事件处理与捕获 ----------------

const fmtTs = s => new Date(s * 1000).toLocaleTimeString('zh-CN', { hour12: false, timeZone: 'Asia/Shanghai' });
const sha1 = s => crypto.createHash('sha1').update(s).digest('hex').slice(0, 12);
const median = a => (a.length < 2 ? null : [...a].sort((x, y) => x - y)[Math.floor((a.length - 1) / 2)]);

// 每个端点的捕获桶
class Buckets {
  constructor() { this.map = new Map(); }
  get(key) {
    if (!this.map.has(key)) this.map.set(key, {
      key, count: 0, tsList: [], samples: [], lastPostData: null, lastStatus: null, ws: false,
    });
    return this.map.get(key);
  }
  all() { return [...this.map.values()]; }
}

const buckets = new Buckets();
const inflight = new Map(); // sid:requestId -> {url, method, postData, status, mime, wantBody, wallTime}

function handleResponseBody(url, method, postData, status, mime, bodyStr, wallTime, headers) {
  let u; try { u = new URL(url); } catch { return; }
  if (!u.hostname.includes(HOST)) return;
  if (method === 'OPTIONS') return;
  if (status === 401) { state.meta.sessionExpired = true; console.log(`  [${fmtTs(wallTime)}] ${u.pathname} 401 —— 会话可能过期，请在调试 Chrome 里重新登录`); }
  // 只要疑似 JSON（部分接口 mime 不规范，做内容兜底）
  const looksJson = (mime || '').includes('json') || /^\s*[{[]/.test(bodyStr || '');
  if (!looksJson) return;
  const key = method + ' ' + u.pathname;
  const b = buckets.get(key);
  b.count++; b.tsList.push(wallTime); b.lastStatus = status;
  if (method === 'WS') return; // WebSocket 帧不存 postData/样本体
  if (postData) b.lastPostData = postData.slice(0, 4000);
  if (b.samples.length < 6 && bodyStr.length <= 400 * 1024) {
    b.samples.push({ ts: wallTime, status, body: bodyStr });
  }
  const dt = b.tsList.length > 1 ? (wallTime - b.tsList[b.tsList.length - 2]).toFixed(1) + 's' : '-';
  console.log(`  [${fmtTs(wallTime)}] ${key}  ${status} ${(bodyStr.length / 1024).toFixed(1)}KB  #${b.count}  Δ${dt}`);
  onPayload && onPayload({ key, url, method, path: u.pathname, postData, bodyStr, wallTime, status, headers });
}

let onPayload = null; // 监控模式的回调
// 页面自身最近一次盘口查询（url+请求体模板+凭证请求头；头仅内存使用，不落盘不打印）
let lastQuery = { url: null, template: null, headers: null, ts: 0 };

async function attachAll(cdp) {
  // 浏览器级自动 attach（flatten），所有标签页/iframe 的网络事件汇聚到本连接
  cdp.on(async m => {
    if (m.method === 'Target.attachedToTarget') {
      const { sessionId, targetInfo } = m.params;
      try {
        await cdp.cmd('Network.enable', {
          maxTotalBufferSize: 100e6, maxResourceBufferSize: 50e6,
        }, sessionId);
        console.log(`  已附加目标: ${targetInfo.type} ${targetInfo.url.slice(0, 90)}`);
      } catch (e) { /* 某些目标类型不支持 Network，忽略 */ }
      return;
    }
    const sid = m.sessionId;
    if (!sid) return;
    const p = m.params || {};
    if (m.method === 'Network.requestWillBeSent') {
      inflight.set(sid + ':' + p.requestId, {
        url: p.request.url, method: p.request.method,
        postData: p.request.postData || null, wallTime: p.wallTime || Date.now() / 1000,
        headers: p.request.headers || null,
      });
    } else if (m.method === 'Network.responseReceived') {
      const k = sid + ':' + p.requestId, f = inflight.get(k);
      if (!f) return;
      f.status = p.response.status; f.mime = p.response.mimeType;
      f.wantBody = (p.type === 'XHR' || p.type === 'Fetch') && p.response.status === 200;
    } else if (m.method === 'Network.loadingFinished') {
      const k = sid + ':' + p.requestId, f = inflight.get(k);
      if (!f || !f.wantBody) { inflight.delete(k); return; }
      inflight.delete(k);
      try {
        const rb = await cdp.cmd('Network.getResponseBody', { requestId: p.requestId }, sid);
        const bodyStr = rb.base64Encoded ? Buffer.from(rb.body, 'base64').toString('utf8') : rb.body;
        handleResponseBody(f.url, f.method, f.postData, f.status, f.mime, bodyStr, f.wallTime, f.headers);
      } catch (e) { /* 响应体已被回收，属正常 */ }
    } else if (m.method === 'Network.webSocketFrameReceived') {
      const k = sid + ':' + p.requestId, f = inflight.get(k) || { url: 'wss://' + HOST + '/unknown-ws', wallTime: p.timestamp };
      const data = p.response && p.response.payloadData;
      if (typeof data === 'string') {
        handleResponseBody(f.url, 'WS', null, 200, 'json', data, p.timestamp);
      }
    } else if (m.method === 'Network.webSocketCreated') {
      inflight.set(sid + ':' + p.requestId, { url: p.url, method: '', postData: null, wallTime: Date.now() / 1000 });
    }
  });
  await cdp.cmd('Target.setAutoAttach', { autoAttach: true, waitForDebuggerOnStart: false, flatten: true });
}

// ---------------- 发现模式 ----------------

async function runDiscover() {
  console.log(`=== 发现模式：被动监听 ${MINUTES} 分钟（对平台零请求）===`);
  console.log('请确认：调试 Chrome 已打开并停留在「滚动撮合」页面（页面保持前台或至少不被休眠）。\n');
  let cdp = await connectBrowser();
  await attachAll(cdp);
  const t0 = Date.now();
  while (Date.now() - t0 < MINUTES * 60 * 1000) {
    await sleep(1000);
    if (cdp.ws.readyState > 2) { console.log('连接断开，重连...'); try { cdp = await connectBrowser(); await attachAll(cdp); } catch {} }
  }
  // 汇总写盘
  const idx = [];
  let n = 0;
  for (const b of buckets.all()) {
    n++;
    const slug = b.key.replace(/[^a-zA-Z0-9]+/g, '_').slice(-80);
    const file = `${String(n).padStart(2, '0')}-${slug}.json`;
    const intervals = [];
    for (let i = 1; i < b.tsList.length; i++) intervals.push(+(b.tsList[i] - b.tsList[i - 1]).toFixed(1));
    fs.writeFileSync(path.join(SAMPLES, file), JSON.stringify({
      key: b.key, count: b.count, ws: b.ws, intervals,
      lastPostData: b.lastPostData, samples: b.samples,
    }, null, 2));
    idx.push({
      file, key: b.key, count: b.count, ws: b.ws,
      medianIntervalSec: median(intervals),
      samplePreview: (b.samples[b.samples.length - 1]?.body || '').slice(0, 600),
    });
  }
  fs.writeFileSync(path.join(SAMPLES, '_index.json'), JSON.stringify(idx, null, 2));
  console.log(`\n=== 完成：捕获 ${idx.length} 个端点，样本已存 ${SAMPLES} ===`);
  console.table(idx.map(x => ({
    端点: x.key.slice(0, 70), 次数: x.count, 中位间隔s: x.medianIntervalSec, WS: x.ws ? '是' : '',
  })));
}

// ---------------- 监控模式 ----------------

const state = {
  meta: {
    mode: 'passive', startedAt: new Date().toISOString(), lastDataTs: null,
    captured: 0, activeRequests: 0, sessionExpired: false, note: '',
    endpoints: [],
    // 预期满足监控（重点窗口巡检）：用户选定窗口 + 分钟级间隔，后端定时查五档（走 getBook 节流/缓存/页面上下文）
    patrol: { enabled: false, codes: [], intervalMin: 5, lastRoundTs: null, results: {}, failures: 0, pausedReason: '' },
    // 历史场次捕获：查看历史交易窗口时被过滤的场次数据暂存于此（key=场次日期串），看板提示是否保存
    historyCatch: {},
  },
  windows: [],   // [{id,label,unit,price,prevPrice,firstPrice,volume,history:[{ts,price,volume}],bidAsk,lastTs}]
  orderBook: [], // 如有独立盘口数据
};

// 跨交易日归档：旧标的数据移入 data/state_archive_*.json，从空白开始新标的
// （旧数据完整保留在归档文件，可随时查看/回滚）
function archiveState(reason) {
  const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const f = path.join(DATA, `state_archive_${stamp}.json`);
  try { fs.writeFileSync(f, JSON.stringify(state)); } catch (e) { console.error('[archive]', e.message); }
  console.log(`📦 已归档旧标的数据 → ${path.basename(f)}（原因：${reason}），开始记录新标的`);
  state.windows = [];
  state.meta.book = null; state.meta.newTrades = null;
  state.meta.captured = 0; state.meta.activeRequests = 0;
  state.meta.lastDataTs = null; state.meta.startedAt = new Date().toISOString();
  seenHash.clear();
  pendingInit = pendingSwitch = null;   // 标的边界确认状态一并复位
  // 巡检设置随标的重置（窗口代码已属旧标的），间隔偏好保留
  state.meta.patrol = { ...state.meta.patrol, enabled: false, codes: [], results: {}, failures: 0, pausedReason: '' };
  state.meta.historyCatch = {};   // 待处理历史场次一并清空
}

function loadState() {
  const f = path.join(DATA, 'state.json');
  if (fs.existsSync(f)) {
    try {
      const prev = JSON.parse(fs.readFileSync(f, 'utf8'));
      Object.assign(state, prev);
      state.meta.mode = 'passive'; state.meta.note = '';
      // 跨日检测：上次数据是另一天（北京日）→ 归档旧标的，避免新旧窗口混在一个看板
      if (state.meta.lastDataTs && bjDay(new Date(state.meta.lastDataTs)) !== bjDay()) {
        const oldDay = state.meta.lastDataTs.slice(0, 10);
        archiveState('跨交易日，上次数据日 ' + oldDay);
      }
      // 巡检设置兼容与运行态复位：旧版本 state 无 patrol 字段；设置保留，结果清空重巡
      if (!state.meta.patrol) state.meta.patrol = { enabled: false, codes: [], intervalMin: 5, lastRoundTs: null, results: {}, failures: 0, pausedReason: '' };
      state.meta.patrol = { ...state.meta.patrol, results: {}, failures: 0, pausedReason: '' };
      state.meta.historyCatch = {};   // 运行态：重启后重新捕获
      // 注：历史窗口混入的存量残留不在此处清理（标的跨度可变，无法按天数判定）——
      // 增量防御在 mergeUpdate（历史日期进不来），残留由跨交易日归档自然清掉
    } catch {}
  }
}

let flushTimer = null;
function scheduleFlush() {
  if (MODE_DEMO) return; // 演示数据只进内存，不污染 data/state.json
  if (flushTimer) return;
  flushTimer = setTimeout(() => {
    flushTimer = null;
    try { fs.writeFileSync(path.join(DATA, 'state.json'), JSON.stringify(state)); } catch (e) { console.error('[state]', e.message); }
  }, 1000);
}

// 快照按启动日（北京）滚动，避免单文件无限增长
const bjDay = (d = new Date()) => new Date(d.getTime() + 8 * 3600e3).toISOString().slice(0, 10).replace(/-/g, '');
const SNAP_FILE = path.join(DATA, `snapshots_${bjDay()}.jsonl`);
const seenHash = new Map(); // key -> hash（原始体去重）

async function runMonitor() {
  loadState();
  const cfgPath = path.join(ROOT, 'config.json');
  const cfg = fs.existsSync(cfgPath) ? JSON.parse(fs.readFileSync(cfgPath, 'utf8')) : null;
  let parser = null;
  if (cfg && cfg.parser) {
    parser = await import(pathToFileURL(path.join(ROOT, cfg.parser)));
  }
  console.log(cfg ? `已加载 config.json（${cfg.endpoints?.length || 0} 个目标端点）` : '⚠ 未找到 config.json，仅存原始快照（raw 模式）');
  // 盘口按需查询种子模板（运行中会被页面最新请求覆盖）
  if (cfg && cfg.queryUrl && cfg.queryTemplate && !lastQuery.url) {
    lastQuery = { url: cfg.queryUrl, template: cfg.queryTemplate, ts: 0 };
  }

  onPayload = ({ key, url, postData, headers, bodyStr, wallTime }) => {
    state.meta.captured++;
    state.meta.lastDataTs = new Date(wallTime * 1000).toISOString();
    if (state.meta.sessionExpired) { state.meta.sessionExpired = false; }
    // 记录页面自身的查询请求模板（含选中窗口code与凭证请求头），供盘口按需查询复用
    if (key.includes('findBuy5AndSell5') && postData) {
      try {
        const tpl = JSON.parse(postData);
        if (tpl && tpl.code) lastQuery = { url: url || lastQuery.url, template: tpl, headers: headers || lastQuery.headers, ts: wallTime };
      } catch {}
    }
    const h = sha1(bodyStr);
    const changed = seenHash.get(key) !== h;
    seenHash.set(key, h);
    // 追加原始留痕（仅在内容变化时存完整体，控制体积）
    try {
      fs.appendFileSync(SNAP_FILE, JSON.stringify({
        ts: new Date(wallTime * 1000).toISOString(), key, h, changed,
        body: changed && bodyStr.length <= 200 * 1024 ? bodyStr : undefined,
      }) + '\n');
    } catch {}
    if (parser && changed) {
      try {
        const upd = parser.parseSnapshot({ key, body: JSON.parse(bodyStr), ts: wallTime });
        if (upd) {
          mergeUpdate(upd, wallTime);
          // 盘口归属窗口：来自页面查询请求体里的 code
          if (upd.book && state.meta.book && lastQuery.template?.code) {
            state.meta.book.code = lastQuery.template.code;
          }
        }
      } catch (e) { console.error('[parser]', e.message); }
    }
    scheduleFlush();
    schedulePush(); // 平台刷新后 +0.5s 推送看板
  };

  // 连接（断线自动重连）
  let cdp = null;
  const connectLoop = async () => {
    for (;;) {
      try {
        cdp = await connectBrowser();
        await attachAll(cdp);
        console.log('已连接调试 Chrome，被动监听中（对平台零请求）...');
        return;
      } catch (e) {
        console.error(e.message + '，5秒后重试');
        await sleep(5000);
      }
    }
  };
  await connectLoop();
  (async () => { // 重连守护
    for (;;) {
      await sleep(3000);
      if (!cdp || cdp.ws.readyState > 2) {
        console.log('连接断开，重连...');
        try { cdp = await connectBrowser(); await attachAll(cdp); } catch {}
      }
    }
  })();

  // 可选兜底轮询（仅 config.poll 显式启用）
  if (cfg && cfg.poll) startPoller(() => cdp, cfg.poll);

  patrolLoop();   // 预期满足监控巡检（后台循环，按 state.meta.patrol 设置工作）
  startServer();
}

// 标的边界防御：响应无标的标识字段（marketId 等为空），标的跨度也不固定（实测有2/4/5个交易日），
// 只能按窗口日期防御"查看历史交易窗口"时的多标的混入（实测一次响应可含5个交易日120窗口）：
//   1) 初始化二次确认：state 为空时首个响应只挂起，第二个响应到达后取两者日期交集——
//      启动恰逢查看历史时，历史日期不在两次响应的交集中，天然滤掉；多日标的（96/120窗）完整保留
//   2) 历史混入过滤：响应中出现当前标的之外的更旧日期 → 只合并当前标的日期内的窗口；
//      过滤后为空 = 页面正停留在历史标的视图，整次忽略
//   3) 换标的二次确认：出现比当前标的更新的日期先挂起，连续两次见到才归档切换，防单次异常误触发；
//      页面的单日/子集查询（点选某天/某几天查看）日期都在标的内，被规则2天然兼容
let pendingInit = null;    // {days:Set} 初始化待确认的日期集
let pendingSwitch = null;  // {days:Set} 换标的待确认的日期集

const dayOf = w => String(w.id).slice(0, 10);

// 历史场次捕获：页面查看"历史交易窗口"时，被防污染过滤掉的场次数据先缓存于此（纯被动，数据来自页面自己的响应）。
// 看板会提示"是否保存为场次"，保存后写入 data/场次_<日期范围>.json，可随时查看/导出。
// 同一场次重复到达时刷新覆盖；最多暂存 5 个待处理场次（最旧的丢弃），防止长期停留在历史页导致内存膨胀。
function catchHistory(histWindows, upd, wallTs) {
  const days = [...new Set(histWindows.map(dayOf))].sort();
  const key = 'D' + days[0].replace(/-/g, '') + (days.length > 1 ? '-' + days[days.length - 1].replace(/-/g, '') : '');
  const hc = state.meta.historyCatch;
  hc[key] = { key, days, windows: histWindows, newTrades: upd.newTrades || null, ts: new Date(wallTs * 1000).toISOString() };
  const keys = Object.keys(hc);
  if (keys.length > 5) {
    keys.sort((a, b) => (hc[a].ts < hc[b].ts ? -1 : 1));
    delete hc[keys[0]];
  }
}

function mergeUpdate(upd, wallTs) {
  // upd: {windows:[{id,label,price,volume,extras}], book?, newTrades?}
  if (!upd || !Array.isArray(upd.windows) || !upd.windows.length) return;
  const respDays = new Set(upd.windows.map(dayOf));

  if (!state.windows.length && !MODE_DEMO) {
    if (!pendingInit) { pendingInit = { days: respDays }; return; }
    const keep = new Set([...respDays].filter(d => pendingInit.days.has(d)));
    if (!keep.size) { pendingInit = { days: respDays }; return; }   // 两次日期全不同，以本次重新挂起
    upd = { ...upd, windows: upd.windows.filter(w => keep.has(dayOf(w))) };
    pendingInit = null;
    mergeWindows(upd, wallTs);
    return;
  }

  if (state.windows.length) {
    const oldDays = new Set(state.windows.map(dayOf));
    // 日期是字符串，最大值用字符串比较（Math.max 对日期串得 NaN）
    const maxOld = [...oldDays].sort().pop();
    const newerDays = [...respDays].filter(d => !oldDays.has(d) && d > maxOld);
    if (newerDays.length) {
      if (!pendingSwitch) { pendingSwitch = { days: new Set(newerDays) }; return; }
      const hit = newerDays.some(d => pendingSwitch.days.has(d));
      pendingSwitch = null;
      if (hit) archiveState('捕获到新标的窗口（连续两次确认）');
      else { pendingSwitch = { days: new Set(newerDays) }; return; }
    } else {
      pendingSwitch = null;
      // 只合并当前标的日期内的窗口（挡住查看历史交易窗口时混入的更旧日期）
      const hist = upd.windows.filter(w => !oldDays.has(dayOf(w)));
      // 历史场次捕获：接口返回的通常恰好是一个完整场次，且可能与当前标的共享日期
      // （实证：查 9-10 场次时响应含 9+10 两日，而 10 日与当前 10-11 标的共享，若只按
      //  "当前标的之外"切分会丢掉共享日）——因此用整个响应作为场次数据；主看板合并不受影响
      if (hist.length) catchHistory(upd.windows, upd, wallTs);
      upd = { ...upd, windows: upd.windows.filter(w => oldDays.has(dayOf(w))) };
      if (!upd.windows.length) return;
    }
  }
  mergeWindows(upd, wallTs);
}

function mergeWindows(upd, wallTs) {
  const byId = new Map(state.windows.map(w => [w.id, w]));
  for (const w of upd.windows) {
    if (w == null || w.price == null) continue;
    let rec = byId.get(w.id);
    if (!rec) {
      rec = { id: w.id, label: w.label || w.id, price: null, prevPrice: null, firstPrice: w.price, volume: null, history: [], extras: null, lastTs: null,
        lastChangeFrom: null, lastChangeTs: null };
      state.windows.push(rec); byId.set(w.id, rec);
    }
    const ts = new Date(wallTs * 1000).toISOString();
    const priceChanged = rec.price == null || Math.abs(w.price - rec.price) > 1e-9;
    if (priceChanged) {
      // 记录本次变动：变动前价格 + 时间 → 看板按"10分钟内保持彩色、之后退灰"展示
      rec.lastChangeFrom = rec.price;
      rec.lastChangeTs = ts;
      rec.history.push({ ts, price: w.price, volume: w.volume ?? rec.volume });
    }
    rec.prevPrice = rec.price;   // 上一次捕获的价格（兼容字段）
    rec.price = w.price;
    if (w.volume != null) rec.volume = w.volume;
    if (w.extras) rec.extras = w.extras;
    rec.lastTs = ts;
    if (w.label && rec.label !== w.label) rec.label = w.label;
  }
  // 选中窗口盘口 + 最新成交明细（附加展示）
  if (upd.book) state.meta.book = { ...upd.book, ts: new Date(wallTs * 1000).toISOString() };
  if (upd.newTrades) state.meta.newTrades = upd.newTrades;
  // 保持窗口顺序稳定（按 id 排序：日期+小时）
  state.windows.sort((a, b) => String(a.id).localeCompare(String(b.id), 'zh-CN', { numeric: true }));
}

// 页面级 CDP 连接（单例复用；用于在平台页面上下文内发起按需查询）
let _pageCdp = null;
async function getPageCdp() {
  if (_pageCdp && _pageCdp.ws.readyState === 1) return _pageCdp;
  const tabs = await cdpFetch('/json');
  // 优先撮合页（用户可能手动开了门户/登录等多个平台标签）
  const tab = tabs.find(t => t.url && t.url.includes('priceCompetition'))
    || tabs.find(t => t.url && t.url.includes('NEWTRADE'));
  if (!tab) throw new Error('未找到平台交易页标签页（调试Chrome里撮合页是否还开着？）');
  const ws = new WebSocket(fixWsUrl(tab.webSocketDebuggerUrl));
  await new Promise((res, rej) => { ws.addEventListener('open', res); ws.addEventListener('error', () => rej(new Error('页面ws连接失败'))); });
  _pageCdp = new Cdp(ws);
  return _pageCdp;
}

// 盘口按需查询：仅在看板用户点击时发起（复用页面自己的查询接口与请求体，仅换code）
// 防频控：请求间隔≥2s；同code 10s 内走缓存；平台页当前窗口的被动数据直接复用（0请求）
const bookCache = new Map(); // code -> {ts, buys, sells}
let _bookQueue = Promise.resolve(), _lastBookFetch = 0;
async function getBook(code, force) {
  // 1) 平台页当前选中窗口且数据新鲜 → 直接用被动数据，零请求
  const mb = state.meta.book;
  if (!force && mb && mb.code === code && mb.ts && Date.now() - new Date(mb.ts) < 15000) {
    return { buys: mb.buys, sells: mb.sells, ts: mb.ts, source: '平台页联动' };
  }
  // 2) 缓存（10s）
  const c = bookCache.get(code);
  if (!force && c && Date.now() - c.ts < 10000) return { ...c, source: '缓存' };
  // 3) 串行 + ≥2s 间隔地发起一次页面上下文查询（带页面同款凭证头：Authorization等，仅内存使用）
  const task = async () => {
    if (!lastQuery.url || !lastQuery.template) throw new Error('尚未捕获到页面查询模板（等页面刷新一次）');
    const wait = 2000 - (Date.now() - _lastBookFetch);
    if (wait > 0) await sleep(wait);
    _lastBookFetch = Date.now();
    // 复用页面请求头：剔除浏览器禁设/自动管理的头，保留 Authorization 等平台凭证头
    const hdrs = {};
    for (const [k, v] of Object.entries(lastQuery.headers || {})) {
      if (/^(host|content-length|cookie|connection|accept-encoding|referer|origin|user-agent|sec-)/i.test(k)) continue;
      hdrs[k] = v;
    }
    if (!hdrs['Content-Type']) hdrs['Content-Type'] = 'application/json;charset=UTF-8';
    const body = JSON.stringify({ ...lastQuery.template, code });
    const pc = await getPageCdp();
    const expr = `fetch(${JSON.stringify(lastQuery.url)}, {method:'POST', credentials:'include', headers:${JSON.stringify(hdrs)}, body:${JSON.stringify(body)}}).then(r => r.text()).catch(e => 'ERR:' + e.message)`;
    const r = await pc.cmd('Runtime.evaluate', { expression: expr, awaitPromise: true, returnByValue: true });
    const txt = r.result?.value;
    if (typeof txt !== 'string' || txt.startsWith('ERR:')) throw new Error(String(txt).slice(0, 120));
    const j = JSON.parse(txt);
    if (!j || j.code !== 0 || !j.right) {
      throw new Error('平台查询被拒绝：' + (j && j.msg ? j.msg : '未知原因') + '（若交易已结束属正常现象；若提示登录/会话相关请在调试Chrome重新登录）');
    }
    const data = j.data || {};
    const buys = [], sells = [];
    for (const x of (data.buy5AndSell5 || [])) {
      const price = parseFloat(x.PRICE), qty = parseFloat(x.DUMPENERGY);
      if (!isFinite(price) || !isFinite(qty) || !price || !qty) continue;
      (/供/.test(String(x.type)) ? sells : buys).push({ price, qty });
    }
    buys.sort((a, b2) => b2.price - a.price); sells.sort((a, b2) => b2.price - a.price);
    const rec = { ts: new Date().toISOString(), buys, sells };
    bookCache.set(code, rec);
    state.meta.activeRequests++; // 每次点击恰好1个请求（与人工在平台页点窗口等价）
    return { ...rec, source: '按需查询' };
  };
  const run = _bookQueue.then(task, task); // 排队，前一个失败不阻塞
  _bookQueue = run.catch(() => {});
  return run;
}

// 兜底轮询：直连平台页面的页面级 CDP，在页面上下文内 fetch 页面自身的查询接口
// （请求与页面自身流量同源、同会话、同头；分钟级频率 + 抖动 + 失败退避）
async function startPoller(getCdp, poll) {
  const baseMs = Math.max(10, poll.intervalSec || 90) * 1000; // 硬性下限10秒，防误配
  let failures = 0;
  let pageCdp = null;
  state.meta.mode = 'passive+poll';
  console.log(`兜底轮询已启用：每 ~${baseMs / 1000}s（±25%抖动，失败退避，下限10s）`);

  const ensurePageCdp = async () => {
    if (pageCdp && pageCdp.ws.readyState === 1) return pageCdp;
    const tabs = await (await fetch(CDP_HTTP + '/json')).json();
    const tab = tabs.find(t => t.url && t.url.includes(HOST));
    if (!tab) throw new Error('未找到平台标签页');
    const ws = new WebSocket(tab.webSocketDebuggerUrl);
    await new Promise((res, rej) => { ws.addEventListener('open', res); ws.addEventListener('error', () => rej(new Error('页面ws连接失败'))); });
    pageCdp = new Cdp(ws);
    return pageCdp;
  };

  for (;;) {
    await sleep(baseMs * (0.75 + Math.random() * 0.5) * Math.pow(2, Math.min(failures, 4)));
    if (!getCdp() || getCdp().ws.readyState > 2) continue; // 主连接断了先跳过本轮
    try {
      const pc = await ensurePageCdp();
      const expr = `fetch(${JSON.stringify(poll.url)}, {method:${JSON.stringify(poll.method || 'POST')}, credentials:'include'` +
        (poll.body ? `, headers:{'Content-Type':'application/json'}, body:${JSON.stringify(poll.body)}` : '') +
        `}).then(r => r.text()).catch(e => 'POLL_ERR:' + e.message)`;
      const r = await pc.cmd('Runtime.evaluate', { expression: expr, awaitPromise: true, returnByValue: true });
      const txt = r.result?.value;
      if (typeof txt !== 'string' || txt.startsWith('POLL_ERR')) throw new Error(String(txt).slice(0, 100));
      state.meta.activeRequests++;
      onPayload({ key: 'POLL ' + new URL(poll.url).pathname, bodyStr: txt, wallTime: Date.now() / 1000, status: 200 });
      failures = 0;
    } catch (e) {
      failures++;
      console.error(`[poll] 失败(${failures})：${e.message}`);
      if (failures >= 5) {
        state.meta.note = '轮询连续失败已暂停，请检查平台登录状态后重启监控';
        console.error(state.meta.note);
        return;
      }
    }
  }
}

// ---------------- 预期满足监控：重点窗口巡检 ----------------
// 按用户设定的分钟级间隔，查询选定窗口（≤12个）的买卖五档。全部走 getBook：
// 页面当前窗口15s内直接复用（0请求）、10s缓存、串行≥2s节流、页面上下文执行（与人工点选等价）。
// 防线：间隔硬下限1分钟、每小时请求预算硬顶（超了自动暂停）、连续5次失败暂停。
const PATROL_BUDGET_PER_HOUR = 120;
const patrolBudget = [];   // 巡检实际发出的请求时间戳（预算统计；getBook 内部的被动复用/缓存不计）
async function patrolLoop() {
  for (;;) {
    const cfg = state.meta.patrol;
    if (!cfg || !cfg.enabled || !cfg.codes.length) { await sleep(3000); continue; }
    const intervalMs = Math.max(1, cfg.intervalMin || 5) * 60e3;
    const now = Date.now();
    while (patrolBudget.length && now - patrolBudget[0] > 3600e3) patrolBudget.shift();
    if (patrolBudget.length >= PATROL_BUDGET_PER_HOUR) {
      cfg.pausedReason = `已达每小时 ${PATROL_BUDGET_PER_HOUR} 次请求预算，本轮暂停（一小时窗口滚动恢复）`;
      schedulePush();
      await sleep(60e3);
      continue;
    }
    cfg.pausedReason = '';
    for (const code of cfg.codes) {
      if (!cfg.enabled) break;   // 巡检中途被关闭
      try {
        const book = await getBook(code, false);
        if (book.source === '按需查询') patrolBudget.push(Date.now());
        cfg.results[code] = { buys: book.buys, sells: book.sells, ts: new Date().toISOString() };
        cfg.failures = 0;
      } catch (e) {
        cfg.failures = (cfg.failures || 0) + 1;
        if (cfg.failures >= 5) {
          cfg.pausedReason = '巡检连续失败已暂停：' + e.message + '（撮合结束属正常；若交易中请检查调试浏览器登录）';
          break;
        }
      }
    }
    cfg.lastRoundTs = new Date().toISOString();
    schedulePush();
    scheduleFlush();
    await sleep(intervalMs);
  }
}



// ---------------- 集中出清电量精算（事后，从快照留痕提取） ----------------
// 接口响应无"集中出清电量"字段（totalBidEnergy 为集中+滚动累计），而集中出清价的加权
// 应使用集中阶段的电量。快照留痕保存了每次响应（内容变化即落盘），故集中阶段电量可
// 事后精算：每窗口取"滚动开始"时刻之前最后一次 totalBidEnergy。
// 滚动开始时刻取自快照中 findJysbysSeqInfo 的 stagetime（缺省按 09:30 北京时）。
import readline from 'node:readline';

const CEN_DEFAULT_ROLL = 9.5 * 3600;   // 滚动开始缺省值：北京时间 09:30（秒）
async function computeCenEnergy(codes, days) {
  const want = new Set(codes);
  const out = {};
  for (const day of days) {
    const f = path.join(DATA, `snapshots_${day}.jsonl`);
    if (!fs.existsSync(f)) continue;
    let rollStart = CEN_DEFAULT_ROLL;
    const lastE = new Map();   // code -> 滚动开始前最后累计量
    const rl = readline.createInterface({ input: fs.createReadStream(f), crlfDelay: Infinity });
    for await (const line of rl) {
      let rec; try { rec = JSON.parse(line); } catch { continue; }
      if (!rec.body || !rec.ts) continue;
      const d = new Date(rec.ts);
      const bjSec = ((d.getTime() / 1000 + 8 * 3600) % 86400 + 86400) % 86400;   // 北京时刻的当日秒数
      if (rec.key.includes('findJysbysSeqInfo')) {
        try {
          const st = JSON.parse(rec.body).data.stagetime || [];
          const r = st.find(x => x.name === '滚动开始' && /^\d{2}:\d{2}$/.test(x.time || ''));
          if (r) rollStart = (+r.time.slice(0, 2)) * 3600 + (+r.time.slice(3, 5)) * 60;
        } catch {}
        continue;
      }
      if (!rec.key.includes('findBuy5AndSell5') || bjSec >= rollStart) continue;
      try {
        const arr = JSON.parse(rec.body).data.findMarketCountInfo || [];
        for (const r of arr) {
          const code = String(r.jydm || '');
          if (!want.has(code)) continue;
          const e = parseFloat(r.totalBidEnergy);
          if (isFinite(e) && e > 0) lastE.set(code, e);
        }
      } catch {}
    }
    for (const [code, e] of lastE) if (out[code] == null || e > out[code]) out[code] = e;
  }
  return out;
}

// 精算缓存：同参数 10 分钟内不重扫（快照文件大，流式扫描秒级）
let cenCache = { key: '', at: 0, data: {} };

// 用系统默认浏览器打开看板（--no-open 可关闭）
function openBrowser(url) {
  try {
    const cmd = process.platform === 'win32' ? ['cmd', ['/c', 'start', '', url]]
      : process.platform === 'darwin' ? ['open', [url]] : ['xdg-open', [url]];
    spawn(cmd[0], cmd[1], { detached: true, stdio: 'ignore' }).unref();
  } catch {}
}

// ---------------- 实时推送（SSE）：平台页面刷新 → +0.5s 推给看板 ----------------

const sseClients = new Set();

// 数据到达后 0.5s 广播一次（0.5s 内的连续到达合并为一次，避免突发刷屏）
let pushTimer = null;
function schedulePush() {
  if (pushTimer) return;
  pushTimer = setTimeout(() => {
    pushTimer = null;
    if (!sseClients.size) return;
    const payload = `data: ${JSON.stringify(state)}\n\n`;
    for (const res of sseClients) { try { res.write(payload); } catch {} }
  }, 500);
}

// ---------------- 预期价格模板文件读取（.xlsx 零依赖解析） ----------------
// xlsx = ZIP(内含XML)。仅用 Node 内置 zlib 解压，解析 sharedStrings + 第一个工作表，
// 输出 TSV 文本交给看板既有的行解析（零第三方依赖，收方机器无需安装任何库）。

function zipEntries(buf) {
  const map = new Map();
  // 定位 EOCD（从尾部扫描签名 0x06054b50）
  let eocd = -1;
  for (let i = buf.length - 22; i >= 0 && i > buf.length - 22 - 65536; i--) {
    if (buf.readUInt32LE(i) === 0x06054b50) { eocd = i; break; }
  }
  if (eocd < 0) throw new Error('xlsx格式错误(无EOCD)');
  const n = buf.readUInt16LE(eocd + 10);
  let p = buf.readUInt32LE(eocd + 16);
  for (let i = 0; i < n; i++) {
    if (buf.readUInt32LE(p) !== 0x02014b50) break;
    const method = buf.readUInt16LE(p + 10);
    const compSize = buf.readUInt32LE(p + 20);
    const nameLen = buf.readUInt16LE(p + 28), extraLen = buf.readUInt16LE(p + 30);
    const localOff = buf.readUInt32LE(p + 42);
    const name = buf.toString('utf8', p + 46, p + 46 + nameLen);
    if (!name.endsWith('/')) {
      // 本地头再取一次 name/extra 长度（可能与中央目录不同）
      const lnLen = buf.readUInt16LE(localOff + 26), lxLen = buf.readUInt16LE(localOff + 28);
      const dataStart = localOff + 30 + lnLen + lxLen;
      const raw = buf.subarray(dataStart, dataStart + compSize);
      map.set(name, method === 8 ? zlib.inflateRawSync(raw) : Buffer.from(raw));
    }
    p += 46 + nameLen + extraLen;
  }
  return map;
}

const xmlDecode = s => s
  .replace(/&#x([0-9a-fA-F]+);/g, (_, h) => String.fromCodePoint(parseInt(h, 16)))
  .replace(/&#(\d+);/g, (_, d) => String.fromCodePoint(+d))
  .replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&apos;/g, "'").replace(/&amp;/g, '&');

const colToIdx = letters => letters.split('').reduce((a, ch) => a * 26 + (ch.charCodeAt(0) - 64), 0) - 1;

function xlsxToTsv(buf) {
  const files = zipEntries(buf);
  const ssXml = files.get('xl/sharedStrings.xml');
  const shared = [];
  if (ssXml) {
    for (const si of ssXml.toString('utf8').matchAll(/<si>([\s\S]*?)<\/si>/g)) {
      shared.push([...si[1].matchAll(/<t[^>]*>([\s\S]*?)<\/t>/g)].map(t => t[1]).join(''));
    }
  }
  const sheetName = [...files.keys()].filter(k => /^xl\/worksheets\/sheet\d+\.xml$/.test(k)).sort()[0];
  if (!sheetName) throw new Error('xlsx中未找到工作表');
  const rows = [];
  for (const rm of sheetName && files.get(sheetName).toString('utf8').matchAll(/<row[^>]*>([\s\S]*?)<\/row>/g)) {
    const cells = [];
    for (const cm of rm[1].matchAll(/<c ([^>]*?)(?:\/>|>([\s\S]*?)<\/c>)/g)) {
      const attrs = cm[1], inner = cm[2] || '';
      const rM = attrs.match(/r="([A-Z]+)\d+"/), tM = attrs.match(/t="(\w+)"/);
      if (!rM) continue;
      let v = '';
      const vm = inner.match(/<v>([\s\S]*?)<\/v>/);
      if (tM && tM[1] === 's' && vm) v = shared[+vm[1]] ?? '';
      else if (tM && tM[1] === 'inlineStr') { const tm = inner.match(/<t[^>]*>([\s\S]*?)<\/t>/); v = tm ? tm[1] : ''; }
      else if (vm) v = vm[1];
      cells[colToIdx(rM[1])] = xmlDecode(v);
    }
    let last = -1;
    for (let i = 0; i < cells.length; i++) if (cells[i] !== undefined) last = i;
    rows.push(Array.from({ length: last + 1 }, (_, i) => cells[i] ?? '').join('\t'));
  }
  return rows.join('\n');
}

function startServer() {
  const srv = http.createServer(async (req, res) => {
    const url = req.url.split('?')[0];
    // 静态文件读取容错：文件被编辑器/同步软件瞬时替换时不能让进程崩溃
    const safeRead = p => { try { return fs.readFileSync(p); } catch (e) { return null; } };
    if (url === '/' || url === '/index.html') {
      const buf = safeRead(path.join(ROOT, 'dashboard.html'));
      if (!buf) { res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('dashboard.html 暂时不可读（可能正被编辑器保存/同步），稍后刷新即可'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(buf);
    } else if (url === '/echarts.min.js') {
      const buf = safeRead(path.join(ROOT, 'lib', 'echarts.min.js'));
      if (!buf) { res.writeHead(500); res.end('echarts.min.js unavailable'); return; }
      res.writeHead(200, { 'Content-Type': 'application/javascript; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(buf);
    } else if (url === '/api/state') {
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(JSON.stringify(state));
    } else if (url === '/api/book') {
      const u2 = new URL(req.url, 'http://127.0.0.1');
      const code = u2.searchParams.get('code') || '';
      const force = u2.searchParams.get('fresh') === '1';
      try {
        if (!/^D\d{8}_H\d{2}$/.test(code)) throw new Error('code格式不合法');
        const book = await getBook(code, force);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-cache' });
        res.end(JSON.stringify({ ok: true, code, ...book }));
      } catch (e) {
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-cache' });
        res.end(JSON.stringify({ ok: false, err: e.message }));
      }
    } else if (url === '/api/expect') {
      // 预期价格模板（优先 Excel：预期价格模板.xlsx；兜底 txt），看板自动读取
      try {
        const xbuf = safeRead(path.join(ROOT, '预期价格模板.xlsx'));
        if (xbuf) {
          res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-cache' });
          res.end(JSON.stringify({ ok: true, src: 'xlsx', text: xlsxToTsv(xbuf) }));
          return;
        }
      } catch (e) {
        console.error('[expect] xlsx解析失败，尝试txt兜底:', e.message);
      }
      const buf = safeRead(path.join(ROOT, '预期价格模板.txt'));
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(JSON.stringify({ ok: !!buf, src: buf ? 'txt' : '', text: buf ? buf.toString('utf8') : '' }));
    } else if (url === '/api/patrol' && req.method === 'POST') {
      // 预期满足监控设置：{enabled?, codes?, intervalMin?}；codes 为窗口代码（Dyyyymmdd_Hh），≤12个
      let body = '';
      req.on('data', c => { body += c; });
      req.on('end', () => {
        try {
          const j = JSON.parse(body || '{}');
          const cfg = state.meta.patrol;
          if (typeof j.enabled === 'boolean') cfg.enabled = j.enabled;
          if (Array.isArray(j.codes)) cfg.codes = [...new Set(j.codes.filter(c => /^D\d{8}_H\d{2}$/.test(c)))].slice(0, 12);
          if (j.intervalMin != null) cfg.intervalMin = Math.min(60, Math.max(1, Math.round(+j.intervalMin || 5) || 1));
          if (!cfg.enabled) cfg.pausedReason = '';
          cfg.failures = 0;
          scheduleFlush(); schedulePush();
          res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-cache' });
          res.end(JSON.stringify({ ok: true, patrol: cfg }));
        } catch (e) {
          res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({ ok: false, err: e.message }));
        }
      });
    } else if (url === '/api/history' && req.method === 'POST') {
      // 历史场次：{action:'save', key, days, windows, newTrades?} 保存场次文件；{action:'dismiss', key} 忽略待存场次
      let body = '';
      req.on('data', c => { body += c; });
      req.on('end', () => {
        const send = j => { res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-cache' }); res.end(JSON.stringify(j)); };
        try {
          const j = JSON.parse(body || '{}');
          if (j.action === 'save') {
            if (!/^D\d{8}(-\d{8})?$/.test(String(j.key || ''))) throw new Error('场次名格式不合法');
            if (!Array.isArray(j.windows) || !j.windows.length) throw new Error('场次数据为空');
            const rec = { key: j.key, days: j.days || [], savedAt: new Date().toISOString(), windowCount: j.windows.length, windows: j.windows, newTrades: j.newTrades || null };
            const f = path.join(DATA, `场次_${j.key}.json`);
            fs.writeFileSync(f, JSON.stringify(rec));
            console.log(`📁 已保存历史场次 → ${path.basename(f)}（${rec.windowCount} 窗口）`);
            delete state.meta.historyCatch[j.key];
            scheduleFlush(); schedulePush();
            send({ ok: true, file: path.basename(f) });
          } else if (j.action === 'dismiss') {
            if (state.meta.historyCatch[j.key]) { delete state.meta.historyCatch[j.key]; schedulePush(); }
            send({ ok: true });
          } else throw new Error('未知操作');
        } catch (e) { send({ ok: false, err: e.message }); }
      });
    } else if (url === '/api/cenEnergy') {
      // 集中出清电量精算：?codes=D..,D..&days=20261010（缺省今天）；从快照留痕提取各窗口
      // "滚动开始"时刻前的累计量（即集中阶段电量），供汇总的集中出清加权使用
      const u2 = new URL(req.url, 'http://127.0.0.1');
      const codes = (u2.searchParams.get('codes') || '').split(',').filter(c => /^D\d{8}_H\d{2}$/.test(c)).slice(0, 200);
      const days = (u2.searchParams.get('days') || '').split(',').filter(d => /^\d{8}$/.test(d)).slice(0, 7);
      if (!days.length) days.push(new Date(Date.now() + 8 * 3600e3).toISOString().slice(0, 10).replace(/-/g, ''));
      const jh = { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-cache' };
      const key = days.join() + '|' + codes.join();
      if (cenCache.key === key && Date.now() - cenCache.at < 10 * 60e3) {
        res.writeHead(200, jh); res.end(JSON.stringify({ ok: true, cen: cenCache.data, cached: true }));
      } else {
        try {
          const cen = await computeCenEnergy(codes, days);
          cenCache = { key, at: Date.now(), data: cen };
          res.writeHead(200, jh); res.end(JSON.stringify({ ok: true, cen }));
        } catch (e) {
          res.writeHead(200, jh); res.end(JSON.stringify({ ok: false, err: e.message }));
        }
      }
    } else if (url === '/api/history/list') {
      // 已保存场次列表：data/场次_*.json
      const list = [];
      try {
        for (const f of fs.readdirSync(DATA)) {
          if (!/^场次_D\d{8}(-\d{8})?\.json$/.test(f)) continue;
          try {
            const j = JSON.parse(fs.readFileSync(path.join(DATA, f), 'utf8'));
            list.push({ file: f, key: j.key, days: j.days, savedAt: j.savedAt, windowCount: j.windowCount });
          } catch {}
        }
      } catch {}
      list.sort((a, b) => (a.savedAt < b.savedAt ? 1 : -1));
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(JSON.stringify({ ok: true, list }));
    } else if (url === '/api/history/get') {
      const u2 = new URL(req.url, 'http://127.0.0.1');
      const f = u2.searchParams.get('file') || '';
      if (!/^场次_D\d{8}(-\d{8})?\.json$/.test(f)) {
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: false, err: '文件名不合法' }));
      } else {
        try {
          const j = JSON.parse(fs.readFileSync(path.join(DATA, f), 'utf8'));
          res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-cache' });
          res.end(JSON.stringify({ ok: true, session: j }));
        } catch (e) {
          res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({ ok: false, err: '读取失败：' + e.message }));
        }
      }
    } else if (url === '/api/stream') {
      res.writeHead(200, {
        'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive',
      });
      res.write('retry: 3000\n\n');
      sseClients.add(res);
      const ping = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 15000);
      req.on('close', () => { clearInterval(ping); sseClients.delete(res); });
    } else {
      res.writeHead(404); res.end('not found');
    }
  });
  srv.on('error', async e => {
    if (e.code === 'EADDRINUSE') {
      console.error(`\nℹ 端口 ${PORT} 已被占用——监控已经在运行了（关闭看板网页不会停止监控）。`);
      // 已运行的实例健康 → 直接帮用户打开看板（用户重启工具多半就是想再看页面）
      try {
        const r = await fetch(`http://127.0.0.1:${PORT}/api/state`, { signal: AbortSignal.timeout(2000) });
        if (r.ok) {
          console.error('已运行的监控状态正常，已直接为你打开看板页面。');
          if (!args.includes('--no-open')) openBrowser(`http://127.0.0.1:${PORT}`);
        }
      } catch {}
      console.error('如确需重启监控：先关闭旧的监控黑窗口，或任务管理器结束旧的 node.exe（监听8787的）。');
      process.exit(0);
    } else {
      console.error('[server]', e.message);
      process.exit(1);
    }
  });
  srv.listen(PORT, '127.0.0.1', () => {
    console.log(`\n✅ 看板已就绪：http://127.0.0.1:${PORT}`);
    console.log('刷新方式：平台页面自动刷新（~5.2s/次）后 +0.5 秒实时推送（SSE），完全跟随平台自身节奏');
    console.log('保持本窗口与调试 Chrome 运行。Ctrl+C 停止（数据已留痕 data/ 目录可复盘）。\n');
    if (!args.includes('--no-open')) openBrowser(`http://127.0.0.1:${PORT}`);
  });
}

function pathToFileURL(p) {
  return 'file:///' + p.replace(/\\/g, '/').replace(/#/g, '%23');
}

// ---------------- 演示模式（本地合成数据，验证看板，不连接浏览器） ----------------

async function runDemo() {
  console.log('=== 演示模式：合成48窗口数据，验证看板（不连接浏览器、不访问平台）===');
  state.meta.mode = 'demo';
  let round = 0;
  const gen = () => {
    const windows = [];
    for (let d = 0; d < 2; d++) {
      const day = d === 0 ? '2026-09-25' : '2026-09-26';
      for (let h = 0; h < 24; h++) {
        const base = 380 + 60 * Math.sin((h - 6) / 24 * 2 * Math.PI) + 40 * Math.random();
        const drift = round * (Math.random() - 0.4);
        windows.push({
          id: `${day}T${String(h).padStart(2, '0')}`,
          label: `${d + 1}日 ${String(h).padStart(2, '0')}:00-${String((h + 1) % 24).padStart(2, '0')}:00`,
          price: +(base + drift).toFixed(2),
          volume: +(200 + 300 * Math.random()).toFixed(1),
          bidQty: +(150 + 200 * Math.random()).toFixed(1),
          bidPrice: +(base - 5).toFixed(2),
          askQty: +(150 + 200 * Math.random()).toFixed(1),
          askPrice: +(base + 5).toFixed(2),
        });
      }
    }
    return { windows };
  };
  mergeUpdate(gen(), Date.now() / 1000);
  state.meta.captured++; state.meta.lastDataTs = new Date().toISOString();
  startServer();
  setInterval(() => {
    round++;
    mergeUpdate(gen(), Date.now() / 1000);
    state.meta.captured++; state.meta.lastDataTs = new Date().toISOString();
    scheduleFlush();
    console.log(`  demo 第${round}轮更新`);
  }, 6000);
}

// ---------------- 入口 ----------------

if (MODE_DEMO) {
  runDemo().catch(e => { console.error(e.message); process.exit(1); });
} else if (MODE_DISCOVER) {
  runDiscover().catch(e => { console.error(e.message); process.exit(1); });
} else {
  runMonitor().catch(e => { console.error(e.message); process.exit(1); });
}
