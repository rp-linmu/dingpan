// cdp-lib.mjs — CDP直连公共库（Chrome 9222）
export const CDP_HTTP = 'http://127.0.0.1:9222';
export const sleep = ms => new Promise(r => setTimeout(r, ms));

export function makeCmd(ws) {
  let id = 0; const pending = new Map();
  ws.addEventListener('message', ev => {
    const m = JSON.parse(ev.data);
    if (m.id && pending.has(m.id)) { const p = pending.get(m.id); pending.delete(m.id); m.error ? p.rej(new Error(m.error.message)) : p.res(m.result); }
  });
  return (method, params = {}) => new Promise((res, rej) => {
    const i = ++id; pending.set(i, { res, rej });
    ws.send(JSON.stringify({ id: i, method, params }));
  });
}

export async function browserWs() {
  const ver = await (await fetch(CDP_HTTP + '/json/version')).json();
  const ws = new WebSocket(ver.webSocketDebuggerUrl);
  await new Promise((res, rej) => { ws.onopen = res; ws.onerror = () => rej(new Error('browser-ws连接失败（Chrome调试端口未开?）')); });
  return { ws, cmd: makeCmd(ws) };
}

// 建受控标签页并导航
export async function openTab(url) {
  let newResp;
  try {
    newResp = await fetch(CDP_HTTP + '/json/new?url=about%3Ablank', { method: 'PUT' });
  } catch (e) {
    throw new Error('Chrome调试端口9222未开启或Chrome已关闭。请用调试参数启动Chrome并登录平台后重试。');
  }
  const t = await newResp.json();
  const ws = new WebSocket(t.webSocketDebuggerUrl);
  await new Promise((res, rej) => { ws.onopen = res; ws.onerror = () => rej(new Error('page-ws连接失败')); });
  const cmd = makeCmd(ws);
  await cmd('Page.enable'); await cmd('Runtime.enable');
  const evl = async (expression, timeoutMs = 60000) => {
    const p = cmd('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true });
    const r2 = await Promise.race([p, new Promise((_, rej) => setTimeout(() => rej(new Error('eval-timeout')), timeoutMs))]);
    if (r2.exceptionDetails) throw new Error('page-eval: ' + String(r2.exceptionDetails.exception?.description || r2.exceptionDetails.text).slice(0, 200));
    return r2.result?.value;
  };
  if (url) { await cmd('Page.navigate', { url }); }
  return { tabId: t.id, ws, cmd, evl, close: async () => { try { await fetch(CDP_HTTP + '/json/close/' + t.id); } catch (e) {} try { ws.close(); } catch (e) {} } };
}

// 真实鼠标点击（按选择器表达式的元素中心）
export async function realClick(tab, elExpr) {
  const rect = await tab.evl(`(function(){const el=${elExpr};if(!el)return null;const r=el.getBoundingClientRect();if(r.width===0)return null;return JSON.stringify({x:Math.round(r.x+r.width/2),y:Math.round(r.y+r.height/2)});})()`);
  if (!rect) return false;
  const { x, y } = JSON.parse(rect);
  await tab.cmd('Input.dispatchMouseEvent', { type: 'mouseMoved', x, y });
  await tab.cmd('Input.dispatchMouseEvent', { type: 'mousePressed', x, y, button: 'left', clickCount: 1 });
  await tab.cmd('Input.dispatchMouseEvent', { type: 'mouseReleased', x, y, button: 'left', clickCount: 1 });
  return true;
}

// 点击同源iframe内的元素（坐标 = iframe偏移 + 元素在iframe内的位置）
export async function realClickInFrame(tab, frameSel, elExpr) {
  const rect = await tab.evl(`(function(){
    const f=document.querySelector(${JSON.stringify(frameSel)});if(!f)return null;
    const doc=f.contentDocument;if(!doc)return null;
    const el=${elExpr};if(!el)return null;
    const fr=f.getBoundingClientRect(),r=el.getBoundingClientRect();
    if(r.width===0)return null;
    return JSON.stringify({x:Math.round(fr.x+r.x+r.width/2),y:Math.round(fr.y+r.y+r.height/2)});
  })()`);
  if (!rect) return false;
  const { x, y } = JSON.parse(rect);
  await tab.cmd('Input.dispatchMouseEvent', { type: 'mouseMoved', x, y });
  await tab.cmd('Input.dispatchMouseEvent', { type: 'mousePressed', x, y, button: 'left', clickCount: 1 });
  await tab.cmd('Input.dispatchMouseEvent', { type: 'mouseReleased', x, y, button: 'left', clickCount: 1 });
  return true;
}
