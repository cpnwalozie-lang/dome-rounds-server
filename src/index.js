// Dome Rounds game server.
// Each room gets its own Durable Object: a tiny room that players connect to over a secure WebSocket
// (wss on port 443, so it looks like normal web traffic to strict networks). One player's game is the referee;
// the server passes each player's updates on to everyone else, and says when someone joins or leaves.
//
// Two kinds of room:
//  - code rooms ("dr-XXXXX"): 1v1, free-for-all and 2v2. Up to 4 players. The creator hosts; if they leave, the room closes.
//  - Battle matches ("bt1-N"): public drop-in matches for up to 10 players. The first one in hosts. If the host
//    leaves, the player who has been there longest takes over, so the match keeps going.
//
// Plus one Hub: every open copy of the game keeps a tiny connection to it, so it knows how many people are
// online right now (on the menu, in public matches, in private rooms, playing bots), and keeps simple totals:
// players today (UK time), players all-time, and the most ever online at once. Only the owner can see them:
// /stats?key=... (and the owner's own game, opened once with ?owner=...). Only a hash of the key lives here.
//
// Uses the WebSocket Hibernation API: a room with nobody talking goes to sleep and costs nothing, and the
// "ping" keep-alives a lone player sends are answered without waking it.

const MAX_MSG = 32768;   // bytes; a game update is 1-3 KB (more when the host is also sending bots)
const MAX_CODE_ROOM = 4; // 1v1, free-for-all (up to 4), 2v2
const MAX_BATTLE = 10;   // Battle: up to 10 people (bots fill the rest on the host's screen)
const isBattle = name => /^bt\d+-/.test(name);

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const cors = { 'Access-Control-Allow-Origin': '*' };
    if (url.pathname === '/' || url.pathname === '/health') {
      return new Response(JSON.stringify({ ok: true, game: 'dome-rounds', battle: true }), { headers: { 'content-type': 'application/json', ...cors } });
    }
    if (url.pathname === '/hub' || url.pathname === '/stats' || url.pathname === '/stats.json') {
      if (url.pathname === '/hub' && req.headers.get('Upgrade') !== 'websocket') return new Response('expected a websocket', { status: 426, headers: cors });
      return env.HUB.get(env.HUB.idFromName('hub')).fetch(req);
    }
    const m = url.pathname.match(/^\/room\/([a-z0-9-]{1,48})$/);
    if (!m) return new Response('not found', { status: 404, headers: cors });
    if (req.headers.get('Upgrade') !== 'websocket') return new Response('expected a websocket', { status: 426, headers: cors });
    const stub = env.ROOMS.get(env.ROOMS.idFromName(m[1]));
    return stub.fetch(req);
  },
};

export class Room {
  constructor(state, env) {
    this.state = state;
    // keep-alive pings get their pong straight from the runtime, without waking the room
    try { state.setWebSocketAutoResponse(new WebSocketRequestResponsePair('ping', 'pong')); } catch (e) {}
  }

  // everyone still connected, with who they are (kept on the socket so it survives the room sleeping)
  live(except) {
    const out = [];
    for (const ws of this.state.getWebSockets()) {
      if (ws === except) continue;
      let a = null; try { a = ws.deserializeAttachment(); } catch (e) {}
      if (!a || !a.peer || a.gone) continue;
      out.push({ ws, a });
    }
    return out;
  }

  async fetch(req) {
    const url = new URL(req.url);
    const name = decodeURIComponent(url.pathname.split('/').pop() || '');
    const battle = isBattle(name);
    let role = url.searchParams.get('role');
    const peer = (url.searchParams.get('peer') || '').replace(/[^a-z0-9]/gi, '').slice(0, 24) || Math.random().toString(36).slice(2, 12);
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    const list = this.live();
    const host = list.find(s => s.a.role === 'host');
    if (role === 'any') role = battle ? (host ? 'guest' : 'host') : 'guest';
    else role = role === 'host' ? 'host' : 'guest';
    let err = null;
    if (role === 'host' && host) err = 'code_taken';
    else if (role === 'guest' && !host) err = 'no_room';
    else if (list.length >= (battle ? MAX_BATTLE : MAX_CODE_ROOM)) err = 'full';
    if (err) {
      server.accept();
      try { server.send(JSON.stringify({ t: 'err', code: err })); server.close(1000, err); } catch (e) {}
      return new Response(null, { status: 101, webSocket: client });
    }

    this.state.acceptWebSocket(server);
    server.serializeAttachment({ peer, role, battle, at: Date.now() });
    server.send(JSON.stringify({ t: 'hello', me: peer, role, host: role === 'host' ? peer : host.a.peer, peers: list.map(s => s.a.peer) }));
    const msg = JSON.stringify({ t: 'joined', f: peer });
    for (const s of list) { try { s.ws.send(msg); } catch (e) {} }
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, d) {
    if (typeof d !== 'string' || d.length > MAX_MSG) return;
    if (d === 'ping') { try { ws.send('pong'); } catch (e) {} return; }
    let a = null; try { a = ws.deserializeAttachment(); } catch (e) {}
    if (!a || !a.peer) return;
    // pass the player's latest state straight on, tagged with who sent it (no parsing needed)
    const out = '{"t":"p","f":"' + a.peer + '","d":' + d + '}';
    for (const s of this.live(ws)) { try { s.ws.send(out); } catch (e) {} }
  }

  async webSocketClose(ws) { this.gone(ws); }
  async webSocketError(ws) { this.gone(ws); }

  gone(ws) {
    let a = null; try { a = ws.deserializeAttachment(); } catch (e) {}
    if (!a || !a.peer || a.gone) return;
    try { ws.serializeAttachment({ ...a, gone: true }); } catch (e) {}
    try { ws.close(1000, 'bye'); } catch (e) {}
    const rest = this.live(ws);
    const left = JSON.stringify({ t: 'left', f: a.peer });
    for (const s of rest) { try { s.ws.send(left); } catch (e) {} }
    if (a.role !== 'host' || !rest.length) return;
    if (a.battle) {
      // hand the referee job to whoever has been in the match longest
      rest.sort((x, y) => x.a.at - y.a.at);
      const next = rest[0];
      try { next.ws.serializeAttachment({ ...next.a, role: 'host' }); } catch (e) {}
      const hm = JSON.stringify({ t: 'host', f: next.a.peer });
      for (const s of rest) { try { s.ws.send(hm); } catch (e) {} }
    } else {
      // code rooms close when their creator leaves
      for (const s of rest) { try { s.ws.close(1000, 'host_left'); } catch (e) {} }
    }
  }
}

// ---------- the Hub: who's online, and simple totals ----------
const WHERE = ['menu', 'battle', 'private', 'bots', 'away'];
const OWNER_HASH = 'e9bc1609da16388d65a1dd1dcacef8280a58faed212ccd79194c68f480fd0f00'; // sha-256 of the owner's key
async function isOwner(k) {
  if (!k || k.length > 64) return false;
  const h = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(k));
  return [...new Uint8Array(h)].map(b => b.toString(16).padStart(2, '0')).join('') === OWNER_HASH;
}
const ukDay = () => new Intl.DateTimeFormat('en-GB', { timeZone: 'Europe/London', year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date());

export class Hub {
  constructor(state, env) {
    this.state = state;
    try { state.setWebSocketAutoResponse(new WebSocketRequestResponsePair('ping', 'pong')); } catch (e) {}
  }

  counts(except) {
    const c = { menu: 0, battle: 0, private: 0, bots: 0, away: 0 };
    for (const ws of this.state.getWebSockets()) {
      if (ws === except) continue;
      let a = null; try { a = ws.deserializeAttachment(); } catch (e) {}
      if (!a || a.gone) continue;
      c[WHERE.includes(a.w) ? a.w : 'menu']++;
    }
    c.online = c.menu + c.battle + c.private + c.bots; // people with the game in front of them
    c.open = c.online + c.away;                           // ...plus anyone with it in a background tab
    return c;
  }

  async totals() {
    const g = await this.state.storage.get(['day', 'dayN', 'allN', 'peak', 'peakAt']);
    const today = ukDay();
    return { day: today, today: g.get('day') === today ? (g.get('dayN') || 0) : 0, allTime: g.get('allN') || 0, peak: g.get('peak') || 0, peakAt: g.get('peakAt') || null };
  }

  // keep the record up to date, and send the live numbers to the owner only
  async tell(except) {
    const c = this.counts(except);
    const peak = (await this.state.storage.get('peak')) || 0;
    if (c.online > peak) await this.state.storage.put({ peak: c.online, peakAt: new Date().toISOString() });
    const msg = JSON.stringify({ t: 'n', online: c.online, battle: c.battle, private: c.private, bots: c.bots, menu: c.menu });
    for (const ws of this.state.getWebSockets()) {
      if (ws === except) continue;
      let a = null; try { a = ws.deserializeAttachment(); } catch (e) {}
      if (!a || a.gone || !a.owner) continue;
      try { ws.send(msg); } catch (e) {}
    }
  }

  // one key per device: the last UK day it played. New device = a new player all-time; first visit today = a player today.
  async seen(id) {
    const today = ukDay(), key = 'u:' + id;
    const g = await this.state.storage.get([key, 'day', 'dayN', 'allN']);
    const last = g.get(key);
    if (last === today) return;
    const put = { [key]: today };
    let dayN = g.get('day') === today ? (g.get('dayN') || 0) : 0;
    put.day = today; put.dayN = dayN + 1;
    if (!last) put.allN = (g.get('allN') || 0) + 1;
    await this.state.storage.put(put);
  }

  async fetch(req) {
    const url = new URL(req.url);
    const cors = { 'Access-Control-Allow-Origin': '*' };
    if (url.pathname === '/stats.json' || url.pathname === '/stats') {
      if (!(await isOwner(url.searchParams.get('key')))) return new Response('not found', { status: 404 });
      const c = this.counts(), t = await this.totals();
      const data = { online: c.online, publicMatches: c.battle, privateRooms: c.private, vsBots: c.bots, onMenu: c.menu, backgroundTabs: c.away, playersToday: t.today, playersAllTime: t.allTime, mostOnlineAtOnce: t.peak, mostOnlineWhen: t.peakAt, ukDate: t.day };
      if (url.pathname === '/stats.json') return new Response(JSON.stringify(data), { headers: { 'content-type': 'application/json', 'cache-control': 'no-store', ...cors } });
      return new Response(statsPage(data), { headers: { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' } });
    }
    const id = (url.searchParams.get('d') || '').replace(/[^a-z0-9]/gi, '').slice(0, 24);
    const w = WHERE.includes(url.searchParams.get('w')) ? url.searchParams.get('w') : 'menu';
    const owner = await isOwner(url.searchParams.get('k'));
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.state.acceptWebSocket(server);
    server.serializeAttachment({ w, id, at: Date.now(), owner });
    if (id) { try { await this.seen(id); } catch (e) {} }
    await this.tell();
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, d) {
    if (typeof d !== 'string' || d.length > 200) return;
    if (d === 'ping') { try { ws.send('pong'); } catch (e) {} return; }
    let m = null; try { m = JSON.parse(d); } catch (e) { return; }
    if (!m || !WHERE.includes(m.w)) return;
    let a = null; try { a = ws.deserializeAttachment(); } catch (e) {}
    if (!a || a.w === m.w) return;
    try { ws.serializeAttachment({ ...a, w: m.w }); } catch (e) {}
    await this.tell();
  }

  async webSocketClose(ws) { await this.bye(ws); }
  async webSocketError(ws) { await this.bye(ws); }
  async bye(ws) {
    let a = null; try { a = ws.deserializeAttachment(); } catch (e) {}
    if (!a || a.gone) return;
    try { ws.serializeAttachment({ ...a, gone: true }); } catch (e) {}
    try { ws.close(1000, 'bye'); } catch (e) {}
    await this.tell(ws);
  }
}

function statsPage(d) {
  const n = v => Number(v || 0).toLocaleString('en-GB');
  const when = d.mostOnlineWhen ? new Date(d.mostOnlineWhen).toLocaleString('en-GB', { timeZone: 'Europe/London', dateStyle: 'medium', timeStyle: 'short' }) : 'not yet';
  const tile = (big, label, sub) => `<div class="t"><b>${big}</b><span>${label}</span>${sub ? `<small>${sub}</small>` : ''}</div>`;
  return `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="refresh" content="10"><title>Dome Rounds · players</title>
<style>body{margin:0;min-height:100vh;background:#1a0d2e;color:#f4ecff;font:16px/1.4 system-ui,sans-serif;display:flex;flex-direction:column;align-items:center;padding:28px 16px;box-sizing:border-box}
h1{font-size:22px;margin:0 0 4px;color:#ffd23d}p{margin:0 0 22px;color:#bfa9e6;font-size:13px}
.live{display:flex;align-items:center;gap:10px;font-size:15px;margin-bottom:18px}.live i{width:12px;height:12px;border-radius:50%;background:#3dff8a;box-shadow:0 0 0 0 rgba(61,255,138,.6);animation:p 1.6s infinite}@keyframes p{70%{box-shadow:0 0 0 10px rgba(61,255,138,0)}}
.g{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;width:100%;max-width:720px}
.t{background:#2a1548;border:2px solid #4a2a80;padding:14px 16px;display:flex;flex-direction:column}.t b{font-size:38px;line-height:1.05;color:#fff}.t span{font-size:13px;color:#d9c8ff;font-weight:600}.t small{font-size:12px;color:#9b86c4;margin-top:4px}
.t.big{grid-column:1/-1;border-color:#ffd23d}.t.big b{font-size:64px;color:#ffd23d}h2{font-size:14px;color:#bfa9e6;margin:22px 0 10px;width:100%;max-width:720px;font-weight:600}</style></head><body>
<h1>Dome Rounds · players</h1><p>Updates every 10 seconds · ${d.ukDate} (UK)</p>
<div class="live"><i></i>Live now</div>
<div class="g">${tile(n(d.online), 'online right now', d.backgroundTabs ? `plus ${n(d.backgroundTabs)} with the game in a background tab` : '')}${tile(n(d.publicMatches), 'in public matches')}${tile(n(d.privateRooms), 'in private rooms')}${tile(n(d.vsBots), 'playing bots')}${tile(n(d.onMenu), 'on the menu')}</div>
<h2>Totals</h2>
<div class="g">${tile(n(d.playersToday), 'players today', 'different devices since midnight UK time')}${tile(n(d.playersAllTime), 'players all-time', 'different devices since counting began')}${tile(n(d.mostOnlineAtOnce), 'most online at once', when)}</div>
</body></html>`.replace('<div class="g">' + tile(n(d.online), 'online right now'), '<div class="g">' + tile(n(d.online), 'online right now')).replace('<div class="t"><b>' + n(d.online), '<div class="t big"><b>' + n(d.online));
}
