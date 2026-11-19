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
