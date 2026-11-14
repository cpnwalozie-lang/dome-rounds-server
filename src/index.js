// Dome Rounds game server.
// Each room code gets its own Durable Object: a tiny always-on room that both players connect to over a secure WebSocket
// (wss on port 443, so it looks like normal web traffic to strict networks). The room creator's game is still the referee;
// the server passes each player's updates to the other player, and tells everyone when someone joins or leaves.

const MAX_MSG = 16384;         // bytes; a game update is about 1-3 KB (more with bots)
const MAX_PLAYERS = 4;         // 1v1 or free-for-all (up to 4)

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const cors = { 'Access-Control-Allow-Origin': '*' };
    if (url.pathname === '/' || url.pathname === '/health') {
      return new Response(JSON.stringify({ ok: true, game: 'dome-rounds' }), { headers: { 'content-type': 'application/json', ...cors } });
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
    this.socks = new Map(); // peer -> { ws, role }
  }

  async fetch(req) {
    const url = new URL(req.url);
    const role = url.searchParams.get('role') === 'host' ? 'host' : 'guest';
    const peer = (url.searchParams.get('peer') || '').replace(/[^a-z0-9]/gi, '').slice(0, 24) || Math.random().toString(36).slice(2, 12);
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();

    const fail = (code) => { try { server.send(JSON.stringify({ t: 'err', code })); server.close(1000, code); } catch (e) {} };
    const hasHost = [...this.socks.values()].some(s => s.role === 'host');
    if (role === 'host' && hasHost) fail('code_taken');
    else if (role === 'guest' && !hasHost) fail('no_room');
    else if (this.socks.size >= MAX_PLAYERS) fail('full');
    else this.add(peer, role, server);
    return new Response(null, { status: 101, webSocket: client });
  }

  add(peer, role, ws) {
    const me = { ws, role };
    this.socks.set(peer, me);
    ws.send(JSON.stringify({ t: 'hello', me: peer, peers: [...this.socks.keys()].filter(p => p !== peer) }));
    this.broadcast(peer, JSON.stringify({ t: 'joined', f: peer }));
    ws.addEventListener('message', (ev) => {
      const d = ev.data;
      if (typeof d !== 'string' || d.length > MAX_MSG) return;
      if (d === 'ping') { try { ws.send('pong'); } catch (e) {} return; }
      // pass the player's latest state straight on, tagged with who sent it (no parsing needed)
      this.broadcast(peer, '{"t":"p","f":"' + peer + '","d":' + d + '}');
    });
    const gone = () => {
      if (this.socks.get(peer) !== me) return;
      this.socks.delete(peer);
      this.broadcast(peer, JSON.stringify({ t: 'left', f: peer }));
      if (role === 'host') for (const [p, s] of this.socks) { try { s.ws.send(JSON.stringify({ t: 'left', f: peer })); s.ws.close(1000, 'host_left'); } catch (e) {} this.socks.delete(p); }
    };
    ws.addEventListener('close', gone);
    ws.addEventListener('error', gone);
  }

  broadcast(from, msg) {
    for (const [p, s] of this.socks) { if (p === from) continue; try { s.ws.send(msg); } catch (e) {} }
  }
}
