# Dome Rounds server

The online rooms for [Dome Rounds](https://dome-rounds.netlify.app). Each room code gets its own Cloudflare Durable Object, and both players connect to it over a secure WebSocket on the normal `https` port, so matches work on strict school and office networks. The room creator's game is the referee; this server passes each player's updates to the other and says when someone joins or leaves.

- `GET /health` checks it's up.
- `wss://<this worker>/room/<code>?role=host|guest&peer=<id>` joins a room. Errors come back as `{"t":"err","code":"no_room" | "code_taken" | "full"}`.

Runs on the Cloudflare Workers Free plan. Pushing to `main` redeploys it when the repo is connected to Cloudflare (Workers & Pages, then Import a repository).

Local test: `npm install`, then `npm run dev`.
