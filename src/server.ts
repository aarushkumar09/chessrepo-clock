import http from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import jwt from 'jsonwebtoken';
import dotenv from 'dotenv';
import { AuthenticatedSocket, GameInitConfig, getRoomKey } from './types.js';
export { getRoomKey };
import { GameRoom } from './engine/GameRoom.js';
import { DbSyncQueue, PersistenceHandler } from './sync/DbSyncQueue.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

dotenv.config();

const PORT = parseInt(process.env.PORT || '4000', 10);
const JWT_SECRET = process.env.CLOCK_SERVER_SECRET || 'dev-secret-clock-key-123';

// In-Memory Active Game Registry
const activeRooms = new Map<string, GameRoom>();

// Rule 3 (Architectural Fix): Cold-Start Mutex Lock
// Guarantees simultaneous connections for the same gameId await the identical loading promise
const roomLoadingLocks = new Map<string, Promise<GameRoom>>();


/**
 * Persistence Handler: Flushes moves to the Mumbai database or Firebase RTDB.
 * In local testing, logs or calls the main API. In production, calls Mumbai endpoint.
 */
const defaultPersistenceHandler: PersistenceHandler = async (gameId, task) => {
  const dbUrl = process.env.MUMBAI_SYNC_URL;
  if (!dbUrl) {
    // Local / dev fallback: logged to console
    return;
  }

  try {
    const response = await fetch(`${dbUrl}/api/internal/clock-sync`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-clock-secret': process.env.CLOCK_SERVER_SECRET || 'dev-secret-clock-key-123',
      },
      body: JSON.stringify({ gameId, simulId: task.simulId, task }),
    });

    if (!response.ok) {
      throw new Error(`[Mumbai Sync] HTTP ${response.status}: ${await response.text()}`);
    }
  } catch (err: any) {
    if (process.env.NODE_ENV !== 'production') {
      // Local development without Next.js active on port 3000: avoid spamming retry loops
      return;
    }
    throw err;
  }
};

/**
 * Fetch initial match configuration from Mumbai DB if not already in Ohio RAM
 */
async function loadGameFromDatabase(gameId: string, simulId?: string): Promise<GameRoom> {
  const dbUrl = process.env.MUMBAI_SYNC_URL;
  let config: GameInitConfig;

  if (dbUrl && !gameId.startsWith('sandbox')) {
    try {
      const query = new URLSearchParams({ gameId });
      if (simulId) query.set('simulId', simulId);
      const res = await fetch(`${dbUrl}/api/internal/game-info?${query.toString()}`, {
        headers: { 'x-clock-secret': JWT_SECRET },
      });
      if (!res.ok) {
        throw new Error(`Failed to load game ${gameId} from Mumbai: ${res.statusText}`);
      }
      config = (await res.json()) as GameInitConfig;
    } catch (err) {
      if (process.env.NODE_ENV !== 'production') {
        console.log(`[Clock Service] Mumbai DB offline. Creating dev room for ${gameId}`);
        config = {
          gameId,
          simulId,
          whitePlayerId: 'white-player',
          blackPlayerId: 'black-player',
          timeControl: '5+0',
          baseTimeMs: 300_000,
          incrementMs: 0,
        };
      } else {
        throw err;
      }
    }
  } else {
    // Default fallback for standalone local dev matches & sandbox
    config = {
      gameId,
      simulId,
      whitePlayerId: 'white-player',
      blackPlayerId: 'black-player',
      timeControl: '5+0',
      baseTimeMs: 300_000,
      incrementMs: 0,
    };
  }

  const roomKey = getRoomKey(gameId, simulId);
  return new GameRoom(
    config,
    defaultPersistenceHandler,
    (finishedKey) => activeRooms.delete(finishedKey)
  );
}

/**
 * Cold-Start Mutex Resolver:
 * Both incoming connections await the exact same Promise instance
 */
async function getOrCreateGameRoom(gameId: string, simulId?: string): Promise<GameRoom> {
  const roomKey = getRoomKey(gameId, simulId);
  const existing = activeRooms.get(roomKey);
  if (existing) return existing;

  let loadingLock = roomLoadingLocks.get(roomKey);
  if (!loadingLock) {
    loadingLock = loadGameFromDatabase(gameId, simulId)
      .then((room) => {
        activeRooms.set(roomKey, room);
        roomLoadingLocks.delete(roomKey);
        return room;
      })
      .catch((err) => {
        roomLoadingLocks.delete(roomKey);
        throw err;
      });
    roomLoadingLocks.set(roomKey, loadingLock);
  }

  return loadingLock;
}

// HTTP Server for Health Checks & Game Init Webhooks
const server = http.createServer((req, res) => {
  const url = new URL(req.url || '/', `http://${req.headers.host}`);

  // Health check endpoint for AWS Lightsail / Caddy
  if (url.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', activeGames: activeRooms.size }));
    return;
  }

  // Standalone Visual Sandbox for browser testing
  if ((url.pathname === '/sandbox' || url.pathname === '/sandbox.html') && req.method === 'GET') {
    const sandboxPath = path.resolve(__dirname, 'sandbox.html');
    const fallbackPath = path.resolve(process.cwd(), 'src', 'sandbox.html');
    const targetPath = fs.existsSync(sandboxPath) ? sandboxPath : fallbackPath;
    if (fs.existsSync(targetPath)) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      fs.createReadStream(targetPath).pipe(res);
      return;
    } else {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('sandbox.html not found');
      return;
    }
  }

  // Static Assets for Sandbox (Chessground & Chess.js)
  if (url.pathname.startsWith('/chessground/') && req.method === 'GET') {
    const asset = url.pathname.replace('/chessground/', '');
    let filePath = '';
    let contentType = 'text/plain';

    if (asset.endsWith('.css')) {
      filePath = path.resolve(process.cwd(), 'node_modules/chessground/assets', asset);
      contentType = 'text/css';
    } else if (asset.endsWith('.js')) {
      filePath = path.resolve(process.cwd(), 'node_modules/chessground/dist', asset);
      contentType = 'application/javascript';
    }

    if (filePath && fs.existsSync(filePath)) {
      res.writeHead(200, { 'Content-Type': contentType });
      fs.createReadStream(filePath).pipe(res);
      return;
    }
  }

  if (url.pathname.startsWith('/chessjs/') && req.method === 'GET') {
    const asset = url.pathname.replace('/chessjs/', '');
    const filePath = path.resolve(process.cwd(), 'node_modules/chess.js/dist/esm', asset);
    if (fs.existsSync(filePath)) {
      res.writeHead(200, { 'Content-Type': 'application/javascript' });
      fs.createReadStream(filePath).pipe(res);
      return;
    }
  }

  // Pre-initialize a game into RAM (called by Next.js when game starts)
  if (url.pathname === '/init-game' && req.method === 'POST') {
    // Security check: require x-clock-secret header matching JWT_SECRET
    const secretHeader = req.headers['x-clock-secret'];
    if (secretHeader !== JWT_SECRET) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Unauthorized: missing or invalid x-clock-secret' }));
      return;
    }

    let body = '';
    req.on('data', (chunk) => (body += chunk));
    req.on('end', () => {
      try {
        const config = JSON.parse(body) as GameInitConfig;
        if (!config.gameId || !config.whitePlayerId || !config.blackPlayerId) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Missing required fields' }));
          return;
        }

        // Memory leak fix: dispose existing room and clear active intervals if replacing
        const roomKey = getRoomKey(config.gameId, config.simulId);
        const existingRoom = activeRooms.get(roomKey);
        if (existingRoom) {
          existingRoom.dispose();
        }

        const room = new GameRoom(
          config,
          defaultPersistenceHandler,
          (finishedKey) => activeRooms.delete(finishedKey)
        );
        activeRooms.set(roomKey, room);

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, gameId: config.gameId }));
      } catch (err: any) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err.message }));
      }
    });
    return;
  }

  // External termination (called by Next.js when coach/admin concludes match externally)
  if (url.pathname === '/terminate-game' && req.method === 'POST') {
    const secretHeader = req.headers['x-clock-secret'];
    if (secretHeader !== JWT_SECRET) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Unauthorized: missing or invalid x-clock-secret' }));
      return;
    }

    let body = '';
    req.on('data', (chunk) => (body += chunk));
    req.on('end', () => {
      try {
        const { gameId, simulId, result, reason } = JSON.parse(body);
        if (!gameId || !result) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Missing gameId or result' }));
          return;
        }

        const roomKey = getRoomKey(gameId, simulId);
        const room = activeRooms.get(roomKey);
        if (room) {
          room.forceTerminate(result, reason || 'Terminated externally');
        }

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true }));
      } catch (err: any) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err.message }));
      }
    });
    return;
  }

  res.writeHead(404);
  res.end('Not Found');
});

// WebSocket Server
const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', async (req, socket, head) => {
  try {
    const url = new URL(req.url || '/', `http://${req.headers.host}`);
    const match = url.pathname.match(/^\/game\/([^/]+)$/);

    if (!match) {
      socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
      socket.destroy();
      return;
    }

    const gameId = match[1];
    const simulId = url.searchParams.get('simulId') || undefined;
    const token = url.searchParams.get('token');
    const roleParam = url.searchParams.get('role');

    // Rule 4: Authentication & Color Binding Verification
    let userId = 'spectator';
    if (token) {
      try {
        const decoded = jwt.verify(token, JWT_SECRET) as any;
        userId = decoded.userId || decoded.uid || decoded.sub || 'spectator';
      } catch {
        // In local development, allow raw user IDs for easy debugging if token fails
        if (process.env.NODE_ENV !== 'production') {
          userId = token;
        } else {
          socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
          socket.destroy();
          return;
        }
      }
    }

    // Load or retrieve room with cold-start mutex protection and simulId routing
    const roomKey = getRoomKey(gameId, simulId);
    const room = await getOrCreateGameRoom(gameId, simulId);

    wss.handleUpgrade(req, socket, head, (ws) => {
      const authSocket = ws as AuthenticatedSocket;
      authSocket.gameId = roomKey;
      authSocket.userId = userId;
      authSocket.isAlive = true;

      // Color binding
      if (
        userId === room.whitePlayerId ||
        (process.env.NODE_ENV !== 'production' && (userId === 'w' || userId === 'white' || roleParam === 'w'))
      ) {
        authSocket.role = 'w';
      } else if (
        userId === room.blackPlayerId ||
        (process.env.NODE_ENV !== 'production' && (userId === 'b' || userId === 'black' || roleParam === 'b'))
      ) {
        authSocket.role = 'b';
      } else {
        authSocket.role = 'spectator';
      }

      wss.emit('connection', authSocket, req);
    });
  } catch (error) {
    console.error('[Upgrade Error]:', error);
    socket.write('HTTP/1.1 500 Internal Server Error\r\n\r\n');
    socket.destroy();
  }
});

wss.on('connection', (ws: AuthenticatedSocket) => {
  const room = activeRooms.get(ws.gameId);
  if (!room) {
    ws.close(1011, 'Room not found');
    return;
  }

  room.attachSocket(ws);

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (raw) => {
    try {
      const data = JSON.parse(raw.toString());
      switch (data.type) {
        case 'move':
          room.handleMove(ws, data.from, data.to, data.promotion);
          break;
        case 'premove':
          room.handlePremove(ws, data.from, data.to, data.promotion);
          break;
        case 'cancel_premove':
          room.handleCancelPremove(ws);
          break;
        case 'resign':
          room.handleResign(ws);
          break;
        case 'offer_draw':
          room.handleOfferDraw(ws);
          break;
        case 'accept_draw':
          room.handleAcceptDraw(ws);
          break;
        case 'decline_draw':
          room.handleDeclineDraw(ws);
          break;
        case 'ping':
          ws.send(JSON.stringify({ type: 'pong' }));
          break;
        default:
          break;
      }
    } catch (e: any) {
      ws.send(JSON.stringify({ type: 'error', message: 'Malformed JSON payload' }));
    }
  });

  ws.on('close', () => {
    room.detachSocket(ws);
  });
});

// Ping/Pong Keep-Alive (terminates dead TCP sockets)
const keepAliveInterval = setInterval(() => {
  for (const client of wss.clients) {
    const authSocket = client as AuthenticatedSocket;
    if (!authSocket.isAlive) {
      authSocket.terminate();
      continue;
    }
    authSocket.isAlive = false;
    authSocket.ping();
  }
}, 30_000);

wss.on('close', () => {
  clearInterval(keepAliveInterval);
});

server.listen(PORT, () => {
  console.log(`[Clock Service] Real-time clock microservice running on port ${PORT}`);
});

export { server, activeRooms, roomLoadingLocks, getOrCreateGameRoom };
