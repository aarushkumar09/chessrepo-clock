import { WebSocket } from 'ws';

export type PlayerColor = 'w' | 'b';
export type SocketRole = 'w' | 'b' | 'spectator';

export interface AuthenticatedSocket extends WebSocket {
  gameId: string;
  userId: string;
  role: SocketRole;
  isAlive: boolean;
}

export function getRoomKey(gameId: string, simulId?: string | null): string {
  return simulId ? `simul_${simulId}_${gameId}` : gameId;
}

export interface Premove {
  from: string;
  to: string;
  promotion?: string;
}

export interface DbSyncTask {
  ply: number;
  san: string;
  from: string;
  to: string;
  promotion?: string;
  color?: PlayerColor;
  fen: string;
  pgn?: string;
  whiteTimeLeft: number;
  blackTimeLeft: number;
  timestamp: number;
  isGameOver: boolean;
  result?: string;
  endReason?: string;
  simulId?: string;
}

export interface GameInitConfig {
  gameId: string;
  simulId?: string;
  isTournament?: boolean;
  initialGraceRemainingMs?: number;
  whitePlayerId: string;
  blackPlayerId: string;
  whiteName?: string;
  blackName?: string;
  timeControl: string;
  baseTimeMs: number;
  incrementMs: number;
  startingFen?: string;
  initialWhiteMs?: number;
  initialBlackMs?: number;
  initialPly?: number;
  destroyDelayMs?: number;
}

// Client -> Server Messages
export type ClientMessage =
  | { type: 'move'; from: string; to: string; promotion?: string }
  | { type: 'premove'; from: string; to: string; promotion?: string }
  | { type: 'cancel_premove' }
  | { type: 'resign' }
  | { type: 'offer_draw' }
  | { type: 'accept_draw' }
  | { type: 'decline_draw' }
  | { type: 'ping' };

// Server -> Client Messages
export type ServerMessage =
  | {
      type: 'init';
      gameId: string;
      fen: string;
      turn: PlayerColor;
      whiteMs: number;
      blackMs: number;
      movesCount: number;
      role: SocketRole;
      status: 'active' | 'completed';
      graceSecsRemaining: number | null;
      drawOfferedBy: PlayerColor | null;
      serverTimestamp: number;
    }
  | {
      type: 'move';
      san: string;
      from: string;
      to: string;
      promotion?: string;
      fen: string;
      turn: PlayerColor;
      whiteMs: number;
      blackMs: number;
      movesCount: number;
      isPremove: boolean;
      serverTimestamp: number;
    }
  | {
      type: 'clock_sync';
      whiteMs: number;
      blackMs: number;
      turn: PlayerColor;
      graceSecsRemaining: number | null;
      serverTimestamp: number;
    }
  | {
      type: 'draw_offer';
      offeredBy: PlayerColor;
    }
  | {
      type: 'draw_declined';
    }
  | {
      type: 'premove_status';
      queued: boolean;
      premove?: Premove | null;
    }
  | {
      type: 'game_over';
      result: string;
      reason: string;
      winner: PlayerColor | 'draw' | null;
      whiteMs: number;
      blackMs: number;
      fen: string;
    }
  | {
      type: 'error';
      message: string;
      code?: string;
      fen?: string;
    }
  | {
      type: 'pong';
    };
