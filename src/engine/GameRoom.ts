import { Chess } from 'chess.js';
import {
  AuthenticatedSocket,
  GameInitConfig,
  PlayerColor,
  Premove,
  ServerMessage,
} from '../types.js';
import { DbSyncQueue, PersistenceHandler } from '../sync/DbSyncQueue.js';

export const MOVE1_GRACE_MS = 240_000; // 4 minutes
export const HEARTBEAT_INTERVAL_MS = 2_000; // 2s periodic sync
export const ROOM_DESTROY_DELAY_MS = 15_000; // 15s after completion to free RAM

export class GameRoom {
  public readonly gameId: string;
  public readonly simulId?: string;
  public readonly isTournament: boolean;
  public readonly whitePlayerId: string;
  public readonly blackPlayerId: string;
  public readonly timeControl: string;
  public readonly baseTimeMs: number;
  public readonly incrementMs: number;
  public drawOffer: PlayerColor | null = null;

  private chess: Chess;
  public status: 'active' | 'completed' = 'active';
  public result: string = '*';
  public endReason: string = '';

  // Monotonic RAM Clocks (process.hrtime.bigint() in nanoseconds)
  public whiteRemainingMs: number;
  public blackRemainingMs: number;
  private turnStartHr: bigint;
  private isGracePeriod: boolean = true;
  private graceStartHr: bigint;
  private graceDurationMs: number;

  // Exact Millisecond Timeout Timer
  private exactFlagTimer: NodeJS.Timeout | null = null;
  private heartbeatTimer: NodeJS.Timeout | null = null;
  private destroyTimer: NodeJS.Timeout | null = null;

  // Zero-Latency Server Premoves
  private whitePremove: Premove | null = null;
  private blackPremove: Premove | null = null;

  // Active Sockets
  public readonly sockets: Set<AuthenticatedSocket> = new Set();

  // Sequential Outbound DB Queue
  private syncQueue: DbSyncQueue;
  private onDestroy: (gameId: string) => void;
  private destroyDelayMs: number;
  private plyCount: number;

  constructor(
    config: GameInitConfig,
    persistHandler: PersistenceHandler,
    onDestroy: (gameId: string) => void
  ) {
    this.gameId = config.gameId;
    this.simulId = config.simulId;
    this.isTournament = config.isTournament ?? false;
    this.whitePlayerId = config.whitePlayerId;
    this.blackPlayerId = config.blackPlayerId;
    this.timeControl = config.timeControl;
    this.baseTimeMs = config.baseTimeMs;
    this.incrementMs = config.incrementMs;
    this.onDestroy = onDestroy;
    this.destroyDelayMs = config.destroyDelayMs ?? ROOM_DESTROY_DELAY_MS;
    this.graceDurationMs = config.initialGraceRemainingMs ?? MOVE1_GRACE_MS;

    if (config.initialPly !== undefined) {
      this.plyCount = config.initialPly;
    } else if (config.startingFen) {
      try {
        const parts = config.startingFen.trim().split(/\s+/);
        if (parts.length >= 6) {
          const activeColor = parts[1]; // 'w' | 'b'
          const fullMove = parseInt(parts[5], 10);
          if (!isNaN(fullMove) && fullMove >= 1) {
            this.plyCount = (fullMove - 1) * 2 + (activeColor === 'b' ? 1 : 0);
          } else {
            this.plyCount = 0;
          }
        } else {
          this.plyCount = 0;
        }
      } catch {
        this.plyCount = 0;
      }
    } else {
      this.plyCount = 0;
    }
    this.chess = new Chess(config.startingFen);
    this.whiteRemainingMs = config.initialWhiteMs ?? config.baseTimeMs;
    this.blackRemainingMs = config.initialBlackMs ?? config.baseTimeMs;

    const nowHr = process.hrtime.bigint();
    this.turnStartHr = nowHr;
    this.graceStartHr = nowHr;
    this.isGracePeriod = this.plyCount < 2;

    this.syncQueue = new DbSyncQueue(
      this.gameId,
      persistHandler,
      () => this.checkGarbageCollection()
    );

    this.armExactFlagTimer();
    this.startHeartbeat();
  }

  public get turn(): PlayerColor {
    return this.chess.turn() as PlayerColor;
  }

  public get fen(): string {
    return this.chess.fen();
  }

  public get movesCount(): number {
    return this.plyCount;
  }

  // Attach a newly authenticated client socket
  public attachSocket(socket: AuthenticatedSocket): void {
    this.sockets.add(socket);

    // Send complete init snapshot immediately
    const initMsg: ServerMessage = {
      type: 'init',
      gameId: this.gameId,
      fen: this.chess.fen(),
      turn: this.turn,
      whiteMs: this.getLiveRemainingMs('w'),
      blackMs: this.getLiveRemainingMs('b'),
      movesCount: this.movesCount,
      role: socket.role,
      status: this.status,
      graceSecsRemaining: this.getGraceSecsRemaining(),
      drawOfferedBy: this.drawOffer,
      serverTimestamp: Date.now(),
    };
    socket.send(JSON.stringify(initMsg));

    // Reconnect Premove Sync: if player has a queued premove, restore it on their UI immediately
    if (socket.role === 'w' && this.whitePremove) {
      socket.send(
        JSON.stringify({
          type: 'premove_status',
          queued: true,
          premove: this.whitePremove,
        })
      );
    } else if (socket.role === 'b' && this.blackPremove) {
      socket.send(
        JSON.stringify({
          type: 'premove_status',
          queued: true,
          premove: this.blackPremove,
        })
      );
    }
  }

  public detachSocket(socket: AuthenticatedSocket): void {
    this.sockets.delete(socket);
  }

  // Calculate live remaining time using CPU monotonic nanosecond counter
  public getLiveRemainingMs(color: PlayerColor): number {
    if (this.status !== 'active') {
      return color === 'w' ? this.whiteRemainingMs : this.blackRemainingMs;
    }

    // During Move 1 grace period (moves < 2) or Unlimited time control, base clocks do not tick down
    if (this.isGracePeriod || this.baseTimeMs === 0 || this.timeControl.toLowerCase() === 'unlimited') {
      return color === 'w' ? this.whiteRemainingMs : this.blackRemainingMs;
    }

    if (this.turn !== color) {
      return color === 'w' ? this.whiteRemainingMs : this.blackRemainingMs;
    }

    const elapsedNs = process.hrtime.bigint() - this.turnStartHr;
    const elapsedMs = Number(elapsedNs / BigInt(1_000_000));
    const base = color === 'w' ? this.whiteRemainingMs : this.blackRemainingMs;
    return Math.max(0, base - elapsedMs);
  }

  private getGraceSecsRemaining(): number | null {
    if (!this.isGracePeriod || this.status !== 'active') return null;
    const elapsedNs = process.hrtime.bigint() - this.graceStartHr;
    const elapsedMs = Number(elapsedNs / BigInt(1_000_000));
    const remainingMs = Math.max(0, this.graceDurationMs - elapsedMs);
    return Math.ceil(remainingMs / 1000);
  }

  // Arm exact millisecond flag timer to eliminate polling delays
  private armExactFlagTimer(): void {
    if (this.exactFlagTimer) {
      clearTimeout(this.exactFlagTimer);
      this.exactFlagTimer = null;
    }

    if (this.status !== 'active') return;

    if (this.isGracePeriod) {
      const elapsedNs = process.hrtime.bigint() - this.graceStartHr;
      const elapsedMs = Number(elapsedNs / BigInt(1_000_000));
      const remainingMs = Math.max(0, this.graceDurationMs - elapsedMs);
      this.exactFlagTimer = setTimeout(() => {
        this.triggerTimeoutForfeit(this.turn, true);
      }, remainingMs);
      return;
    }

    // Unlimited time control games never flag on time after Move 1
    if (this.baseTimeMs === 0 || this.timeControl.toLowerCase() === 'unlimited') {
      return;
    }

    const activeMs = this.getLiveRemainingMs(this.turn);
    this.exactFlagTimer = setTimeout(() => {
      this.triggerTimeoutForfeit(this.turn, false);
    }, activeMs);
  }

  // Handle incoming move from a client socket
  public handleMove(
    socket: AuthenticatedSocket,
    from: string,
    to: string,
    promotion?: string
  ): void {
    if (this.status !== 'active') {
      socket.send(JSON.stringify({ type: 'error', message: 'Game is not active' }));
      return;
    }

    if (socket.role === 'spectator') {
      socket.send(JSON.stringify({ type: 'error', message: 'Spectators cannot make moves' }));
      return;
    }

    if (socket.role !== this.turn) {
      socket.send(JSON.stringify({ type: 'error', message: 'Not your turn' }));
      return;
    }

    // Rule 2: Pre-check monotonic timeout BEFORE running chess.move()
    const currentTurn = this.turn;
    const liveMs = this.getLiveRemainingMs(currentTurn);
    if (!this.isGracePeriod && liveMs <= 0) {
      this.triggerTimeoutForfeit(currentTurn, false);
      return;
    }

    const success = this.executeValidatedMove(from, to, promotion, false);
    if (!success) {
      socket.send(
        JSON.stringify({
          type: 'error',
          message: 'Illegal move',
          fen: this.chess.fen(),
        })
      );
    }
  }

  // Queue a zero-latency premove when it is NOT the player's turn
  public handlePremove(
    socket: AuthenticatedSocket,
    from: string,
    to: string,
    promotion?: string
  ): void {
    if (this.status !== 'active') return;
    if (socket.role === 'spectator') return;

    if (socket.role === 'w') {
      this.whitePremove = { from, to, promotion };
    } else {
      this.blackPremove = { from, to, promotion };
    }

    socket.send(
      JSON.stringify({
        type: 'premove_status',
        queued: true,
        premove: { from, to, promotion },
      })
    );
  }

  public handleCancelPremove(socket: AuthenticatedSocket): void {
    if (socket.role === 'w') {
      this.whitePremove = null;
    } else if (socket.role === 'b') {
      this.blackPremove = null;
    }
    socket.send(JSON.stringify({ type: 'premove_status', queued: false }));
  }

  // Core move execution logic (used by both live moves and queued premoves)
  private executeValidatedMove(
    from: string,
    to: string,
    promotion?: string,
    isPremoveExecution: boolean = false
  ): boolean {
    const moverColor = this.turn;

    // Rule 1 Fix: Validate move in chess.js FIRST before ANY clock or state mutation!
    let moveResult: any;
    try {
      moveResult = this.chess.move({
        from,
        to,
        promotion: (promotion as any) || 'q',
      });
    } catch {
      return false;
    }

    if (!moveResult) return false;

    // Move is strictly legal: Now perform clock deduction and state updates
    const nowHr = process.hrtime.bigint();
    if (!this.isGracePeriod) {
      if (this.baseTimeMs === 0 || this.timeControl.toLowerCase() === 'unlimited') {
        // Unlimited games do not deduct time
      } else if (isPremoveExecution) {
        // Zero-latency premove: 0ms deducted, plus increment
        if (moverColor === 'w') {
          this.whiteRemainingMs += this.incrementMs;
        } else {
          this.blackRemainingMs += this.incrementMs;
        }
      } else {
        const elapsedNs = nowHr - this.turnStartHr;
        const elapsedMs = Math.max(1, Number(elapsedNs / BigInt(1_000_000)));
        if (moverColor === 'w') {
          const newTime = Math.max(0, this.whiteRemainingMs - elapsedMs + this.incrementMs);
          this.whiteRemainingMs = this.incrementMs === 0 ? Math.min(newTime, this.whiteRemainingMs) : newTime;
        } else {
          const newTime = Math.max(0, this.blackRemainingMs - elapsedMs + this.incrementMs);
          this.blackRemainingMs = this.incrementMs === 0 ? Math.min(newTime, this.blackRemainingMs) : newTime;
        }
      }
    }

    // Reset turn timestamp anchor & advance ply
    this.turnStartHr = nowHr;
    this.plyCount++;
    const newPly = this.plyCount;

    // Move 1 Grace Period transition: active until 2 half-moves (1 full move each)
    if (newPly === 1) {
      // White played move 1; Black now gets their full 240s grace period
      this.graceStartHr = nowHr;
      this.graceDurationMs = MOVE1_GRACE_MS;
    } else if (newPly >= 2) {
      this.isGracePeriod = false;
    }

    // Auto-cancel any pending draw offer on move
    if (this.drawOffer !== null) {
      this.drawOffer = null;
      this.broadcast({ type: 'draw_declined' });
    }

    // 3. Check for game termination (Checkmate, Stalemate, Draws)
    let isGameOver = false;
    if (this.chess.isCheckmate()) {
      isGameOver = true;
      this.status = 'completed';
      this.result = moverColor === 'w' ? '1-0' : '0-1';
      this.endReason = 'Checkmate';
    } else if (this.chess.isDraw() || this.chess.isStalemate() || this.chess.isThreefoldRepetition() || this.chess.isInsufficientMaterial()) {
      isGameOver = true;
      this.status = 'completed';
      this.result = '1/2-1/2';
      this.endReason = this.chess.isStalemate()
        ? 'Stalemate'
        : this.chess.isThreefoldRepetition()
        ? 'Threefold Repetition'
        : 'Draw';
    }

    // 4. Enqueue to sequential FIFO outbound DB queue
    this.syncQueue.enqueue({
      ply: newPly,
      san: moveResult.san,
      from: moveResult.from,
      to: moveResult.to,
      promotion: moveResult.promotion,
      color: moverColor,
      fen: this.chess.fen(),
      pgn: this.chess.pgn(),
      whiteTimeLeft: this.whiteRemainingMs,
      blackTimeLeft: this.blackRemainingMs,
      timestamp: Date.now(),
      isGameOver,
      result: isGameOver ? this.result : undefined,
      endReason: isGameOver ? this.endReason : undefined,
      simulId: this.simulId,
    });

    // 5. Broadcast move to all connected sockets
    const moveMsg: ServerMessage = {
      type: 'move',
      san: moveResult.san,
      from: moveResult.from,
      to: moveResult.to,
      promotion: moveResult.promotion,
      fen: this.chess.fen(),
      turn: this.turn,
      whiteMs: this.whiteRemainingMs,
      blackMs: this.blackRemainingMs,
      movesCount: newPly,
      isPremove: isPremoveExecution,
      serverTimestamp: Date.now(),
    };
    this.broadcast(moveMsg);

    if (isGameOver) {
      this.broadcastGameOver(this.result, this.endReason, moverColor);
      this.cleanupTimers();
      this.checkGarbageCollection();
      return true;
    }

    // 6. Rearm exact millisecond flag timer for the new active player
    this.armExactFlagTimer();

    // 7. Rule 1: Instant Server-Side Premove Execution
    const nextTurn = this.turn;
    const queuedPremove = nextTurn === 'w' ? this.whitePremove : this.blackPremove;
    if (queuedPremove) {
      if (nextTurn === 'w') this.whitePremove = null;
      else this.blackPremove = null;

      // Try executing premove immediately on the server
      const executed = this.executeValidatedMove(
        queuedPremove.from,
        queuedPremove.to,
        queuedPremove.promotion,
        true
      );

      if (!executed) {
        // Premove was illegal in new position: notify player
        const targetSocket = Array.from(this.sockets).find((s) => s.role === nextTurn);
        if (targetSocket) {
          targetSocket.send(JSON.stringify({ type: 'premove_status', queued: false }));
        }
      }
    }

    return true;
  }

  // Trigger timeout forfeit when clock or grace period expires
  public triggerTimeoutForfeit(flaggedColor: PlayerColor, isGraceTimeout: boolean): void {
    if (this.status !== 'active') return;
    this.status = 'completed';

    let winner: PlayerColor | null = null;
    if (isGraceTimeout && !this.isTournament) {
      // Non-tournament games abort on Move 1 timeout matching claimFirstMoveTimeout
      this.result = '*';
      this.endReason = 'Game aborted — failed to play Move 1 within 4 minutes';
      winner = null;
    } else {
      winner = flaggedColor === 'w' ? 'b' : 'w';
      this.result = winner === 'w' ? '1-0' : '0-1';
      this.endReason = isGraceTimeout
        ? `${flaggedColor === 'w' ? 'White' : 'Black'} forfeited — failed to play Move 1 within 4 minutes`
        : `${flaggedColor === 'w' ? 'White' : 'Black'} ran out of time`;
    }

    if (flaggedColor === 'w') this.whiteRemainingMs = 0;
    else this.blackRemainingMs = 0;

    // Enqueue timeout event to DB queue
    this.syncQueue.enqueue({
      ply: this.plyCount,
      san: 'TIMEOUT',
      from: '',
      to: '',
      color: flaggedColor,
      fen: this.chess.fen(),
      pgn: this.chess.pgn(),
      whiteTimeLeft: this.whiteRemainingMs,
      blackTimeLeft: this.blackRemainingMs,
      timestamp: Date.now(),
      isGameOver: true,
      result: this.result,
      endReason: this.endReason,
      simulId: this.simulId,
    });

    this.broadcastGameOver(this.result, this.endReason, winner);
    this.cleanupTimers();
    this.checkGarbageCollection();
  }

  public handleResign(socket: AuthenticatedSocket): void {
    if (this.status !== 'active' || socket.role === 'spectator') return;

    // Edge Case 1: Capture live remaining times BEFORE setting status = 'completed'
    this.whiteRemainingMs = this.getLiveRemainingMs('w');
    this.blackRemainingMs = this.getLiveRemainingMs('b');

    this.status = 'completed';
    this.drawOffer = null;

    const resigningColor = socket.role;
    const winner: PlayerColor = resigningColor === 'w' ? 'b' : 'w';
    this.result = winner === 'w' ? '1-0' : '0-1';
    this.endReason = `${resigningColor === 'w' ? 'White' : 'Black'} resigned`;

    this.syncQueue.enqueue({
      ply: this.plyCount,
      san: 'RESIGN',
      from: '',
      to: '',
      color: resigningColor,
      fen: this.chess.fen(),
      pgn: this.chess.pgn(),
      whiteTimeLeft: this.whiteRemainingMs,
      blackTimeLeft: this.blackRemainingMs,
      timestamp: Date.now(),
      isGameOver: true,
      result: this.result,
      endReason: this.endReason,
      simulId: this.simulId,
    });

    this.broadcastGameOver(this.result, this.endReason, winner);
    this.cleanupTimers();
    this.checkGarbageCollection();
  }

  // Lichess-style Draw Offer Protocol
  public handleOfferDraw(socket: AuthenticatedSocket): void {
    if (this.status !== 'active' || socket.role === 'spectator') return;

    // Mutual Draw Auto-Accept: if opponent already offered draw, this acts as auto-accept
    if (this.drawOffer && this.drawOffer !== socket.role) {
      this.handleAcceptDraw(socket);
      return;
    }

    this.drawOffer = socket.role;
    this.broadcast({ type: 'draw_offer', offeredBy: socket.role });
  }

  public handleDeclineDraw(socket: AuthenticatedSocket): void {
    if (this.status !== 'active' || socket.role === 'spectator') return;
    if (this.drawOffer && this.drawOffer !== socket.role) {
      this.drawOffer = null;
      this.broadcast({ type: 'draw_declined' });
    }
  }

  public handleAcceptDraw(socket: AuthenticatedSocket): void {
    if (this.status !== 'active' || socket.role === 'spectator') return;
    if (!this.drawOffer || this.drawOffer === socket.role) return;

    this.whiteRemainingMs = this.getLiveRemainingMs('w');
    this.blackRemainingMs = this.getLiveRemainingMs('b');
    this.status = 'completed';
    this.result = '1/2-1/2';
    this.endReason = 'Draw by agreement';
    this.drawOffer = null;

    this.syncQueue.enqueue({
      ply: this.plyCount,
      san: 'DRAW',
      from: '',
      to: '',
      color: socket.role,
      fen: this.chess.fen(),
      pgn: this.chess.pgn(),
      whiteTimeLeft: this.whiteRemainingMs,
      blackTimeLeft: this.blackRemainingMs,
      timestamp: Date.now(),
      isGameOver: true,
      result: this.result,
      endReason: this.endReason,
      simulId: this.simulId,
    });

    this.broadcastGameOver(this.result, this.endReason, 'draw');
    this.cleanupTimers();
    this.checkGarbageCollection();
  }

  // External termination (called by /terminate-game when coach/admin concludes match)
  public forceTerminate(result: string, reason: string): void {
    if (this.status !== 'active') return;
    this.whiteRemainingMs = this.getLiveRemainingMs('w');
    this.blackRemainingMs = this.getLiveRemainingMs('b');
    this.status = 'completed';
    this.result = result;
    this.endReason = reason;
    this.drawOffer = null;

    let winner: PlayerColor | 'draw' | null = null;
    if (result === '1-0') winner = 'w';
    else if (result === '0-1') winner = 'b';
    else if (result === '1/2-1/2') winner = 'draw';

    this.broadcastGameOver(this.result, this.endReason, winner);
    this.cleanupTimers();
    this.checkGarbageCollection();
  }

  // Periodic heartbeat broadcast for client clock alignment
  private startHeartbeat(): void {
    this.heartbeatTimer = setInterval(() => {
      if (this.status !== 'active') return;
      const syncMsg: ServerMessage = {
        type: 'clock_sync',
        whiteMs: this.getLiveRemainingMs('w'),
        blackMs: this.getLiveRemainingMs('b'),
        turn: this.turn,
        graceSecsRemaining: this.getGraceSecsRemaining(),
        serverTimestamp: Date.now(),
      };
      this.broadcast(syncMsg);
    }, HEARTBEAT_INTERVAL_MS);
  }

  private broadcast(msg: ServerMessage): void {
    const data = JSON.stringify(msg);
    for (const socket of this.sockets) {
      if (socket.readyState === 1) { // OPEN
        socket.send(data);
      }
    }
  }

  private broadcastGameOver(result: string, reason: string, winner: PlayerColor | 'draw' | null): void {
    const msg: ServerMessage = {
      type: 'game_over',
      result,
      reason,
      winner,
      whiteMs: this.whiteRemainingMs,
      blackMs: this.blackRemainingMs,
      fen: this.chess.fen(),
    };
    this.broadcast(msg);
  }

  private cleanupTimers(): void {
    if (this.exactFlagTimer) {
      clearTimeout(this.exactFlagTimer);
      this.exactFlagTimer = null;
    }
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  // Rule 3: Memory Leak Cleanup (Garbage Collection)
  private checkGarbageCollection(): void {
    if (this.status === 'completed' && this.syncQueue.isIdle) {
      if (this.destroyTimer) clearTimeout(this.destroyTimer);
      this.destroyTimer = setTimeout(() => {
        this.cleanupTimers();
        for (const socket of this.sockets) {
          socket.close(1000, 'Game finished and archived');
        }
        this.sockets.clear();
        this.onDestroy(this.gameId);
      }, this.destroyDelayMs);
    }
  }

  // Explicit disposal to prevent memory leaks if room is replaced or manually terminated
  public dispose(): void {
    this.cleanupTimers();
    if (this.destroyTimer) {
      clearTimeout(this.destroyTimer);
      this.destroyTimer = null;
    }
    for (const socket of this.sockets) {
      socket.close(1000, 'Game room disposed');
    }
    this.sockets.clear();
  }
}
