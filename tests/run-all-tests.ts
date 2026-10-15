import { Chess } from 'chess.js';
import { GameRoom } from '../src/engine/GameRoom.js';
import { DbSyncQueue } from '../src/sync/DbSyncQueue.js';
import { GameInitConfig, DbSyncTask, getRoomKey } from '../src/types.js';

let passed = 0;
let failed = 0;

function assert(condition: boolean, testName: string, details?: string) {
  if (condition) {
    console.log(`  [PASS] ${testName}`);
    passed++;
  } else {
    console.error(`  [FAIL] ${testName} - ${details || 'Assertion failed'}`);
    failed++;
  }
}

async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ============================================================================
// TEST 1: Server-Side Zero-Latency Premoves (Lichess Style)
// ============================================================================
async function testServerSidePremove() {
  console.log('\n=== TEST 1: Server-Side Zero-Latency Premove ===');
  
  const savedTasks: DbSyncTask[] = [];
  const room = new GameRoom(
    {
      gameId: 'test-premove-1',
      whitePlayerId: 'white-1',
      blackPlayerId: 'black-1',
      timeControl: '5+0',
      baseTimeMs: 300_000,
      incrementMs: 0,
    },
    async (_id, task) => { savedTasks.push(task); },
    () => {}
  );

  const mockWhiteSocket: any = {
    role: 'w',
    readyState: 1,
    send: (msg: string) => {},
  };
  const mockBlackSocket: any = {
    role: 'b',
    readyState: 1,
    send: (msg: string) => {},
  };

  room.attachSocket(mockWhiteSocket);
  room.attachSocket(mockBlackSocket);

  // Black queues premove: e7 -> e5 while it is White's turn
  room.handlePremove(mockBlackSocket, 'e7', 'e5');

  // White plays e2 -> e4
  room.handleMove(mockWhiteSocket, 'e2', 'e4');
  await sleep(10);

  // The moment White's move succeeded, the server must have executed Black's premove instantly!
  assert(room.movesCount === 2, 'Premove executed automatically in RAM on same tick');
  assert(savedTasks.length === 2, 'Both White move and Black premove pushed to DB queue');
  assert(savedTasks[0].san === 'e4', 'Move 1 is e4');
  assert(savedTasks[1].san === 'e5', 'Move 2 is e5 (premove)');
  assert(savedTasks[1].whiteTimeLeft === 300_000, 'White clock intact');
  assert(savedTasks[1].blackTimeLeft === 300_000, 'Black premove deducted 0ms');
}

// ============================================================================
// TEST 2: Exact Millisecond Timeout Enforcement
// ============================================================================
async function testExactMillisecondTimeout() {
  console.log('\n=== TEST 2: Exact Millisecond Timeout Enforcement ===');

  let gameOverReason = '';
  const room = new GameRoom(
    {
      gameId: 'test-timeout-1',
      whitePlayerId: 'white-2',
      blackPlayerId: 'black-2',
      timeControl: '0.005+0',
      baseTimeMs: 400, // 400ms base time
      incrementMs: 0,
    },
    async () => {},
    () => {}
  );

  const mockWhiteSocket: any = {
    role: 'w',
    readyState: 1,
    send: (msg: string) => {
      const parsed = JSON.parse(msg);
      if (parsed.type === 'game_over') {
        gameOverReason = parsed.reason;
      }
    },
  };
  room.attachSocket(mockWhiteSocket);

  // Fast forward past move 1 grace to start clock ticking
  (room as any).isGracePeriod = false;
  (room as any).turnStartHr = process.hrtime.bigint();
  (room as any).armExactFlagTimer();

  const startMs = Date.now();
  await sleep(450); // wait for 400ms flag timer

  const elapsed = Date.now() - startMs;
  assert(room.status === 'completed', 'Game auto-completed upon exact timeout');
  assert(room.result === '0-1', 'White flagged, Black won (0-1)');
  assert(gameOverReason.includes('White ran out of time'), `Timeout reason recorded: "${gameOverReason}"`);
  assert(elapsed >= 400 && elapsed < 550, `Flag triggered at ${elapsed}ms (exact millisecond precision)`);
}

// ============================================================================
// TEST 3: Out-of-Order FIFO DB Sync Queue (Cross-Continental Latency)
// ============================================================================
async function testFifoSyncQueue() {
  console.log('\n=== TEST 3: FIFO Sequential DB Queue ===');

  const persistedOrder: number[] = [];
  const queue = new DbSyncQueue(
    'test-fifo-1',
    async (_id, task) => {
      // Simulate 50ms Mumbai network latency
      await sleep(50);
      persistedOrder.push(task.ply);
    }
  );

  // Rapidly enqueue 5 moves without waiting
  for (let i = 1; i <= 5; i++) {
    queue.enqueue({
      ply: i,
      san: `move-${i}`,
      from: 'a2',
      to: 'a3',
      fen: 'fen',
      whiteTimeLeft: 300_000,
      blackTimeLeft: 300_000,
      timestamp: Date.now(),
      isGameOver: false,
    });
  }

  // Wait for queue to flush
  while (!queue.isIdle) {
    await sleep(20);
  }

  assert(persistedOrder.length === 5, 'All 5 tasks persisted');
  assert(JSON.stringify(persistedOrder) === JSON.stringify([1, 2, 3, 4, 5]), 'Tasks persisted strictly in sequential order 1..5');
}

// ============================================================================
// TEST 4: Memory Leak Cleanup (Garbage Collection)
// ============================================================================
async function testGarbageCollection() {
  console.log('\n=== TEST 4: Memory Leak Cleanup (Garbage Collection) ===');

  let roomDestroyed = false;
  const room = new GameRoom(
    {
      gameId: 'test-gc-1',
      whitePlayerId: 'white-4',
      blackPlayerId: 'black-4',
      timeControl: '5+0',
      baseTimeMs: 300_000,
      incrementMs: 0,
      destroyDelayMs: 40,
    },
    async () => {},
    (destroyedId) => {
      if (destroyedId === 'test-gc-1') {
        roomDestroyed = true;
      }
    }
  );

  // Trigger resignation
  const mockWhiteSocket: any = { role: 'w', readyState: 1, send: () => {}, close: () => {} };
  room.attachSocket(mockWhiteSocket);
  room.handleResign(mockWhiteSocket);

  assert(room.status === 'completed', 'Room marked completed on resignation');

  await sleep(80);
  assert(roomDestroyed, 'Room purged from memory after completion and queue empty');
}

// ============================================================================
// TEST 5: Illegal Move & Illegal Premove Clock Integrity
// ============================================================================
async function testIllegalMoveAndPremoveIntegrity() {
  console.log('\n=== TEST 5: Illegal Move & Illegal Premove Clock Integrity ===');

  let lastErrorMessage = '';
  let lastErrorFen = '';

  const room = new GameRoom(
    {
      gameId: 'test-illegal-1',
      whitePlayerId: 'white-5',
      blackPlayerId: 'black-5',
      timeControl: '5+2',
      baseTimeMs: 300_000,
      incrementMs: 2_000, // 2s increment
      initialPly: 2, // Past grace period so clock deductions/increments apply
    },
    async () => {},
    () => {}
  );

  const mockWhiteSocket: any = {
    role: 'w',
    readyState: 1,
    send: (raw: string) => {
      const msg = JSON.parse(raw);
      if (msg.type === 'error') {
        lastErrorMessage = msg.message;
        lastErrorFen = msg.fen;
      }
    },
  };
  const mockBlackSocket: any = {
    role: 'b',
    readyState: 1,
    send: (raw: string) => {},
  };

  room.attachSocket(mockWhiteSocket);
  room.attachSocket(mockBlackSocket);

  // Initial White remaining ms
  const initialWhiteMs = room.whiteRemainingMs;

  // 1. White attempts an ILLEGAL move: e2 -> e5 (pawns cannot move 3 squares)
  await sleep(50); // let 50ms elapse
  room.handleMove(mockWhiteSocket, 'e2', 'e5');

  assert(lastErrorMessage === 'Illegal move', 'Illegal move rejected with error message');
  assert(Boolean(lastErrorFen), 'Current board FEN returned to client for visual piece snapback');
  assert(room.movesCount === 2, 'Ply count not advanced on illegal move');
  assert(room.whiteRemainingMs === initialWhiteMs, 'White clock NOT mutated on illegal move (no deduction, no increment)');

  // Now White makes a LEGAL move: e2 -> e4
  room.handleMove(mockWhiteSocket, 'e2', 'e4');
  assert(room.movesCount === 3, 'Legal move accepted');
  assert(room.whiteRemainingMs > initialWhiteMs, 'Clock correctly deducted once and received increment on legal move');

  // Now Black plays a legal move: e7 -> e5
  room.handleMove(mockBlackSocket, 'e7', 'e5');
  assert(room.movesCount === 4, 'Black legal move accepted');

  // 2. Now it is White's turn. Black queues an ILLEGAL premove: e8 -> e1 (King jumps across board)
  const blackMsBeforePremove = room.blackRemainingMs;
  room.handlePremove(mockBlackSocket, 'e8', 'e1');

  // White plays d2 -> d4
  room.handleMove(mockWhiteSocket, 'd2', 'd4');

  // White move executed (ply 5). Server attempted to execute Black's premove e8 -> e1 in new position, which is ILLEGAL.
  // It must fail silently, not advance ply to 6, and NOT add +2000ms increment to Black!
  assert(room.movesCount === 5, 'White move executed (ply 5), illegal premove skipped');
  assert(room.blackRemainingMs === blackMsBeforePremove, 'Black did NOT receive free +2000ms increment for failed premove');
}

// ============================================================================
// TEST 6: Resign Clock Capture, FEN Ply Calculation & Reconnect Premove Sync
// ============================================================================
async function testEdgeCases() {
  console.log('\n=== TEST 6: Resign Clock, FEN Ply & Reconnect Premove Sync ===');

  // 1. Resign Clock Capture: Verify remaining time is captured at moment of resignation
  const resignRoom = new GameRoom(
    {
      gameId: 'test-resign-1',
      whitePlayerId: 'w-res',
      blackPlayerId: 'b-res',
      timeControl: '5+0',
      baseTimeMs: 100_000,
      incrementMs: 0,
      initialPly: 2, // past grace period so clock ticks down
    },
    async () => {},
    () => {}
  );

  const mockWhite: any = { role: 'w', readyState: 1, send: () => {}, close: () => {} };
  resignRoom.attachSocket(mockWhite);

  await sleep(100); // 100ms elapses on White's turn
  resignRoom.handleResign(mockWhite);

  assert(resignRoom.status === 'completed', 'Room marked completed on resign');
  assert(resignRoom.result === '0-1', 'White resigned, Black wins (0-1)');
  assert(
    resignRoom.whiteRemainingMs < 100_000 && resignRoom.whiteRemainingMs > 90_000,
    `White clock captured live at resignation: ${resignRoom.whiteRemainingMs}ms (< 100000ms)`
  );
  assert(resignRoom.blackRemainingMs === 100_000, 'Black clock untouched (100000ms)');

  // 2. Custom FEN Ply Detection: Parse fullmove 4, turn 'w' => ply 6
  const midgameFenWhite = 'r1bqkb1r/pppp1ppp/2n2n2/4p3/4P3/2N2N2/PPPP1PPP/R1BQKB1R w KQkq - 4 4';
  const customFenRoomW = new GameRoom(
    {
      gameId: 'test-fen-w',
      whitePlayerId: 'w-fen',
      blackPlayerId: 'b-fen',
      timeControl: '5+0',
      baseTimeMs: 300_000,
      incrementMs: 0,
      startingFen: midgameFenWhite,
    },
    async () => {},
    () => {}
  );
  assert(customFenRoomW.movesCount === 6, `Custom FEN fullmove 4 turn 'w' parsed to ply 6 (actual: ${customFenRoomW.movesCount})`);
  assert((customFenRoomW as any).isGracePeriod === false, 'Grace period bypassed for midgame FEN');

  // Parse fullmove 4, turn 'b' => ply 7
  const midgameFenBlack = 'r1bqkb1r/pppp1ppp/2n2n2/4p3/4P3/2N2N2/PPPP1PPP/R1BQKB1R b KQkq - 5 4';
  const customFenRoomB = new GameRoom(
    {
      gameId: 'test-fen-b',
      whitePlayerId: 'w-fen-b',
      blackPlayerId: 'b-fen-b',
      timeControl: '5+0',
      baseTimeMs: 300_000,
      incrementMs: 0,
      startingFen: midgameFenBlack,
    },
    async () => {},
    () => {}
  );
  assert(customFenRoomB.movesCount === 7, `Custom FEN fullmove 4 turn 'b' parsed to ply 7 (actual: ${customFenRoomB.movesCount})`);

  // 3. Reconnect Premove Sync: Queued premove restored immediately on new socket attach
  const reconnectRoom = new GameRoom(
    {
      gameId: 'test-reconnect-1',
      whitePlayerId: 'w-rec',
      blackPlayerId: 'b-rec',
      timeControl: '5+0',
      baseTimeMs: 300_000,
      incrementMs: 0,
    },
    async () => {},
    () => {}
  );

  const blackSocket1: any = { role: 'b', readyState: 1, send: () => {} };
  reconnectRoom.attachSocket(blackSocket1);
  reconnectRoom.handlePremove(blackSocket1, 'g8', 'f6');

  // Player refreshes browser: new socket connects as 'b'
  let receivedPremoveOnReconnect = false;
  let reconnectedPremove: any = null;
  const blackSocket2: any = {
    role: 'b',
    readyState: 1,
    send: (raw: string) => {
      const msg = JSON.parse(raw);
      if (msg.type === 'premove_status' && msg.queued) {
        receivedPremoveOnReconnect = true;
        reconnectedPremove = msg.premove;
      }
    },
  };

  reconnectRoom.attachSocket(blackSocket2);
  assert(receivedPremoveOnReconnect, 'Premove status automatically synced to reconnected socket');
  assert(reconnectedPremove?.from === 'g8' && reconnectedPremove?.to === 'f6', 'Restored exact premove coordinates (g8->f6)');
}

// ============================================================================
// TEST 7: Draw Negotiation Protocol (Lichess Style)
// ============================================================================
async function testDrawProtocol() {
  console.log('\n=== TEST 7: Draw Negotiation Protocol (Lichess Style) ===');

  // 1. Offer and Decline
  let lastServerMsgWhite: any = null;
  let lastServerMsgBlack: any = null;
  const mockWhite: any = {
    role: 'w',
    readyState: 1,
    send: (raw: string) => { lastServerMsgWhite = JSON.parse(raw); }
  };
  const mockBlack: any = {
    role: 'b',
    readyState: 1,
    send: (raw: string) => { lastServerMsgBlack = JSON.parse(raw); }
  };

  const room = new GameRoom(
    {
      gameId: 'test-draw-1',
      whitePlayerId: 'w-draw',
      blackPlayerId: 'b-draw',
      timeControl: '5+0',
      baseTimeMs: 300_000,
      incrementMs: 0,
    },
    async () => {},
    () => {}
  );
  room.attachSocket(mockWhite);
  room.attachSocket(mockBlack);

  // White offers draw
  room.handleOfferDraw(mockWhite);
  assert(room.drawOffer === 'w', 'White draw offer registered in RAM');
  assert(lastServerMsgBlack?.type === 'draw_offer' && lastServerMsgBlack?.offeredBy === 'w', 'Black notified of White draw offer');

  // Black declines draw
  room.handleDeclineDraw(mockBlack);
  assert(room.drawOffer === null, 'Draw offer cleared after decline');
  assert(lastServerMsgWhite?.type === 'draw_declined', 'White notified draw declined');

  // 2. Offer and Accept
  let queueDrawTask: DbSyncTask | null = null;
  const roomAccept = new GameRoom(
    {
      gameId: 'test-draw-accept',
      whitePlayerId: 'w-draw-2',
      blackPlayerId: 'b-draw-2',
      timeControl: '5+0',
      baseTimeMs: 300_000,
      incrementMs: 0,
    },
    async (_id, task) => { if (task.san === 'DRAW') queueDrawTask = task; },
    () => {}
  );
  roomAccept.attachSocket(mockWhite);
  roomAccept.attachSocket(mockBlack);

  roomAccept.handleOfferDraw(mockWhite);
  roomAccept.handleAcceptDraw(mockBlack);
  assert(roomAccept.status === 'completed', 'Game completed upon draw acceptance');
  assert(roomAccept.result === '1/2-1/2', 'Result recorded as 1/2-1/2');
  assert(roomAccept.endReason === 'Draw by agreement', 'End reason is Draw by agreement');
  assert(lastServerMsgBlack?.type === 'game_over' && lastServerMsgBlack?.winner === 'draw', 'Game over broadcast with winner: draw');
  assert(queueDrawTask !== null && (queueDrawTask as any)?.san === 'DRAW', 'DRAW event enqueued to DB sync queue');

  // 3. Mutual Offer Auto-Accept
  const roomMutual = new GameRoom(
    {
      gameId: 'test-draw-mutual',
      whitePlayerId: 'w-draw-3',
      blackPlayerId: 'b-draw-3',
      timeControl: '5+0',
      baseTimeMs: 300_000,
      incrementMs: 0,
    },
    async () => {},
    () => {}
  );
  roomMutual.attachSocket(mockWhite);
  roomMutual.attachSocket(mockBlack);

  roomMutual.handleOfferDraw(mockWhite);
  // Black also offers draw -> triggers mutual auto-accept
  roomMutual.handleOfferDraw(mockBlack);
  assert(roomMutual.status === 'completed', 'Mutual draw offers auto-accept immediately');
  assert(roomMutual.result === '1/2-1/2', 'Mutual draw results in 1/2-1/2');

  // 4. Cancel Draw Offer on Next Move
  const roomMoveCancel = new GameRoom(
    {
      gameId: 'test-draw-cancel',
      whitePlayerId: 'w-draw-4',
      blackPlayerId: 'b-draw-4',
      timeControl: '5+0',
      baseTimeMs: 300_000,
      incrementMs: 0,
    },
    async () => {},
    () => {}
  );
  roomMoveCancel.attachSocket(mockWhite);
  roomMoveCancel.attachSocket(mockBlack);

  roomMoveCancel.handleOfferDraw(mockWhite);
  assert(roomMoveCancel.drawOffer === 'w', 'Draw offered before move');

  // White makes a move -> should auto-cancel draw offer
  roomMoveCancel.handleMove(mockWhite, 'e2', 'e4');
  assert(roomMoveCancel.drawOffer === null, 'Draw offer auto-cancelled after move is played');
}

// ============================================================================
// TEST 8: Move 1 Timeout Abort vs Tournament Forfeit
// ============================================================================
async function testMove1TimeoutAbortVsForfeit() {
  console.log('\n=== TEST 8: Move 1 Timeout Abort vs Tournament Forfeit ===');

  // Case A: Casual / Non-Tournament game -> Aborts on Move 1 timeout (result = '*', winner = null)
  let casualGameOver: any = null;
  const casualWhite: any = {
    role: 'w',
    readyState: 1,
    send: (raw: string) => {
      const parsed = JSON.parse(raw);
      if (parsed.type === 'game_over') casualGameOver = parsed;
    }
  };
  const casualRoom = new GameRoom(
    {
      gameId: 'test-casual-abort',
      whitePlayerId: 'w-casual',
      blackPlayerId: 'b-casual',
      timeControl: '10+0',
      baseTimeMs: 600_000,
      incrementMs: 0,
      initialGraceRemainingMs: 50, // 50ms grace period for fast testing
      isTournament: false,
    },
    async () => {},
    () => {}
  );
  casualRoom.attachSocket(casualWhite);

  await sleep(90); // Wait for 50ms grace timeout
  assert(casualRoom.status === 'completed', 'Casual game completed on Move 1 timeout');
  assert(casualRoom.result === '*', `Casual game aborted with result '*' (actual: ${casualRoom.result})`);
  assert(casualRoom.endReason.includes('aborted'), `End reason specifies abort: "${casualRoom.endReason}"`);
  assert(casualGameOver?.winner === null, 'Casual abort has winner = null');

  // Case B: Tournament game -> Forfeits on Move 1 timeout (result = '0-1', winner = 'b')
  let tourneyGameOver: any = null;
  const tourneyWhite: any = {
    role: 'w',
    readyState: 1,
    send: (raw: string) => {
      const parsed = JSON.parse(raw);
      if (parsed.type === 'game_over') tourneyGameOver = parsed;
    }
  };
  const tourneyRoom = new GameRoom(
    {
      gameId: 'test-tourney-forfeit',
      whitePlayerId: 'w-tourney',
      blackPlayerId: 'b-tourney',
      timeControl: '10+0',
      baseTimeMs: 600_000,
      incrementMs: 0,
      initialGraceRemainingMs: 50, // 50ms grace period for fast testing
      isTournament: true,
    },
    async () => {},
    () => {}
  );
  tourneyRoom.attachSocket(tourneyWhite);

  await sleep(90); // Wait for 50ms grace timeout
  assert(tourneyRoom.status === 'completed', 'Tournament game completed on Move 1 timeout');
  assert(tourneyRoom.result === '0-1', `Tournament game forfeits with result '0-1' (actual: ${tourneyRoom.result})`);
  assert(tourneyRoom.endReason.includes('forfeited'), `End reason specifies forfeit: "${tourneyRoom.endReason}"`);
  assert(tourneyGameOver?.winner === 'b', 'Tournament forfeit awards win to Black (winner: "b")');
}

// ============================================================================
// TEST 9: External Termination (forceTerminate)
// ============================================================================
async function testForceTerminate() {
  console.log('\n=== TEST 9: External Termination (forceTerminate) ===');

  let gameOverMsg: any = null;
  const mockWhite: any = {
    role: 'w',
    readyState: 1,
    send: (raw: string) => {
      const parsed = JSON.parse(raw);
      if (parsed.type === 'game_over') gameOverMsg = parsed;
    }
  };

  const room = new GameRoom(
    {
      gameId: 'test-force-terminate',
      whitePlayerId: 'w-term',
      blackPlayerId: 'b-term',
      timeControl: '5+0',
      baseTimeMs: 300_000,
      incrementMs: 0,
    },
    async () => {},
    () => {}
  );
  room.attachSocket(mockWhite);

  // Coach concludes match externally
  room.forceTerminate('1-0', 'Simul coach awarded victory');

  assert(room.status === 'completed', 'Room marked completed on forceTerminate');
  assert(room.result === '1-0', 'Result set to 1-0');
  assert(room.endReason === 'Simul coach awarded victory', 'End reason matches provided string');
  assert(gameOverMsg?.type === 'game_over', 'Broadcast game_over sent to sockets');
  assert(gameOverMsg?.winner === 'w', 'Winner set to w for 1-0 termination');
}

// ============================================================================
// TEST 10: DbSyncTask Payload Enrichment (pgn, color, simulId)
// ============================================================================
async function testDbSyncPayloadEnrichment() {
  console.log('\n=== TEST 10: DbSyncTask Payload Enrichment ===');

  const syncedTasks: DbSyncTask[] = [];
  const room = new GameRoom(
    {
      gameId: 'test-sync-enrich',
      simulId: 'simul_xyz_789',
      whitePlayerId: 'w-enrich',
      blackPlayerId: 'b-enrich',
      timeControl: '5+0',
      baseTimeMs: 300_000,
      incrementMs: 0,
    },
    async (_id, task) => { syncedTasks.push(task); },
    () => {}
  );

  const mockWhite: any = { role: 'w', readyState: 1, send: () => {} };
  const mockBlack: any = { role: 'b', readyState: 1, send: () => {} };
  room.attachSocket(mockWhite);
  room.attachSocket(mockBlack);

  // White plays e4
  room.handleMove(mockWhite, 'e2', 'e4');
  // Black plays e5
  room.handleMove(mockBlack, 'e7', 'e5');

  await sleep(20);

  assert(syncedTasks.length === 2, '2 moves synced');
  assert(syncedTasks[0].color === 'w', 'Task 1 color is w');
  assert(syncedTasks[0].simulId === 'simul_xyz_789', 'Task 1 includes simulId');
  assert(typeof syncedTasks[0].pgn === 'string' && syncedTasks[0].pgn.includes('1. e4'), `Task 1 includes valid PGN: "${syncedTasks[0].pgn}"`);

  assert(syncedTasks[1].color === 'b', 'Task 2 color is b');
  assert(syncedTasks[1].simulId === 'simul_xyz_789', 'Task 2 includes simulId');
  assert(typeof syncedTasks[1].pgn === 'string' && syncedTasks[1].pgn.includes('1. e4 e5'), `Task 2 includes updated PGN: "${syncedTasks[1].pgn}"`);
}

// ============================================================================
// TEST 11: Simul Room Key Routing & Isolation
// ============================================================================
async function testSimulRoomKeyRouting() {
  console.log('\n=== TEST 11: Simul Room Key Routing & Isolation ===');

  const normalKey = getRoomKey('game_123');
  assert(normalKey === 'game_123', `Normal game key matches: "${normalKey}"`);

  const simulKey = getRoomKey('game_123', 'simul_456');
  assert(simulKey === 'simul_simul_456_game_123', `Simul game key partitioned: "${simulKey}"`);

  const simulKeyNull = getRoomKey('game_123', null);
  assert(simulKeyNull === 'game_123', 'Null simulId falls back to normal gameId');

  // Verify room key isolation in practice:
  // Two different simuls can have a board with the same board index (e.g. "board_1")
  const keySimulA = getRoomKey('board_1', 'simul_A');
  const keySimulB = getRoomKey('board_1', 'simul_B');
  assert(keySimulA !== keySimulB, 'Simul A and Simul B have distinct room keys for board_1');
}

// ============================================================================
// Runner
// ============================================================================
async function runAll() {
  console.log('========================================================');
  console.log('  CHESS CLOCK MICROSERVICE TEST SUITE (c:\\chessrepo-clock)');
  console.log('========================================================');

  await testServerSidePremove();
  await testExactMillisecondTimeout();
  await testFifoSyncQueue();
  await testGarbageCollection();
  await testIllegalMoveAndPremoveIntegrity();
  await testEdgeCases();
  await testDrawProtocol();
  await testMove1TimeoutAbortVsForfeit();
  await testForceTerminate();
  await testDbSyncPayloadEnrichment();
  await testSimulRoomKeyRouting();

  console.log('\n========================================================');
  console.log(`  RESULTS: ${passed} PASSED, ${failed} FAILED`);
  console.log('========================================================');

  if (failed > 0) {
    process.exit(1);
  } else {
    process.exit(0);
  }
}

runAll().catch((err) => {
  console.error('Fatal test error:', err);
  process.exit(1);
});

