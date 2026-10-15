import { DbSyncTask } from '../types.js';

export type PersistenceHandler = (gameId: string, task: DbSyncTask) => Promise<void>;

/**
 * Sequential In-Memory FIFO Queue
 * Guarantees Move N is strictly persisted to the Mumbai database before Move N+1 begins,
 * preventing any out-of-order writes across the Ohio <-> Mumbai ~200ms link.
 */
export class DbSyncQueue {
  private queue: DbSyncTask[] = [];
  private isFlushing = false;
  private gameId: string;
  private persistHandler: PersistenceHandler;
  private onQueueEmptyCallback?: () => void;

  constructor(
    gameId: string,
    persistHandler: PersistenceHandler,
    onQueueEmpty?: () => void
  ) {
    this.gameId = gameId;
    this.persistHandler = persistHandler;
    this.onQueueEmptyCallback = onQueueEmpty;
  }

  public enqueue(task: DbSyncTask): void {
    this.queue.push(task);
    this.flush();
  }

  public get pendingCount(): number {
    return this.queue.length;
  }

  public get isIdle(): boolean {
    return this.queue.length === 0 && !this.isFlushing;
  }

  private async flush(): Promise<void> {
    if (this.isFlushing || this.queue.length === 0) return;
    this.isFlushing = true;

    while (this.queue.length > 0) {
      const task = this.queue[0];
      try {
        await this.persistHandler(this.gameId, task);
        // Only dequeue upon successful write
        this.queue.shift();
      } catch (error) {
        console.error(`[DbSyncQueue] Failed to persist task for game ${this.gameId} (ply ${task.ply}):`, error);
        // Pause 1 second before retrying to prevent hammering Mumbai DB on network flap
        setTimeout(() => {
          this.isFlushing = false;
          this.flush();
        }, 1000);
        return;
      }
    }

    this.isFlushing = false;
    if (this.queue.length === 0) {
      this.onQueueEmptyCallback?.();
    }
  }
}
