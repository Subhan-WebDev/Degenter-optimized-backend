// common/batch.js
// Robust batch queue with size/time flush, serialized flushes, and proper drain.

export class BatchQueue {
  constructor({ maxItems = 500, maxWaitMs = 500, flushFn }) {
    if (typeof flushFn !== 'function') throw new Error('flushFn required');

    this.maxItems = Number(maxItems) || 500;
    this.maxWaitMs = Number(maxWaitMs) || 500;
    this.flushFn = flushFn;

    this.queue = [];
    this.timer = null;
    this.flushing = false;
    this.pendingFlush = Promise.resolve(); // serialize all flushes
  }

  _startTimer() {
    if (this.timer || this.maxWaitMs <= 0) return;
    this.timer = setTimeout(() => {
      this.timer = null;
      this.pendingFlush = this.pendingFlush.then(() => this._flush()).catch(() => {});
    }, this.maxWaitMs);
    // don't hold the event loop open
    if (typeof this.timer.unref === 'function') this.timer.unref();
  }

  _stopTimer() {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }

  push(item) {
    this.queue.push(item);

    // size-based flush
    if (this.queue.length >= this.maxItems) {
      this._stopTimer();
      this.pendingFlush = this.pendingFlush.then(() => this._flush()).catch(() => {});
      return;
    }

    // time-based flush
    this._startTimer();
  }

  async _flush() {
    if (this.flushing) return;
    if (!this.queue.length) return;

    this.flushing = true;
    this._stopTimer();

    const items = this.queue;
    this.queue = [];

    try {
      await this.flushFn(items);
    } finally {
      this.flushing = false;

      // if items arrived during flush, chain another flush
      if (this.queue.length) {
        queueMicrotask(() => {
          this.pendingFlush = this.pendingFlush.then(() => this._flush()).catch(() => {});
        });
      }
    }
  }

  async drain() {
    this._stopTimer();
    if (this.queue.length) {
      this.pendingFlush = this.pendingFlush.then(() => this._flush());
    }
    await this.pendingFlush;
  }
}

export default BatchQueue;
