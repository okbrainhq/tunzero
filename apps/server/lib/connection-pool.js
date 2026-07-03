// ConnectionPool — Server-side multipath/parallel connection manager
// Accepts multiple TLS connections from the same client, dedups inbound, and pins each outbound stream to one connection

const { encodeFrame, FrameType, CONTROL_FRAME_TYPES, DedupWindow } = require('../../../packages/frame-protocol');

const SEQ_RESET_THRESHOLD = 0xFFFFFF0F; // 2^32 - 1,000,000

// Grace period before cleaning up completed-stream dedup/seq state.
const STREAM_CLEANUP_GRACE_MS = 60000;

class ConnectionPool {
  constructor(options = {}) {
    this.connections = new Map(); // `${clientSerial}:${interfaceName}` -> socket
    this.dedupWindows = new Map(); // streamId -> DedupWindow
    this.seqCounters = new Map(); // streamId -> nextSeqNo
    this.cleanupTimers = new Map(); // streamId -> setTimeout handle (TTL cleanup)
    this.activeStreams = new Map(); // streamId -> { frameHandler, errorHandler }
    this.clientSerial = options.clientSerial ? String(options.clientSerial) : null; // Active authenticated client identity
    this.streamOutboundConnections = new Map(); // streamId -> connection key (server -> client single-flow)
    this.streamInboundConnections = new Map(); // streamId -> Set<socket> (client -> server)
    this.streamOptions = new Map(); // streamId -> { singleFlow }
    this.roundRobinCursor = 0;
  }

  /**
   * Register a new connection. Replaces existing one with same interface name.
   * Returns false when a different client identity is already active.
   */
  add(clientSerial, interfaceName, socket) {
    const serial = String(clientSerial);
    if (this.clientSerial && this.clientSerial !== serial) {
      return false;
    }

    this.clientSerial = serial;
    const key = `${serial}:${interfaceName}`;

    // Replace existing connection for this interface
    if (this.connections.has(key)) {
      const old = this.connections.get(key);
      this._forgetConnection(key, old);
      try { old.destroy(); } catch {}
    }
    this.connections.set(key, socket);
    return true;
  }

  /**
   * Remove a connection
   */
  remove(socket) {
    let removedKey = null;
    for (const [name, s] of this.connections) {
      if (s === socket) {
        removedKey = name;
        this.connections.delete(name);
        break;
      }
    }

    if (removedKey) {
      this._forgetConnection(removedKey, socket);
    }

    // If no connections left, clean up all streams
    if (this.connections.size === 0) {
      this.clientSerial = null;
      this._cleanupAllStreams();
    }
  }

  get count() {
    return this.connections.size;
  }

  /**
   * Process an incoming data frame from any connection.
   * Handles dedup and routes to stream handlers.
   * @returns {'new' | 'duplicate'}
   */
  onFrame(frame, socket = null) {
    // Control frames are handled per-connection, skip
    if (frame.streamId === 0 && CONTROL_FRAME_TYPES.has(frame.type)) {
      return 'new';
    }

    const streamId = frame.streamId;
    if (socket) this._markInboundConnection(streamId, socket);

    // Stream lifecycle — FIN/ERROR: dedup before delivery
    if (frame.type === FrameType.FIN || frame.type === FrameType.ERROR) {
      let window = this.dedupWindows.get(streamId);
      if (!window) {
        window = new DedupWindow(frame.seqNo);
        window.checkAndAdd(frame.seqNo);
        this.dedupWindows.set(streamId, window);
      } else {
        const result = window.checkAndAdd(frame.seqNo);
        if (result === 'duplicate') return 'duplicate';
      }
      // Keep dedupWindow for late multipath duplicates.
      // Do NOT delete seqCounters: the outbound seqNo must keep growing
      // across stream-ID reuses so the client's dedup window sees a
      // higher seqNo instead of treating the new stream as a duplicate.
      // Schedule TTL cleanup to bound memory for long-lived tunnels.
      this._scheduleStreamCleanup(streamId);
      this._routeToHandler(frame);
      return 'new';
    }

    // HEADERS / UPGRADE — dedup if window already exists
    if (frame.type === FrameType.HEADERS || frame.type === FrameType.UPGRADE) {
      let window = this.dedupWindows.get(streamId);
      if (!window) {
        window = new DedupWindow(frame.seqNo);
        this.dedupWindows.set(streamId, window);
      }
      if (window.checkAndAdd(frame.seqNo) === 'duplicate') return 'duplicate';
      this._cancelStreamCleanup(streamId); // Stream (re)started — cancel pending cleanup
      this._routeToHandler(frame);
      return 'new';
    }

    // DATA frames — dedup check
    let window = this.dedupWindows.get(streamId);
    if (!window) {
      window = new DedupWindow(frame.seqNo);
      window.checkAndAdd(frame.seqNo);
      this.dedupWindows.set(streamId, window);
      this._routeToHandler(frame);
      return 'new';
    }

    const result = window.checkAndAdd(frame.seqNo);
    if (result === 'duplicate') return 'duplicate';

    this._routeToHandler(frame);
    return 'new';
  }

  _routeToHandler(frame) {
    const handler = this.activeStreams.get(frame.streamId);
    if (!handler) return;

    if (frame.type === FrameType.ERROR && handler.errorHandler) {
      handler.errorHandler(new Error(frame.payload.toString()));
    } else if (handler.frameHandler) {
      handler.frameHandler(frame);
    }
  }

  /**
   * Send a frame to the client — assigns seqNo. Normal streams keep the
   * traditional multipath behavior (broadcast to every tunnel connection).
   * Streams marked singleFlow are pinned to one connection.
   */
  send(frameBuf) {
    const type = frameBuf.readUInt8(4);

    if (CONTROL_FRAME_TYPES.has(type)) {
      return this._writeToConnections(frameBuf, this._allLiveConnections());
    }

    const streamId = frameBuf.readUInt32BE(0);
    let seqNo = (this.seqCounters.get(streamId) || 0) + 1;

    if (seqNo > SEQ_RESET_THRESHOLD) {
      this._sendResetSeq(streamId);
      seqNo = 0;
    }

    this.seqCounters.set(streamId, seqNo);
    frameBuf.writeUInt32BE(seqNo, 5);

    // Outbound ERROR means the server is aborting the stream — both
    // directions are done. Schedule TTL cleanup.
    // (Outbound FIN is just end-of-request-body; the client's response
    // may still be active, so no cleanup here — it's scheduled when
    // the server receives the client's inbound FIN/ERROR in onFrame().)
    if (type === FrameType.ERROR) {
      this._scheduleStreamCleanup(streamId);
    }

    const targets = this._isSingleFlowStream(streamId)
      ? this._selectOutboundConnection(streamId)
      : this._allLiveConnections();

    return this._writeToConnections(frameBuf, targets);
  }

  onceDrain(callback) {
    this._onceDrainOnSockets(this._allLiveConnections().map(([, sock]) => sock), callback);
  }

  onceDrainForStream(streamId, callback) {
    const sockets = this._isSingleFlowStream(streamId) ? this._getOutboundSocketsForStream(streamId) : [];
    this._onceDrainOnSockets(sockets.length > 0 ? sockets : this._allLiveConnections().map(([, sock]) => sock), callback);
  }

  pause() {
    for (const sock of this.connections.values()) {
      if (!sock.destroyed && typeof sock.pause === 'function') sock.pause();
    }
  }

  resume() {
    for (const sock of this.connections.values()) {
      if (!sock.destroyed && typeof sock.resume === 'function') sock.resume();
    }
  }

  pauseStream(streamId) {
    const sockets = this._getInboundSocketsForStream(streamId);
    if (sockets.length === 0) return this.pause();
    for (const sock of sockets) {
      if (!sock.destroyed && typeof sock.pause === 'function') sock.pause();
    }
  }

  resumeStream(streamId) {
    const sockets = this._getInboundSocketsForStream(streamId);
    if (sockets.length === 0) return this.resume();
    for (const sock of sockets) {
      if (!sock.destroyed && typeof sock.resume === 'function') sock.resume();
    }
  }

  _allLiveConnections() {
    return [...this.connections.entries()].filter(([, sock]) => !sock.destroyed);
  }

  _writeToConnections(frameBuf, entries) {
    let connected = 0;
    let backpressured = false;

    for (const [, sock] of entries) {
      if (!sock.destroyed) {
        connected++;
        if (!sock.write(frameBuf)) backpressured = true;
      }
    }

    return connected > 0 && !backpressured;
  }

  setStreamMode(streamId, options = {}) {
    const current = this.streamOptions.get(streamId) || {};
    if (typeof options.singleFlow === 'boolean') {
      current.singleFlow = options.singleFlow;
    }

    if (current.singleFlow) {
      this.streamOptions.set(streamId, current);
    } else {
      this.streamOptions.delete(streamId);
      this.streamOutboundConnections.delete(streamId);
    }
  }

  clearStreamMode(streamId) {
    this.streamOptions.delete(streamId);
    this.streamOutboundConnections.delete(streamId);
    this.streamInboundConnections.delete(streamId);
  }

  _isSingleFlowStream(streamId) {
    return this.streamOptions.get(streamId)?.singleFlow === true;
  }

  _selectOutboundConnection(streamId) {
    const existingKey = this.streamOutboundConnections.get(streamId);
    const existing = existingKey ? this.connections.get(existingKey) : null;
    if (existing && !existing.destroyed) return [[existingKey, existing]];

    const live = this._allLiveConnections();
    if (live.length === 0) return [];

    const loadByKey = new Map();
    for (const key of this.streamOutboundConnections.values()) {
      if (this.connections.has(key)) loadByKey.set(key, (loadByKey.get(key) || 0) + 1);
    }

    let best = null;
    let bestScore = Infinity;
    const start = live.length > 0 ? this.roundRobinCursor % live.length : 0;

    for (let i = 0; i < live.length; i++) {
      const index = (start + i) % live.length;
      const [key, sock] = live[index];
      const load = loadByKey.get(key) || 0;
      const score = load + (sock.writableNeedDrain ? 1000000 : 0);
      if (score < bestScore) {
        bestScore = score;
        best = { key, sock, index };
      }
    }

    if (!best) return [];
    this.roundRobinCursor = (best.index + 1) % live.length;
    this.streamOutboundConnections.set(streamId, best.key);
    return [[best.key, best.sock]];
  }

  _getOutboundSocketsForStream(streamId) {
    const key = this.streamOutboundConnections.get(streamId);
    const sock = key ? this.connections.get(key) : null;
    return sock && !sock.destroyed ? [sock] : [];
  }

  _markInboundConnection(streamId, socket) {
    if (socket.destroyed) return;
    let sockets = this.streamInboundConnections.get(streamId);
    if (!sockets) {
      sockets = new Set();
      this.streamInboundConnections.set(streamId, sockets);
    }
    sockets.add(socket);
  }

  _getInboundSocketsForStream(streamId) {
    const sockets = this.streamInboundConnections.get(streamId);
    if (!sockets) return [];
    return [...sockets].filter(sock => !sock.destroyed);
  }

  _onceDrainOnSockets(sockets, callback) {
    const waiting = sockets.filter(sock => !sock.destroyed && sock.writableNeedDrain);

    if (waiting.length === 0) {
      process.nextTick(callback);
      return;
    }

    let pending = waiting.length;
    let callbackCalled = false;

    const finishOne = () => {
      pending--;
      if (pending <= 0 && !callbackCalled) {
        callbackCalled = true;
        callback();
      }
    };

    for (const sock of waiting) {
      let settled = false;
      const done = () => {
        if (settled) return;
        settled = true;
        sock.removeListener('drain', done);
        sock.removeListener('close', done);
        sock.removeListener('error', done);
        finishOne();
      };
      sock.once('drain', done);
      sock.once('close', done);
      sock.once('error', done);
    }
  }

  _forgetConnection(key, socket) {
    for (const [streamId, assignedKey] of this.streamOutboundConnections) {
      if (assignedKey === key) this.streamOutboundConnections.delete(streamId);
    }

    for (const [streamId, sockets] of this.streamInboundConnections) {
      sockets.delete(socket);
      if (sockets.size === 0) this.streamInboundConnections.delete(streamId);
    }
  }

  _sendResetSeq(streamId) {
    const frame = encodeFrame(0, FrameType.RESET_SEQ, JSON.stringify({
      streams: [streamId]
    }), 0);

    for (const [name, sock] of this.connections) {
      if (!sock.destroyed) {
        sock.write(frame);
      }
    }

    this.seqCounters.set(streamId, 0);
  }

  /**
   * Handle an incoming RESET_SEQ from the client.
   * Clears only dedup windows (incoming) — not outbound seqCounters.
   */
  handleResetSeq(frame) {
    try {
      const data = JSON.parse(frame.payload.toString());
      for (const streamId of data.streams) {
        this.dedupWindows.delete(streamId);
        this._cancelStreamCleanup(streamId); // Protect seqCounters from stale TTL timer
        // Do NOT reset seqCounters — RESET_SEQ from client means
        // "I reset my outbound", so we only clear our incoming dedup.
      }
    } catch {}
  }

  registerStream(streamId, handlers, options = {}) {
    this.activeStreams.set(streamId, handlers);
    if (Object.prototype.hasOwnProperty.call(options, 'singleFlow')) {
      this.setStreamMode(streamId, { singleFlow: Boolean(options.singleFlow) });
    }
  }

  unregisterStream(streamId) {
    this.activeStreams.delete(streamId);
    this.clearStreamMode(streamId);
  }

  getStreamHandler(streamId) {
    return this.activeStreams.get(streamId) || null;
  }

  _cleanupAllStreams() {
    for (const [streamId, handlers] of this.activeStreams) {
      if (handlers.errorHandler) {
        handlers.errorHandler(new Error('Client disconnected'));
      }
    }
    this.activeStreams.clear();
    for (const timer of this.cleanupTimers.values()) clearTimeout(timer);
    this.cleanupTimers.clear();
    this.dedupWindows.clear();
    this.seqCounters.clear();
    this.streamOutboundConnections.clear();
    this.streamInboundConnections.clear();
    this.streamOptions.clear();
  }

  /**
   * Schedule TTL cleanup of dedup/seq state for a completed stream.
   * The grace period catches late multipath duplicates while bounding memory.
   */
  _scheduleStreamCleanup(streamId) {
    this._cancelStreamCleanup(streamId);
    const timer = setTimeout(() => {
      this.cleanupTimers.delete(streamId);
      this.dedupWindows.delete(streamId);
      this.seqCounters.delete(streamId);
      this.streamOutboundConnections.delete(streamId);
      this.streamInboundConnections.delete(streamId);
      this.streamOptions.delete(streamId);
    }, STREAM_CLEANUP_GRACE_MS);
    if (typeof timer.unref === 'function') timer.unref();
    this.cleanupTimers.set(streamId, timer);
  }

  _cancelStreamCleanup(streamId) {
    const timer = this.cleanupTimers.get(streamId);
    if (timer) {
      clearTimeout(timer);
      this.cleanupTimers.delete(streamId);
    }
  }
}

module.exports = { ConnectionPool, SEQ_RESET_THRESHOLD };
