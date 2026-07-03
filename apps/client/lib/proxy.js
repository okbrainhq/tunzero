// Proxy - HTTP proxy to local target service
// Works with both single RealSocket and multipath VirtualSocket

const { request, Agent } = require('node:http');
const { encodeFrame, FrameType, MAX_FRAME_SIZE } = require('../../../packages/frame-protocol');

const HOP_BY_HOP_HEADERS = new Set([
  'connection',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'te',
  'trailer',
  'transfer-encoding',
  'upgrade',
  'content-length'
]);

const X_FORWARDED_HEADERS = new Set([
  'x-forwarded-for',
  'x-forwarded-host',
  'x-forwarded-proto',
  'x-forwarded-port',
  'x-forwarded-path',
  'forwarded'
]);

function filterRequestHeaders(headers) {
  const filtered = {};
  for (const [key, value] of Object.entries(headers)) {
    const lowerKey = key.toLowerCase();
    if (!HOP_BY_HOP_HEADERS.has(lowerKey) && !X_FORWARDED_HEADERS.has(lowerKey)) {
      filtered[key] = value;
    }
  }
  return filtered;
}

const WS_HOP_BY_HOP_HEADERS = new Set([
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'te',
  'trailer',
  'transfer-encoding',
  'content-length'
]);

function filterWebSocketHeaders(headers) {
  const filtered = {};
  for (const [key, value] of Object.entries(headers)) {
    const lowerKey = key.toLowerCase();
    if (!WS_HOP_BY_HOP_HEADERS.has(lowerKey) && !X_FORWARDED_HEADERS.has(lowerKey)) {
      filtered[key] = value;
    }
  }
  return filtered;
}

const MAX_WS_BUFFER_SIZE = 16 * 1024 * 1024;
const DEFAULT_TARGET_TIMEOUT = 30000;
const DEFAULT_TARGET_KEEPALIVE_TIMEOUT = 60 * 60 * 1000;
const LOG_VALUE_MAX_LENGTH = 200;

function normalizeTargetTimeout(value) {
  if (value === undefined || value === null) return DEFAULT_TARGET_TIMEOUT;
  const timeout = Number(value);
  if (!Number.isFinite(timeout) || timeout < 0) return DEFAULT_TARGET_TIMEOUT;
  return timeout;
}

function normalizeNonNegativeInteger(value, fallback) {
  if (value === undefined || value === null) return fallback;
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) return fallback;
  return Math.floor(parsed);
}

function truncateForLog(value, maxLength = LOG_VALUE_MAX_LENGTH) {
  const str = String(value ?? '');
  return str.length > maxLength ? `${str.slice(0, maxLength)}…` : str;
}

function createTimingLog(scope, details = {}, initialMark = 'tunnel_request_received') {
  const startedAt = process.hrtime.bigint();
  const marks = new Map([[initialMark, 0]]);
  let logged = false;

  function elapsedMs() {
    return Number(process.hrtime.bigint() - startedAt) / 1e6;
  }

  function mark(name) {
    if (!marks.has(name)) marks.set(name, elapsedMs());
  }

  function log(outcome = 'done') {
    if (logged) return;
    logged = true;
    mark('total');

    const detailText = Object.entries(details)
      .map(([key, value]) => `${key}=${JSON.stringify(truncateForLog(value))}`)
      .join(' ');
    const markText = [...marks.entries()]
      .map(([key, ms]) => `${key}=${ms.toFixed(1)}ms`)
      .join(' ');

    console.log(`[TIMING] ${scope} outcome=${outcome} ${detailText} ${markText}`);
  }

  return { mark, log };
}

function waitForConnectionDrain(connection, callback, streamId = null) {
  if (streamId !== null && connection && typeof connection.onceDrainForStream === 'function') {
    connection.onceDrainForStream(streamId, callback);
    return;
  }

  if (connection && typeof connection.onceDrain === 'function') {
    connection.onceDrain(callback);
    return;
  }

  const socket = connection?.socket;
  if (!socket || socket.destroyed || !socket.writableNeedDrain) {
    process.nextTick(callback);
    return;
  }

  let settled = false;
  const done = () => {
    if (settled) return;
    settled = true;
    socket.removeListener('drain', done);
    socket.removeListener('close', done);
    socket.removeListener('error', done);
    callback();
  };

  socket.once('drain', done);
  socket.once('close', done);
  socket.once('error', done);
}

function pauseConnection(connection, streamId = null) {
  if (streamId !== null && connection && typeof connection.pauseStream === 'function') connection.pauseStream(streamId);
  else if (connection && typeof connection.pause === 'function') connection.pause();
  else if (connection?.socket && typeof connection.socket.pause === 'function') connection.socket.pause();
}

function resumeConnection(connection, streamId = null) {
  if (streamId !== null && connection && typeof connection.resumeStream === 'function') connection.resumeStream(streamId);
  else if (connection && typeof connection.resume === 'function') connection.resume();
  else if (connection?.socket && typeof connection.socket.resume === 'function') connection.socket.resume();
}

function isSingleFlowRequest(info) {
  return info?.tunnelMode === 'single-flow' || info?.singleFlow === true || info?.tunnel?.singleFlow === true;
}

function createProxy(connection, targetPort, targetHost = 'localhost', maxStreams = 100, options = {}) {
  const preserveHost = Boolean(options.preserveHost);
  const targetTimeout = normalizeTargetTimeout(options.targetTimeout);
  const targetKeepAliveTimeout = normalizeNonNegativeInteger(options.targetKeepAliveTimeout, DEFAULT_TARGET_KEEPALIVE_TIMEOUT);
  const targetAgent = new Agent({
    keepAlive: true,
    keepAliveMsecs: Math.min(targetKeepAliveTimeout || DEFAULT_TARGET_KEEPALIVE_TIMEOUT, DEFAULT_TARGET_KEEPALIVE_TIMEOUT),
    maxSockets: Math.max(1, maxStreams),
    maxFreeSockets: Math.max(1, Math.min(maxStreams, 256)),
    timeout: targetKeepAliveTimeout
  });
  const activeStreams = new Map();
  const activeWebSockets = new Map();

  // Set max listeners if connection exposes a raw socket
  if (connection.socket) {
    connection.socket.setMaxListeners(maxStreams + 10);
  }

  function handleFrame(frame) {
    if (activeWebSockets.has(frame.streamId)) {
      handleWebSocketFrame(frame);
      return;
    }

    if (frame.type === FrameType.HEADERS) {
      startProxyRequest(frame.streamId, frame.payload);
    } else if (frame.type === FrameType.UPGRADE) {
      startWebSocketProxy(frame.streamId, frame.payload);
    } else if (frame.type === FrameType.DATA) {
      const streamState = activeStreams.get(frame.streamId);
      if (streamState && !streamState.req.destroyed) {
        streamState.writeRequestData(frame.payload);
      }
    } else if (frame.type === FrameType.FIN) {
      const streamState = activeStreams.get(frame.streamId);
      if (streamState && !streamState.req.destroyed) {
        streamState.endRequest();
      }
    } else if (frame.type === FrameType.ERROR) {
      const streamState = activeStreams.get(frame.streamId);
      if (streamState) {
        streamState.destroy();
      }
    }
  }

  function handleWebSocketFrame(frame) {
    const wsState = activeWebSockets.get(frame.streamId);
    if (!wsState) return;

    if (frame.type === FrameType.DATA) {
      wsState.reassemblyBuffer = Buffer.concat([wsState.reassemblyBuffer, frame.payload]);

      if (!wsState.socket) {
        if (wsState.reassemblyBuffer.length > MAX_WS_BUFFER_SIZE) {
          console.error('WebSocket pre-upgrade buffer overflow - closing connection');
          wsState.cleanup();
        }
        return;
      }

      while (wsState.reassemblyBuffer.length >= 2 && !wsState.closeFramePending) {
        const result = parseWebSocketFrame(wsState.reassemblyBuffer, true);
        if (!result) break;

        const { frameSize, opcode, remaining } = result;

        const isCloseFrame = opcode === 0x08;
        if (isCloseFrame) {
          wsState.closeFramePending = true;
        }

        const completeFrame = wsState.reassemblyBuffer.subarray(0, frameSize);
        wsState.reassemblyBuffer = remaining;

        const canWrite = wsState.socket.write(completeFrame);
        if (!canWrite && !isCloseFrame) {
          pauseConnection(connection, frame.streamId);
          wsState.socket.once('drain', () => {
            if (!wsState.cleanupCalled) resumeConnection(connection, frame.streamId);
          });
        }

        if (isCloseFrame) {
          setTimeout(() => {
            if (!wsState.cleanupCalled) {
              wsState.cleanup();
            }
          }, 5000);
          break;
        }
      }

      if (wsState.reassemblyBuffer.length > MAX_WS_BUFFER_SIZE) {
        console.error('WebSocket reassembly buffer overflow - closing connection');
        wsState.cleanup();
        return;
      }
    } else if (frame.type === FrameType.FIN) {
      if (!wsState.closeFramePending) {
        wsState.cleanup();
      }
    } else if (frame.type === FrameType.ERROR) {
      wsState.cleanup();
    }
  }

  function startWebSocketProxy(streamId, payload) {
    try {
      const upgradeInfo = JSON.parse(payload.toString());
      
      if (upgradeInfo.protocol !== 'websocket') {
        connection.write(encodeFrame(streamId, FrameType.ERROR, Buffer.from('Unsupported protocol')));
        return;
      }

      const singleFlow = isSingleFlowRequest(upgradeInfo);
      if (typeof connection.setStreamMode === 'function') {
        connection.setStreamMode(streamId, { singleFlow });
      }

      const timing = createTimingLog('client-ws', {
        stream: streamId,
        method: upgradeInfo.method,
        path: upgradeInfo.path,
        target: `${targetHost}:${targetPort}`
      }, 'tunnel_upgrade_received');

      const proxyHeaders = filterWebSocketHeaders(upgradeInfo.headers);
      proxyHeaders.host = preserveHost && upgradeInfo.publicHost ? upgradeInfo.publicHost : `${targetHost}:${targetPort}`;

      if (upgradeInfo.remoteAddress) {
        proxyHeaders['x-forwarded-for'] = upgradeInfo.remoteAddress;
      }
      if (upgradeInfo.publicHost) proxyHeaders['x-forwarded-host'] = upgradeInfo.publicHost;
      if (upgradeInfo.publicProto) proxyHeaders['x-forwarded-proto'] = upgradeInfo.publicProto;

      const proxyReq = request({
        hostname: targetHost,
        port: targetPort,
        method: upgradeInfo.method,
        path: upgradeInfo.path,
        headers: proxyHeaders,
        agent: targetAgent
      });

      proxyReq.on('socket', (socket) => {
        timing.mark('target_socket_assigned');
        if (socket.connecting) {
          socket.once('connect', () => timing.mark('target_connected'));
        } else {
          if (proxyReq.reusedSocket) timing.mark('target_socket_reused');
          timing.mark('target_connected');
        }
      });

      let upgradeTimeoutTriggered = false;
      if (targetTimeout > 0) {
        proxyReq.setTimeout(targetTimeout, () => {
          upgradeTimeoutTriggered = true;
          timing.mark('target_upgrade_timeout');
          timing.log('timeout');
          proxyReq.destroy();
          connection.write(encodeFrame(streamId, FrameType.ERROR, Buffer.from('Upgrade timeout')));
          cleanup(false);
        });
      }

      let cleanupCalled = false;

      const wsState = {
        socket: null,
        cleanup: null,
        reassemblyBuffer: Buffer.alloc(0),
        closeFramePending: false,
        cleanupCalled: false
      };

      function cleanup(sendFin = true) {
        if (cleanupCalled || wsState.cleanupCalled) return;
        cleanupCalled = true;
        wsState.cleanupCalled = true;

        activeWebSockets.delete(streamId);
        resumeConnection(connection, streamId);

        if (wsState.socket && !wsState.socket.destroyed) {
          wsState.socket.destroy();
        }

        if (sendFin) {
          connection.write(encodeFrame(streamId, FrameType.FIN, Buffer.alloc(0)));
        }

        if (typeof connection.clearStreamMode === 'function') {
          connection.clearStreamMode(streamId);
        }

        timing.log(sendFin ? 'closed' : 'failed');
      }

      wsState.cleanup = cleanup;
      activeWebSockets.set(streamId, wsState);

      proxyReq.on('upgrade', (proxyRes, proxySocket, proxyHead) => {
        timing.mark('target_upgrade_headers');
        wsState.socket = proxySocket;

        const responseHeaders = {
          upgrade: proxyRes.headers.upgrade || 'websocket',
          connection: proxyRes.headers.connection || 'Upgrade',
          'sec-websocket-accept': proxyRes.headers['sec-websocket-accept']
        };

        const responseHeadersLower = Object.fromEntries(
          Object.entries(responseHeaders).map(([k, v]) => [k.toLowerCase(), v])
        );
        for (const [key, value] of Object.entries(proxyRes.headers)) {
          if (!(key.toLowerCase() in responseHeadersLower)) {
            responseHeaders[key] = value;
          }
        }

        connection.write(encodeFrame(streamId, FrameType.UPGRADE, JSON.stringify({
          status: 101,
          headers: responseHeaders
        })));

        let buffer = Buffer.alloc(0);
        let pendingLargeFrame = null;
        let pendingOffset = 0;

        function sendLargeFrameChunk() {
          while (pendingOffset < pendingLargeFrame.length) {
            const end = Math.min(pendingOffset + MAX_FRAME_SIZE, pendingLargeFrame.length);
            const chunk = pendingLargeFrame.subarray(pendingOffset, end);
            const canWrite = connection.write(encodeFrame(streamId, FrameType.DATA, chunk));
            if (!canWrite) {
              proxySocket.pause();
              pendingOffset = end;
              waitForConnectionDrain(connection, sendLargeFrameChunk, streamId);
              return;
            }
            pendingOffset = end;
          }
          pendingLargeFrame = null;
          pendingOffset = 0;
          proxySocket.resume();
        }

        let firstTargetByte = false;

        function processTargetData(chunk) {
          if (!firstTargetByte && chunk.length > 0) {
            firstTargetByte = true;
            timing.mark('first_byte_from_target');
          }
          if (buffer.length + chunk.length > MAX_WS_BUFFER_SIZE) {
            console.error('WebSocket buffer overflow - destroying connection');
            cleanup();
            return;
          }
          buffer = Buffer.concat([buffer, chunk]);

          while (buffer.length >= 2) {
            if (pendingLargeFrame) break;

            const frameInfo = parseWebSocketFrame(buffer, true);
            if (!frameInfo) break;

            const { frameSize, opcode, remaining } = frameInfo;

            const rawFrame = Buffer.from(buffer.subarray(0, frameSize));
            buffer = remaining;

            const isCloseFrame = opcode === 0x08;
            if (isCloseFrame) {
              wsState.closeFramePending = true;
            }

            if (rawFrame.length <= MAX_FRAME_SIZE) {
              const canWrite = connection.write(encodeFrame(streamId, FrameType.DATA, rawFrame));
              if (!canWrite) {
                proxySocket.pause();
                waitForConnectionDrain(connection, () => {
                  if (!proxySocket.destroyed) proxySocket.resume();
                }, streamId);
              }
            } else {
              pendingLargeFrame = rawFrame;
              pendingOffset = 0;
              sendLargeFrameChunk();
              if (pendingLargeFrame) break;
            }

            if (isCloseFrame) {
              setImmediate(() => {
                if (!connection.write(encodeFrame(streamId, FrameType.FIN, Buffer.alloc(0)))) {
                  waitForConnectionDrain(connection, cleanup, streamId);
                } else {
                  cleanup();
                }
              });
              return;
            }
          }
        }

        proxySocket.on('data', (chunk) => {
          processTargetData(chunk);
        });

        if (proxyHead && proxyHead.length > 0) {
          processTargetData(proxyHead);
        }

        if (wsState.reassemblyBuffer.length > 0) {
          handleWebSocketFrame({
            streamId,
            type: FrameType.DATA,
            payload: Buffer.alloc(0)
          });
        }

        proxySocket.on('close', () => {
          if (!cleanupCalled && !wsState.closeFramePending) {
            cleanup();
          }
        });

        proxySocket.on('error', (err) => {
          console.error(`[CLIENT WS ERROR] WebSocket target error:`, err.message, `(code: ${err.code || 'none'})`);
          cleanup();
        });
      });

      proxyReq.on('error', (err) => {
        if (upgradeTimeoutTriggered) return;
        timing.log('upgrade_error');
        console.error(`[CLIENT WS ERROR] WebSocket upgrade failed for ${upgradeInfo.path}:`, err.message, `(code: ${err.code || 'none'})`);
        const errorDetail = err.code ? `Upgrade failed: ${err.code}` : 'Upgrade failed';
        connection.write(encodeFrame(streamId, FrameType.ERROR, Buffer.from(errorDetail)));
        cleanup(false);
      });

      proxyReq.end();

    } catch (err) {
      console.error('Invalid UPGRADE payload:', err.message);
      connection.write(encodeFrame(streamId, FrameType.ERROR, Buffer.from('Invalid upgrade request')));
      if (typeof connection.clearStreamMode === 'function') {
        connection.clearStreamMode(streamId);
      }
    }
  }

  function startProxyRequest(streamId, payload) {
    try {
      const reqInfo = JSON.parse(payload.toString());
      const singleFlow = isSingleFlowRequest(reqInfo);
      if (typeof connection.setStreamMode === 'function') {
        connection.setStreamMode(streamId, { singleFlow });
      }

      const timing = createTimingLog('client-http', {
        stream: streamId,
        method: reqInfo.method,
        path: reqInfo.path,
        target: `${targetHost}:${targetPort}`
      });

      const proxyHeaders = filterRequestHeaders(reqInfo.headers);
      proxyHeaders.host = preserveHost && reqInfo.publicHost ? reqInfo.publicHost : `${targetHost}:${targetPort}`;

      delete proxyHeaders.origin;
      delete proxyHeaders.referer;

      if (reqInfo.remoteAddress) {
        proxyHeaders['x-forwarded-for'] = reqInfo.remoteAddress;
      }
      if (reqInfo.publicHost) proxyHeaders['x-forwarded-host'] = reqInfo.publicHost;
      if (reqInfo.publicProto) proxyHeaders['x-forwarded-proto'] = reqInfo.publicProto;

      let streamState = null;

      function sendPlainResponse(status, message) {
        connection.write(encodeFrame(streamId, FrameType.HEADERS, JSON.stringify({
          status,
          headers: { 'content-type': 'text/plain' }
        })));
        if (message) {
          connection.write(encodeFrame(streamId, FrameType.DATA, Buffer.from(message)));
        }
        connection.write(encodeFrame(streamId, FrameType.FIN, Buffer.alloc(0)));
      }

      const proxyReq = request({
        hostname: targetHost,
        port: targetPort,
        method: reqInfo.method,
        path: reqInfo.path,
        headers: proxyHeaders,
        agent: targetAgent
      }, (proxyRes) => {
        if (!streamState || streamState.completed) {
          proxyRes.resume();
          return;
        }

        streamState.responseStarted = true;
        streamState.clearTargetTimer();
        timing.mark('target_headers');

        const filteredHeaders = filterRequestHeaders(proxyRes.headers);

        const canWrite = connection.write(encodeFrame(streamId, FrameType.HEADERS, JSON.stringify({
          status: proxyRes.statusCode,
          headers: filteredHeaders
        })));
        timing.mark('tunnel_response_headers_sent');

        if (!canWrite) {
          streamState.pauseTargetResponse(proxyRes);
        }

        let responseEnded = false;
        let firstByteFromTarget = false;

        proxyRes.on('data', (chunk) => {
          if (!firstByteFromTarget && chunk.length > 0) {
            firstByteFromTarget = true;
            timing.mark('first_byte_from_target');
          }

          let offset = 0;
          let canContinue = true;
          while (offset < chunk.length) {
            const end = Math.min(offset + MAX_FRAME_SIZE, chunk.length);
            const frameChunk = chunk.subarray(offset, end);
            if (!connection.write(encodeFrame(streamId, FrameType.DATA, frameChunk))) {
              canContinue = false;
            }
            offset = end;
          }

          if (!canContinue) {
            streamState.pauseTargetResponse(proxyRes);
          }
        });

        proxyRes.on('end', () => {
          responseEnded = true;
          connection.write(encodeFrame(streamId, FrameType.FIN, Buffer.alloc(0)));
          timing.log('complete');
          streamState.cleanup();
        });

        proxyRes.on('close', () => {
          if (!responseEnded && streamState && !streamState.completed) {
            console.error(`[CLIENT ERROR] Target response closed early for ${reqInfo.method} ${reqInfo.path}`);
            timing.log('target_response_closed');
            connection.write(encodeFrame(streamId, FrameType.ERROR, Buffer.from('Target response closed')));
            streamState.cleanup();
          }
        });

        proxyRes.on('error', (err) => {
          if (streamState && streamState.completed) return;
          console.error(`[CLIENT ERROR] Target response error for ${reqInfo.method} ${reqInfo.path}:`, err.message, `(code: ${err.code || 'none'})`);
          timing.log('target_response_error');
          const errorDetail = err.code ? `Target error: ${err.code}` : 'Target error';
          connection.write(encodeFrame(streamId, FrameType.ERROR, Buffer.from(errorDetail)));
          if (streamState) streamState.cleanup();
        });
      });

      proxyReq.on('socket', (socket) => {
        timing.mark('target_socket_assigned');
        if (socket.connecting) {
          socket.once('connect', () => timing.mark('target_connected'));
        } else {
          if (proxyReq.reusedSocket) timing.mark('target_socket_reused');
          timing.mark('target_connected');
        }
      });

      streamState = {
        req: proxyReq,
        responseStarted: false,
        requestEnded: false,
        completed: false,
        timedOut: false,
        targetTimer: null,
        targetRequestBackpressured: false,
        targetResponseBackpressured: false,

        clearTargetTimer() {
          if (this.targetTimer) {
            clearTimeout(this.targetTimer);
            this.targetTimer = null;
          }
        },

        startTargetTimer() {
          if (targetTimeout <= 0 || this.responseStarted || this.completed) return;
          this.clearTargetTimer();
          this.targetTimer = setTimeout(() => {
            this.timeoutTarget();
          }, targetTimeout);
          if (typeof this.targetTimer.unref === 'function') this.targetTimer.unref();
        },

        timeoutTarget() {
          if (this.completed) return;
          this.timedOut = true;
          timing.mark('target_timeout');
          timing.log('target_timeout');
          console.error(`[CLIENT TIMEOUT] Target response timeout after ${targetTimeout}ms for ${reqInfo.method} ${reqInfo.path}`);

          if (this.responseStarted) {
            connection.write(encodeFrame(streamId, FrameType.ERROR, Buffer.from('Target response timeout')));
          } else {
            sendPlainResponse(504, 'Target response timeout');
          }

          if (!this.req.destroyed) {
            this.req.destroy(new Error('Target response timeout'));
          }
          this.cleanup();
        },

        writeRequestData(chunk) {
          if (this.completed || this.req.destroyed) return;
          if (!this.req.write(chunk) && !this.targetRequestBackpressured) {
            this.targetRequestBackpressured = true;
            pauseConnection(connection, streamId);
            this.req.once('drain', () => {
              this.targetRequestBackpressured = false;
              if (!this.completed) resumeConnection(connection, streamId);
            });
          }
        },

        pauseTargetResponse(proxyRes) {
          if (this.completed || this.targetResponseBackpressured) return;
          this.targetResponseBackpressured = true;
          proxyRes.pause();
          waitForConnectionDrain(connection, () => {
            this.targetResponseBackpressured = false;
            if (!this.completed) proxyRes.resume();
          }, streamId);
        },

        endRequest() {
          if (this.requestEnded || this.completed) return;
          this.requestEnded = true;
          this.req.end();
          this.startTargetTimer();
        },

        destroy() {
          if (!this.req.destroyed) this.req.destroy();
          this.cleanup();
        },

        cleanup() {
          if (this.completed) return;
          this.completed = true;
          if (this.targetRequestBackpressured) resumeConnection(connection, streamId);
          this.clearTargetTimer();
          activeStreams.delete(streamId);
          if (typeof connection.clearStreamMode === 'function') {
            connection.clearStreamMode(streamId);
          }
        }
      };

      activeStreams.set(streamId, streamState);

      proxyReq.on('error', (err) => {
        if (streamState && (streamState.completed || streamState.timedOut)) return;

        console.error(`[CLIENT ERROR] Target error for ${reqInfo.method} ${reqInfo.path}:`, err.message, `(code: ${err.code || 'none'})`);
        timing.log('target_error');
        if (err.code === 'ECONNREFUSED') {
          sendPlainResponse(502, 'Target service not available');
        } else {
          const errorDetail = err.code ? `Target error: ${err.code}` : 'Target error';
          connection.write(encodeFrame(streamId, FrameType.ERROR, Buffer.from(errorDetail)));
        }
        if (streamState) streamState.cleanup();
      });

      proxyReq.on('drain', () => {
        // Target ready for more data — server will resume sending
      });

    } catch (err) {
      connection.write(encodeFrame(streamId, FrameType.ERROR, Buffer.from('Invalid request')));
      if (typeof connection.clearStreamMode === 'function') {
        connection.clearStreamMode(streamId);
      }
    }
  }

  function parseWebSocketFrame(buffer, boundariesOnly = false) {
    if (buffer.length < 2) return null;
    
    const fin = (buffer[0] & 0x80) !== 0;
    const opcode = buffer[0] & 0x0f;
    const masked = (buffer[1] & 0x80) !== 0;
    let payloadLen = buffer[1] & 0x7f;
    
    let offset = 2;
    
    if (payloadLen === 126) {
      if (buffer.length < 4) return null;
      payloadLen = buffer.readUInt16BE(2);
      offset = 4;
    } else if (payloadLen === 127) {
      if (buffer.length < 10) return null;
      const high = buffer.readUInt32BE(2);
      if (high !== 0) return null;
      payloadLen = buffer.readUInt32BE(6);
      offset = 10;
    }
    
    if (masked) {
      offset += 4;
    }
    
    const frameSize = offset + payloadLen;
    if (buffer.length < frameSize) return null;
    
    const remaining = buffer.subarray(frameSize);
    
    if (boundariesOnly) {
      return { frameSize, opcode, remaining };
    }
    
    let payload = buffer.subarray(offset, frameSize);
    
    if (masked) {
      const maskKey = buffer.subarray(offset - 4, offset);
      payload = Buffer.from(payload);
      for (let i = 0; i < payload.length; i++) {
        payload[i] ^= maskKey[i % 4];
      }
    }
    
    return { fin, opcode, payload, remaining };
  }

  function buildWebSocketFrame(opcode, payload) {
    const payloadLen = payload.length;
    let frame;
    
    if (payloadLen < 126) {
      frame = Buffer.allocUnsafe(2 + payloadLen);
      frame[0] = 0x80 | opcode;
      frame[1] = payloadLen;
      payload.copy(frame, 2);
    } else if (payloadLen < 65536) {
      frame = Buffer.allocUnsafe(4 + payloadLen);
      frame[0] = 0x80 | opcode;
      frame[1] = 126;
      frame.writeUInt16BE(payloadLen, 2);
      payload.copy(frame, 4);
    } else {
      frame = Buffer.allocUnsafe(10 + payloadLen);
      frame[0] = 0x80 | opcode;
      frame[1] = 127;
      frame.writeUInt32BE(0, 2);
      frame.writeUInt32BE(payloadLen, 6);
      payload.copy(frame, 10);
    }
    
    return frame;
  }

  function destroy() {
    targetAgent.destroy();

    for (const [streamId, wsState] of activeWebSockets) {
      wsState.cleanup();
    }
    activeWebSockets.clear();

    for (const [streamId, streamState] of activeStreams) {
      streamState.destroy();
    }
    activeStreams.clear();
  }

  return {
    handleFrame,
    destroy
  };
}

module.exports = { createProxy };
