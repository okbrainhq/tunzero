// HTTP Router - Routes public HTTP requests to the tunnel client(s)
// Works with ConnectionPool for multipath support

const { createServer } = require('node:http');
const { encodeFrame, FrameType, MAX_FRAME_SIZE } = require('../../../packages/frame-protocol');

const STREAM_TIMEOUT = 300000; // 5 minutes — allows large uploads/downloads without false 504s
const DEFAULT_MAX_BODY_SIZE = 220 * 1024 * 1024;
const MAX_WS_BUFFER_SIZE = 16 * 1024 * 1024;
const DEFAULT_HTTP_KEEPALIVE_TIMEOUT = 60 * 60 * 1000;
const HTTP_HEADERS_TIMEOUT_BUFFER = 5000;
const LOG_VALUE_MAX_LENGTH = 200;
const SINGLE_FLOW_MEDIA_EXTENSION_RE = /\.(?:aac|aif|aiff|flac|m4a|m4b|mp3|oga|ogg|opus|wav|weba|m4v|mkv|mov|mp4|webm)(?:$|[?#])/i;
const SINGLE_FLOW_REQUEST_CONTENT_TYPE_RE = /^(?:multipart\/form-data|audio\/|video\/|application\/octet-stream\b)/i;

const STATUS_TEXTS = {
  400: 'Bad Request',
  403: 'Forbidden',
  404: 'Not Found',
  502: 'Bad Gateway',
  503: 'Service Unavailable',
  504: 'Gateway Timeout'
};

function getStatusText(code) {
  return STATUS_TEXTS[code] || 'Error';
}

const HOP_BY_HOP_HEADERS = new Set([
  'connection',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'te',
  'trailer',
  'transfer-encoding',
  'upgrade'
]);

function isWebSocketUpgrade(req) {
  const upgrade = req.headers.upgrade?.toLowerCase();
  const connection = req.headers.connection?.toLowerCase();
  return upgrade === 'websocket' && 
          (connection === 'upgrade' || connection?.includes('upgrade'));
}

function filterResponseHeaders(headers) {
  const filtered = {};
  for (const [key, value] of Object.entries(headers)) {
    if (!HOP_BY_HOP_HEADERS.has(key.toLowerCase())) {
      filtered[key] = value;
    }
  }
  return filtered;
}

function sanitizeRequestHeaders(headers) {
  const sanitized = {};
  for (const [key, value] of Object.entries(headers)) {
    if (typeof value === 'string') {
      sanitized[key] = value;
    } else if (Array.isArray(value)) {
      sanitized[key] = value.join(', ');
    }
  }
  return sanitized;
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

function getRequestPathname(reqUrl) {
  try {
    return new URL(reqUrl || '/', 'http://okproxy.local').pathname || '/';
  } catch {
    return String(reqUrl || '/').split('?')[0] || '/';
  }
}

function shouldUseSingleFlow(reqUrl, headers = {}, method = 'GET') {
  const pathname = getRequestPathname(reqUrl);
  if (SINGLE_FLOW_MEDIA_EXTENSION_RE.test(pathname)) return true;

  const accept = String(headers.accept || '');
  if (/\b(?:audio|video)\//i.test(accept)) return true;

  const fetchDest = String(headers['sec-fetch-dest'] || '');
  if (/^(?:audio|video)$/i.test(fetchDest)) return true;

  // Browser media players commonly use Range requests. Treat them as heavy
  // single-flow candidates so they do not duplicate across every multipath lane.
  if (headers.range) return true;

  const requestMethod = String(method || 'GET').toUpperCase();
  const hasRequestBody = !['GET', 'HEAD', 'OPTIONS'].includes(requestMethod);
  if (hasRequestBody) {
    const contentType = String(headers['content-type'] || '');
    if (SINGLE_FLOW_REQUEST_CONTENT_TYPE_RE.test(contentType)) return true;

    // Resumable/chunked upload protocols advertise these headers even when
    // the request content type is generic.
    if (headers['content-range'] || headers['upload-length'] || headers['upload-offset'] || headers['tus-resumable']) {
      return true;
    }
  }

  return false;
}

function createTimingLog(scope, details = {}, startedAt = process.hrtime.bigint()) {
  const marks = new Map([['browser_accepted', 0]]);
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

function waitForDrain(target, callback) {
  if (!target || target.destroyed || !target.writableNeedDrain) {
    process.nextTick(callback);
    return;
  }

  let settled = false;
  const done = () => {
    if (settled) return;
    settled = true;
    target.removeListener('drain', done);
    target.removeListener('close', done);
    target.removeListener('error', done);
    callback();
  };

  target.once('drain', done);
  target.once('close', done);
  target.once('error', done);
}

function waitForPoolDrain(pool, callback, streamId = null) {
  if (streamId !== null && pool && typeof pool.onceDrainForStream === 'function') {
    pool.onceDrainForStream(streamId, callback);
  } else if (pool && typeof pool.onceDrain === 'function') {
    pool.onceDrain(callback);
  } else {
    process.nextTick(callback);
  }
}

function pausePool(pool, streamId = null) {
  if (streamId !== null && pool && typeof pool.pauseStream === 'function') pool.pauseStream(streamId);
  else if (pool && typeof pool.pause === 'function') pool.pause();
}

function resumePool(pool, streamId = null) {
  if (streamId !== null && pool && typeof pool.resumeStream === 'function') pool.resumeStream(streamId);
  else if (pool && typeof pool.resume === 'function') pool.resume();
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

function createHTTPServer(connectionPool, tcpServer, options = {}) {
  const streamTimeout = options.streamTimeout || STREAM_TIMEOUT;
  const maxStreams = options.maxConcurrentStreams || 100;
  const maxWebSocketStreams = options.maxWebSocketStreams || 50;
  const maxBodySize = options.maxBodySize || DEFAULT_MAX_BODY_SIZE;
  const certBoundDomains = Boolean(options.certBoundDomains);
  const httpKeepAliveTimeout = normalizeNonNegativeInteger(options.httpKeepAliveTimeout, DEFAULT_HTTP_KEEPALIVE_TIMEOUT);
  const httpHeadersTimeout = Math.max(
    normalizeNonNegativeInteger(options.httpHeadersTimeout, 0),
    httpKeepAliveTimeout + HTTP_HEADERS_TIMEOUT_BUFFER
  );

  function resolveRequestRoute(hostHeader) {
    if (!certBoundDomains) {
      if (connectionPool.count === 0) return { error: 502, message: 'Tunnel client not connected' };
      return { pool: connectionPool, domain: null, session: null };
    }

    const route = connectionPool.resolveByHost(hostHeader);
    if (route.status === 'invalid-host') return { error: 400, message: 'Bad Request' };
    if (route.status === 'unknown') return { error: 404, message: 'Unknown tunnel domain' };
    if (route.status === 'disconnected') return { error: 502, message: `Tunnel client not connected for ${route.domain}` };
    if (!route.session || !route.session.hasConnections()) return { error: 502, message: `Tunnel client not connected for ${route.domain}` };
    return { pool: route.session.pool, domain: route.domain, session: route.session };
  }

  function allocateStream(route) {
    return route.session ? route.session.allocateStreamId() : tcpServer.allocateStreamId();
  }

  function releaseStream(route, streamId) {
    if (route.session) route.session.releaseStreamId(streamId);
    else tcpServer.releaseStreamId(streamId);
  }

  const server = createServer((req, res) => {
    const acceptedAt = process.hrtime.bigint();

    if (certBoundDomains && req.url.startsWith('/_okproxy/caddy-ask')) {
      try {
        const url = new URL(req.url, 'http://127.0.0.1');
        const domain = url.searchParams.get('domain');
        if (connectionPool.isAskAllowed(domain)) {
          res.statusCode = 200;
          res.end('OK');
        } else {
          res.statusCode = 404;
          res.end('Not Found');
        }
      } catch {
        res.statusCode = 400;
        res.end('Bad Request');
      }
      return;
    }

    const route = resolveRequestRoute(req.headers.host);
    if (route.error) {
      console.error(`[${route.error}] ${route.message} for ${req.method} ${req.url} from ${req.socket.remoteAddress}`);
      res.statusCode = route.error;
      res.end(route.message);
      return;
    }

    const selectedPool = route.pool;
    if (selectedPool.activeStreams.size >= maxStreams) {
      console.error(`[503] Max concurrent streams exceeded (${selectedPool.activeStreams.size}/${maxStreams}) for ${req.method} ${req.url}`);
      res.statusCode = 503;
      res.end('Max concurrent streams exceeded');
      return;
    }

    let streamId;
    try {
      streamId = allocateStream(route);
    } catch {
      res.statusCode = 503;
      res.end('No available stream IDs');
      return;
    }

    for (const [name, sock] of selectedPool.connections) {
      sock.setMaxListeners(maxStreams + 10);
    }

    const clientIp = req.socket.remoteAddress || '127.0.0.1';
    const publicProto = req.headers['x-forwarded-proto'] || (req.socket.encrypted ? 'https' : 'http');
    const singleFlow = shouldUseSingleFlow(req.url, req.headers, req.method);
    if (typeof selectedPool.setStreamMode === 'function') {
      selectedPool.setStreamMode(streamId, { singleFlow });
    }
    const timing = createTimingLog('server-http', {
      stream: streamId,
      method: req.method,
      path: req.url,
      host: req.headers.host || '',
      client: clientIp,
      tunnelMode: singleFlow ? 'single-flow' : 'multipath'
    }, acceptedAt);
    let bodySize = 0;
    let cleanedUp = false;
    let requestBackpressured = false;
    let responseBackpressured = false;
    let firstByteToBrowser = false;

    const sentHeaders = selectedPool.send(encodeFrame(streamId, FrameType.HEADERS, JSON.stringify({
      method: req.method,
      path: req.url,
      headers: sanitizeRequestHeaders(req.headers),
      clientSerial: route.session?.serial,
      publicHost: route.domain || req.headers.host,
      publicProto,
      remoteAddress: clientIp,
      tunnelMode: singleFlow ? 'single-flow' : 'multipath',
      tunnel: { singleFlow }
    })));
    timing.mark('tunnel_headers_sent');

    if (!sentHeaders) {
      requestBackpressured = true;
      req.pause();
      waitForPoolDrain(selectedPool, () => {
        requestBackpressured = false;
        if (!cleanedUp) req.resume();
      }, streamId);
    }

    let streamTimer = null;

    function scheduleStreamTimeout(isReset) {
      if (streamTimer) clearTimeout(streamTimer);
      streamTimer = setTimeout(() => {
        console.error(`[504] Stream timeout${isReset ? ' (reset)' : ''} for ${req.method} ${req.url} (stream ${streamId}, client ${clientIp})`);
        timing.mark('stream_timeout');
        timing.log('stream_timeout');
        selectedPool.send(encodeFrame(streamId, FrameType.ERROR, Buffer.from('Stream timeout')));
        cleanup();
        if (!res.writableEnded) {
          res.statusCode = 504;
          res.end('Gateway timeout');
        }
      }, streamTimeout);
    }

    function resetStreamTimeout() {
      scheduleStreamTimeout(true);
    }

    scheduleStreamTimeout(false);

    req.on('data', (chunk) => {
      if (cleanedUp) return;
      resetStreamTimeout();

      bodySize += chunk.length;
      if (bodySize > maxBodySize) {
        console.error(`[413] Request body too large: ${bodySize} bytes (max: ${maxBodySize}) for stream ${streamId}`);
        timing.log('request_too_large');
        abortTunnelStream('Request body too large');
        if (!res.writableEnded) {
          res.statusCode = 413;
          res.end('Request body too large');
        }
        req.destroy();
        return;
      }

      let offset = 0;
      let canContinue = true;
      while (offset < chunk.length) {
        const end = Math.min(offset + MAX_FRAME_SIZE, chunk.length);
        if (!selectedPool.send(encodeFrame(streamId, FrameType.DATA, chunk.subarray(offset, end)))) {
          canContinue = false;
        }
        offset = end;
      }

      if (!canContinue && !requestBackpressured) {
        requestBackpressured = true;
        req.pause();
        waitForPoolDrain(selectedPool, () => {
          requestBackpressured = false;
          if (!cleanedUp) req.resume();
        }, streamId);
      }
    });

    req.on('end', () => {
      if (!cleanedUp) {
        resetStreamTimeout();
        selectedPool.send(encodeFrame(streamId, FrameType.FIN, Buffer.alloc(0)));
      }
    });

    req.on('error', (err) => {
      console.error('Request error:', err.message);
      timing.log('request_error');
      cleanup();
    });

    function cleanup() {
      if (cleanedUp) return;
      cleanedUp = true;
      if (requestBackpressured) req.resume();
      if (responseBackpressured) resumePool(selectedPool, streamId);
      clearTimeout(streamTimer);
      selectedPool.unregisterStream(streamId);
      releaseStream(route, streamId);
    }

    function abortTunnelStream(message) {
      if (!cleanedUp) selectedPool.send(encodeFrame(streamId, FrameType.ERROR, Buffer.from(message)));
      cleanup();
    }

    let headersSent = false;
    selectedPool.registerStream(streamId, {
      frameHandler: (frame) => {
        resetStreamTimeout();

        if (frame.type === FrameType.HEADERS) {
          try {
            timing.mark('response_headers_received');
            const headers = JSON.parse(frame.payload.toString());
            res.statusCode = headers.status || 200;
            if (headers.headers) {
              const filteredHeaders = filterResponseHeaders(headers.headers);
              for (const [k, v] of Object.entries(filteredHeaders)) {
                try { res.setHeader(k, v); } catch (headerErr) { console.error(`Skipping malformed header '${k}':`, headerErr.message); }
              }
            }
            headersSent = true;
            if (!res.headersSent && typeof res.flushHeaders === 'function') {
              res.flushHeaders();
              timing.mark('response_headers_flushed');
            }
            resetStreamTimeout();
          } catch (err) {
            console.error('Invalid headers frame:', err.message);
            timing.log('invalid_response');
            cleanup();
            res.statusCode = 502;
            res.end('Invalid response');
          }
        } else if (frame.type === FrameType.DATA) {
          if (!headersSent) {
            res.statusCode = 200;
            headersSent = true;
            if (!res.headersSent && typeof res.flushHeaders === 'function') {
              res.flushHeaders();
              timing.mark('response_headers_flushed');
            }
          }
          if (!firstByteToBrowser && frame.payload.length > 0) {
            firstByteToBrowser = true;
            timing.mark('first_byte_to_browser');
          }
          if (!res.write(frame.payload) && !responseBackpressured) {
            responseBackpressured = true;
            pausePool(selectedPool, streamId);
            waitForDrain(res, () => {
              responseBackpressured = false;
              if (!cleanedUp) resumePool(selectedPool, streamId);
            });
          }
          resetStreamTimeout();
        } else if (frame.type === FrameType.FIN) {
          resetStreamTimeout();
          cleanup();
          if (!res.writableEnded) res.end();
        } else if (frame.type === FrameType.ERROR) {
          const errorMsg = frame.payload?.toString() || 'Unknown error';
          console.error(`[502] Client sent ERROR frame for ${req.method} ${req.url} (stream ${streamId}): ${errorMsg}`);
          timing.log('client_error');
          cleanup();
          if (!res.writableEnded) {
            res.statusCode = 502;
            res.end('Bad Gateway');
          }
        }
      },
      errorHandler: (err) => {
        console.error(`[502] Stream error for ${req.method} ${req.url} (stream ${streamId}):`, err.message);
        timing.log('stream_error');
        cleanup();
        if (!res.writableEnded) {
          res.statusCode = 502;
          res.end('Bad Gateway');
        }
      }
    });

    res.on('finish', () => {
      timing.log('complete');
    });

    res.on('close', () => {
      if (!res.writableEnded) {
        console.error(`[INFO] Client closed connection early for ${req.method} ${req.url} (stream ${streamId})`);
        timing.log('client_closed');
        abortTunnelStream('Public client closed connection');
      }
    });
  });

  server.on('upgrade', (req, socket, head) => {
    const route = resolveRequestRoute(req.headers.host);
    if (route.error) {
      socket.write(`HTTP/1.1 ${route.error} ${getStatusText(route.error)}\r\nConnection: close\r\n\r\n`);
      socket.destroy();
      return;
    }

    const selectedPool = route.pool;
    let headBuffer = head && head.length > 0 ? head : Buffer.alloc(0);
    const webSockets = route.session ? route.session.activeWebSockets : server._legacyActiveWebSockets || (server._legacyActiveWebSockets = new Set());

    if (webSockets.size >= maxWebSocketStreams) {
      socket.write('HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n');
      socket.destroy();
      return;
    }

    if (!isWebSocketUpgrade(req)) {
      socket.write('HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n');
      socket.destroy();
      return;
    }

    let streamId;
    try {
      streamId = allocateStream(route);
    } catch {
      socket.write('HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n');
      socket.destroy();
      return;
    }
    webSockets.add(streamId);

    for (const [name, sock] of selectedPool.connections) {
      sock.setMaxListeners(maxStreams + maxWebSocketStreams + 10);
    }

    const singleFlow = shouldUseSingleFlow(req.url, req.headers, req.method);
    if (typeof selectedPool.setStreamMode === 'function') {
      selectedPool.setStreamMode(streamId, { singleFlow });
    }

    const upgradePayload = JSON.stringify({
      protocol: 'websocket',
      method: req.method,
      path: req.url,
      headers: sanitizeRequestHeaders(req.headers),
      clientSerial: route.session?.serial,
      publicHost: route.domain || req.headers.host,
      publicProto: req.headers['x-forwarded-proto'] || (req.socket.encrypted ? 'https' : 'http'),
      remoteAddress: req.socket.remoteAddress || '127.0.0.1',
      tunnelMode: singleFlow ? 'single-flow' : 'multipath',
      tunnel: { singleFlow }
    });

    selectedPool.send(encodeFrame(streamId, FrameType.UPGRADE, upgradePayload));

    let wsBuffer = Buffer.alloc(0);
    let targetToBrowserBuffer = Buffer.alloc(0);
    let upgradeResponseReceived = false;
    let cleanupCalled = false;
    let closeFramePending = false;
    let browserBackpressured = false;
    const WS_IDLE_TIMEOUT = 300000;
    let idleTimer = null;

    function resetIdleTimer() {
      if (idleTimer) clearTimeout(idleTimer);
      idleTimer = setTimeout(() => {
        if (!cleanupCalled) {
          if (upgradeResponseReceived) {
            const closeFrame = buildWebSocketFrame(0x08, Buffer.from([0x03, 0xe9]));
            socket.write(closeFrame, () => cleanup());
          } else {
            cleanup();
          }
        }
      }, WS_IDLE_TIMEOUT);
    }

    function cleanup() {
      if (cleanupCalled) return;
      cleanupCalled = true;
      if (idleTimer) clearTimeout(idleTimer);
      if (browserBackpressured) resumePool(selectedPool, streamId);
      webSockets.delete(streamId);
      selectedPool.unregisterStream(streamId);
      releaseStream(route, streamId);
      socket.destroy();
    }

    resetIdleTimer();

    selectedPool.registerStream(streamId, {
      frameHandler: (frame) => {
        if (frame.type === FrameType.UPGRADE) {
          resetIdleTimer();
          try {
            const response = JSON.parse(frame.payload.toString());
            if (response.status !== 101) {
              const errorStatus = response.status || 502;
              const errorHeaders = response.headers || {};
              const errorBody = errorHeaders['content-length'] ? '' : `WebSocket upgrade failed: ${errorStatus}\r\n`;
              const headerLines = [`HTTP/1.1 ${errorStatus} ${getStatusText(errorStatus)}`, 'Connection: close', `Content-Length: ${Buffer.byteLength(errorBody)}`, '', ''];
              socket.write(headerLines.join('\r\n') + errorBody, () => cleanup());
              return;
            }

            const headers = response.headers || {};
            const headerLines = ['HTTP/1.1 101 Switching Protocols', `Upgrade: ${headers.upgrade || 'websocket'}`, `Connection: ${headers.connection || 'Upgrade'}`, `Sec-WebSocket-Accept: ${headers['sec-websocket-accept'] || ''}`, '', ''];
            upgradeResponseReceived = true;
            socket.write(headerLines.join('\r\n'), (err) => { if (err) cleanup(); });
          } catch (err) {
            console.error('Invalid UPGRADE response:', err.message);
            cleanup();
          }
        } else if (frame.type === FrameType.DATA && upgradeResponseReceived) {
          resetIdleTimer();
          targetToBrowserBuffer = Buffer.concat([targetToBrowserBuffer, frame.payload]);
          while (targetToBrowserBuffer.length >= 2 && !closeFramePending) {
            const result = parseWebSocketFrame(targetToBrowserBuffer, true);
            if (!result) break;
            const { frameSize, opcode, remaining } = result;
            const completeFrame = targetToBrowserBuffer.subarray(0, frameSize);
            targetToBrowserBuffer = remaining;
            const isCloseFrame = opcode === 0x08;
            if (isCloseFrame) closeFramePending = true;
            const canWrite = socket.write(completeFrame, (err) => {
              if (err) cleanup();
              else if (isCloseFrame) cleanup();
            });
            if (!canWrite && !browserBackpressured && !isCloseFrame) {
              browserBackpressured = true;
              pausePool(selectedPool, streamId);
              waitForDrain(socket, () => {
                browserBackpressured = false;
                if (!cleanupCalled) resumePool(selectedPool, streamId);
              });
            }
            if (isCloseFrame) break;
          }
          if (targetToBrowserBuffer.length > MAX_WS_BUFFER_SIZE) {
            console.error('WebSocket reassembly buffer overflow - closing connection');
            cleanup();
          }
        } else if (frame.type === FrameType.FIN) {
          if (!closeFramePending) cleanup();
        } else if (frame.type === FrameType.ERROR) {
          cleanup();
        }
      },
      errorHandler: (err) => {
        console.error('WebSocket stream error:', err.message);
        cleanup();
      }
    });

    let pendingLargeFrame = null;
    let pendingOffset = 0;

    function sendLargeFrameChunk() {
      while (pendingOffset < pendingLargeFrame.length) {
        const end = Math.min(pendingOffset + MAX_FRAME_SIZE, pendingLargeFrame.length);
        const canWrite = selectedPool.send(encodeFrame(streamId, FrameType.DATA, pendingLargeFrame.subarray(pendingOffset, end)));
        pendingOffset = end;
        if (!canWrite) {
          socket.pause();
          waitForPoolDrain(selectedPool, sendLargeFrameChunk, streamId);
          return;
        }
      }
      pendingLargeFrame = null;
      pendingOffset = 0;
      socket.resume();
    }

    function processBrowserData(chunk) {
      if (wsBuffer.length + chunk.length > MAX_WS_BUFFER_SIZE) {
        console.error('WebSocket buffer overflow - destroying connection');
        cleanup();
        return;
      }
      wsBuffer = Buffer.concat([wsBuffer, chunk]);
      while (wsBuffer.length >= 2) {
        if (pendingLargeFrame) break;
        const result = parseWebSocketFrame(wsBuffer, true);
        if (!result) break;
        const { frameSize, remaining, opcode } = result;
        const rawFrame = Buffer.from(wsBuffer.subarray(0, frameSize));
        wsBuffer = remaining;
        if (rawFrame.length <= MAX_FRAME_SIZE) {
          if (!selectedPool.send(encodeFrame(streamId, FrameType.DATA, rawFrame))) {
            socket.pause();
            waitForPoolDrain(selectedPool, () => {
              if (!cleanupCalled) socket.resume();
            }, streamId);
          }
        } else {
          pendingLargeFrame = rawFrame;
          pendingOffset = 0;
          socket.pause();
          sendLargeFrameChunk();
          if (pendingLargeFrame) break;
        }
        if (opcode === 0x08) return;
      }
    }

    socket.on('data', (chunk) => {
      resetIdleTimer();
      processBrowserData(chunk);
    });

    if (headBuffer.length > 0) {
      resetIdleTimer();
      processBrowserData(headBuffer);
      headBuffer = Buffer.alloc(0);
    }

    socket.on('end', () => {
      if (!cleanupCalled) {
        selectedPool.send(encodeFrame(streamId, FrameType.FIN, Buffer.alloc(0)));
        cleanup();
      }
    });

    socket.on('close', () => {
      if (!cleanupCalled) {
        selectedPool.send(encodeFrame(streamId, FrameType.FIN, Buffer.alloc(0)));
        cleanup();
      }
    });

    socket.on('error', (err) => {
      console.error('WebSocket socket error:', err.message);
      cleanup();
    });
  });

  server.keepAliveTimeout = httpKeepAliveTimeout;
  server.headersTimeout = httpHeadersTimeout;

  return server;
}

module.exports = {
  createHTTPServer,
  isWebSocketUpgrade,
  buildWebSocketFrame,
  parseWebSocketFrame,
  shouldUseSingleFlow
};
