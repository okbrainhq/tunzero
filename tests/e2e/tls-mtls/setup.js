// Shared test setup for TLS e2e tests (multipath-aware)

const { createTLSServer } = require('../../../apps/server/lib/tls-server');
const { createHTTPServer } = require('../../../apps/server/lib/http-router');
const { ConnectionPool } = require('../../../apps/server/lib/connection-pool');
const { VirtualSocket } = require('../../../apps/client/lib/virtual-socket');
const { createProxy } = require('../../../apps/client/lib/proxy');
const { createMockTarget } = require('./mock-target');
const { initCA, issueClientCertificate, issueServerCertificate } = require('../../../apps/server/lib/ca');
const { mkdtempSync, writeFileSync, mkdirSync } = require('node:fs');
const { join } = require('node:path');
const { tmpdir } = require('node:os');

let testCaDir = null;
let testCertDir = null;

function initTestCerts() {
  if (testCaDir) return;

  testCaDir = mkdtempSync(join(tmpdir(), 'tunnel-ca-'));
  testCertDir = mkdtempSync(join(tmpdir(), 'tunnel-certs-'));

  initCA(testCaDir);

  const serverDir = join(testCertDir, 'server');
  mkdirSync(serverDir, { recursive: true });
  issueServerCertificate('localhost', serverDir, testCaDir);

  const clientDir = join(testCertDir, 'client');
  mkdirSync(clientDir, { recursive: true });
  issueClientCertificate(clientDir, testCaDir);
}

function issueTestClientCertificate(name, domains = []) {
  initTestCerts();
  const clientDir = join(testCertDir, name);
  mkdirSync(clientDir, { recursive: true });
  issueClientCertificate(clientDir, testCaDir, { name, domains });
  return {
    clientKey: join(clientDir, 'client-key.pem'),
    clientCert: join(clientDir, 'client-cert.pem'),
    clientCa: join(clientDir, 'ca-cert.pem'),
    caDir: testCaDir,
    issuedDomainIndex: join(testCaDir, 'issued-domains.json')
  };
}

function getCertPaths() {
  initTestCerts();
  return {
    caCert: join(testCaDir, 'ca-cert.pem'),
    caDir: testCaDir,
    serverKey: join(testCertDir, 'server', 'server-key.pem'),
    serverCert: join(testCertDir, 'server', 'server-cert.pem'),
    clientKey: join(testCertDir, 'client', 'client-key.pem'),
    clientCert: join(testCertDir, 'client', 'client-cert.pem'),
    clientCa: join(testCertDir, 'client', 'ca-cert.pem')
  };
}

async function getPort() {
  const { createServer } = require('node:net');
  return new Promise((resolve) => {
    const server = createServer();
    server.listen(0, () => {
      const port = server.address().port;
      server.close(() => resolve(port));
    });
  });
}

async function createTestEnv(options = {}) {
  const certs = getCertPaths();
  const tlsPort = await getPort();
  const httpPort = await getPort();
  const targetPort = await getPort();

  const connectionPool = new ConnectionPool();
  const tlsServer = createTLSServer(connectionPool, {
    serverKey: certs.serverKey,
    serverCert: certs.serverCert,
    caCert: certs.caCert,
    caDir: certs.caDir,
    maxConcurrentStreams: options.maxStreams || 100,
    streamTimeout: options.streamTimeout || 30000,
    keepaliveInterval: options.keepaliveInterval || 10000,
    keepaliveTimeout: options.keepaliveTimeout || 25000,
    initTimeout: options.initTimeout || 10000
  });

  const httpServer = createHTTPServer(connectionPool, tlsServer, {
    maxConcurrentStreams: options.maxStreams || 100,
    streamTimeout: options.streamTimeout || 30000,
    maxBodySize: options.maxBodySize
  });

  const mockTarget = createMockTarget(options.mockTarget);

  await new Promise((resolve) => tlsServer.listen(tlsPort, resolve));
  await new Promise((resolve) => httpServer.listen(httpPort, resolve));
  await new Promise((resolve) => mockTarget.listen(targetPort, resolve));

  let virtualSocket = null;
  let clientProxy = null;
  let clientReady = false;

  function doConnect() {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('Client connection timeout'));
      }, 5000);

      virtualSocket = new VirtualSocket({
        serverHost: 'localhost',
        serverPort: tlsPort,
        clientKey: certs.clientKey,
        clientCert: certs.clientCert,
        caCert: certs.clientCa,
        parallelSockets: options.parallelSockets
      });

      virtualSocket.on('ready', () => {
        clearTimeout(timeout);
        clientReady = true;
        clientProxy = createProxy(virtualSocket, targetPort, 'localhost', options.maxStreams || 100, {
          targetTimeout: options.targetTimeout
        });
        resolve();
      });

      virtualSocket.on('frame', (frame) => {
        if (clientProxy) {
          clientProxy.handleFrame(frame);
        }
      });

      virtualSocket.on('error', (err) => {
        clearTimeout(timeout);
        if (!clientReady) reject(err);
      });

      virtualSocket.start();
    });
  }

  function startClient() {
    clientReady = false;
    return doConnect();
  }

  function disconnectClient() {
    // Destroy one RealSocket (there's at least one)
    for (const rs of virtualSocket.realSockets.values()) {
      if (rs.socket) {
        rs.socket.destroy();
        break;
      }
    }
  }

  function stopClient() {
    if (clientProxy) {
      clientProxy.destroy();
      clientProxy = null;
    }
    if (virtualSocket) {
      virtualSocket.destroy();
      virtualSocket = null;
    }
    clientReady = false;
  }

  function isClientConnected() {
    return virtualSocket && virtualSocket.isConnected();
  }

  async function cleanup() {
    stopClient();
    mockTarget.forceCloseAllSockets?.();
    await Promise.all([
      new Promise(r => tlsServer.close(r)),
      new Promise(r => httpServer.close(r)),
      new Promise(r => mockTarget.close(r))
    ]);
  }

  return {
    ports: { tlsPort, httpPort, targetPort },
    certs,
    servers: { tlsServer, httpServer, mockTarget },
    connectionPool,
    virtualSocket: () => virtualSocket,
    clientProxy: () => clientProxy,
    startClient,
    stopClient,
    disconnectClient,
    isClientConnected,
    isConnected: () => clientReady,
    cleanup
  };
}

function httpRequest(options) {
  const { request } = require('node:http');
  return new Promise((resolve, reject) => {
    const req = request(options, (res) => {
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => {
        resolve({
          statusCode: res.statusCode,
          headers: res.headers,
          body: Buffer.concat(chunks)
        });
      });
      res.on('error', reject);
    });
    req.on('error', reject);
    if (options.body) {
      req.write(options.body);
    }
    req.end();
  });
}

function httpRequestStream(options, onData) {
  const { request } = require('node:http');
  return new Promise((resolve, reject) => {
    const req = request(options, (res) => {
      res.on('data', onData);
      res.on('end', () => resolve({
        statusCode: res.statusCode,
        headers: res.headers
      }));
      res.on('error', reject);
    });
    req.on('error', reject);
    if (options.body) {
      req.write(options.body);
    }
    req.end();
  });
}

module.exports = {
  getPort,
  getCertPaths,
  createTestEnv,
  httpRequest,
  httpRequestStream,
  issueTestClientCertificate
};
