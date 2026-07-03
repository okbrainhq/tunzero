#!/usr/bin/env node
// Client Entry Point - TLS with multipath support

const { createProxy } = require('./lib/proxy');
const { VirtualSocket } = require('./lib/virtual-socket');

const DEFAULT_KEY = './.certs/client-key.pem';
const DEFAULT_CERT = './.certs/client-cert.pem';
const DEFAULT_CA = './.ca/ca-cert.pem';

function parseArgs(argv) {
  const args = argv || process.argv.slice(2);
  const options = {
    serverHost: 'localhost',
    serverPort: 9443,
    targetHost: 'localhost',
    targetPort: 3000,
    clientKey: DEFAULT_KEY,
    clientCert: DEFAULT_CERT,
    caCert: DEFAULT_CA,
    domains: [],
    preserveHost: false,
    targetTimeout: 30000,
    targetKeepAliveTimeout: 60 * 60 * 1000,
    parallelSockets: 1
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    switch (arg) {
      case '--server':
        const server = args[++i];
        if (!server.includes(':')) {
          console.error(`Error: --server requires host:port format (e.g., localhost:8080)`);
          process.exit(1);
        }
        const [host, port] = server.split(':');
        options.serverHost = host;
        const serverPort = parseInt(port, 10);
        if (isNaN(serverPort) || serverPort < 1 || serverPort > 65535) {
          console.error(`Error: Invalid server port ${port}. Must be 1-65535.`);
          process.exit(1);
        }
        options.serverPort = serverPort;
        break;
      case '--target':
        const target = args[++i];
        if (!target.includes(':')) {
          console.error(`Error: --target requires host:port format (e.g., localhost:3000)`);
          process.exit(1);
        }
        const [tHost, tPort] = target.split(':');
        options.targetHost = tHost;
        const targetPort = parseInt(tPort, 10);
        if (isNaN(targetPort) || targetPort < 1 || targetPort > 65535) {
          console.error(`Error: Invalid target port ${tPort}. Must be 1-65535.`);
          process.exit(1);
        }
        options.targetPort = targetPort;
        break;
      case '--target-timeout':
        const targetTimeoutValue = args[++i];
        const targetTimeout = parseInt(targetTimeoutValue, 10);
        if (isNaN(targetTimeout) || targetTimeout < 0) {
          console.error(`Error: Invalid target timeout ${targetTimeoutValue}. Must be 0 or greater.`);
          process.exit(1);
        }
        options.targetTimeout = targetTimeout;
        break;
      case '--target-keepalive-timeout':
        const targetKeepAliveTimeoutValue = args[++i];
        const targetKeepAliveTimeout = parseInt(targetKeepAliveTimeoutValue, 10);
        if (isNaN(targetKeepAliveTimeout) || targetKeepAliveTimeout < 0) {
          console.error(`Error: Invalid target keepalive timeout ${targetKeepAliveTimeoutValue}. Must be 0 or greater.`);
          process.exit(1);
        }
        options.targetKeepAliveTimeout = targetKeepAliveTimeout;
        break;
      case '--parallel-sockets':
        const parallelSocketsValue = args[++i];
        if (!/^[0-9]+$/.test(parallelSocketsValue || '')) {
          console.error(`Error: Invalid parallel sockets ${parallelSocketsValue}. Must be an integer from 1 to 32.`);
          process.exit(1);
        }
        const parallelSockets = parseInt(parallelSocketsValue, 10);
        if (parallelSockets < 1 || parallelSockets > 32) {
          console.error(`Error: Invalid parallel sockets ${parallelSocketsValue}. Must be an integer from 1 to 32.`);
          process.exit(1);
        }
        options.parallelSockets = parallelSockets;
        break;
      case '--key':
        options.clientKey = args[++i];
        break;
      case '--cert':
        options.clientCert = args[++i];
        break;
      case '--ca':
        options.caCert = args[++i];
        break;
      case '--multipath':
        process.env.MULTIPATH_ENABLED = 'true';
        break;
      case '--domain':
        options.domains.push(args[++i]);
        break;
      case '--preserve-host':
        options.preserveHost = true;
        break;
      case '--help':
        console.log(`
Usage: node index.js [options]

Options:
  --server <host:port>    Tunnel server address (default: localhost:9443)
  --target <host:port>    Local target service (default: localhost:3000)
  --target-timeout <ms>   Target response/upgrade timeout; 0 disables (default: 30000)
  --target-keepalive-timeout <ms> Target idle keep-alive timeout; 0 disables idle expiry (default: 3600000)
  --parallel-sockets <n>  Parallel tunnel sockets per interface, 1-32 (default: 1)
  --key <path>            Client private key (default: ${DEFAULT_KEY})
  --cert <path>           Client certificate (default: ${DEFAULT_CERT})
  --ca <path>             CA certificate to verify server (default: ${DEFAULT_CA})
  --multipath             Enable multipath (multiple network interfaces)
  --domain <domain>       Optional authorized domain subset (repeatable)
  --preserve-host         Forward original public Host header to target
  --help                  Show this help
        `);
        process.exit(0);
    }
  }

  return options;
}

function main() {
  const config = parseArgs();

  console.log('Starting TLS tunnel client...');
  console.log(`Server: ${config.serverHost}:${config.serverPort}`);
  console.log(`Target: ${config.targetHost}:${config.targetPort}`);
  console.log(`Target timeout: ${config.targetTimeout === 0 ? 'disabled' : `${config.targetTimeout}ms`}`);
  console.log(`Target keep-alive timeout: ${config.targetKeepAliveTimeout === 0 ? 'disabled' : `${config.targetKeepAliveTimeout}ms`}`);
  console.log(`Parallel sockets per interface: ${config.parallelSockets}`);
  console.log(`Multipath: ${process.env.MULTIPATH_ENABLED === 'true' ? 'enabled' : 'disabled (use --multipath to enable)'}`);

  let proxy = null;
  let isReady = false;

  const vs = new VirtualSocket(config);

  vs.on('ready', () => {
    isReady = true;
    console.log('Connected to TLS tunnel server (multipath ready)');
    proxy = createProxy(vs, config.targetPort, config.targetHost, vs.maxConcurrentStreams, {
      preserveHost: config.preserveHost,
      targetTimeout: config.targetTimeout,
      targetKeepAliveTimeout: config.targetKeepAliveTimeout
    });
  });

  vs.on('socketConnected', (interfaceName) => {
    console.log(`[${new Date().toISOString()}] [virtual-socket] Interface ${interfaceName} connected`);
  });

  vs.on('frame', (frame) => {
    if (proxy) {
      proxy.handleFrame(frame);
    }
  });

  vs.on('error', (err) => {
    console.error('VirtualSocket error:', err.message);
    if (!isReady) {
      console.error('Failed to connect to server. Exiting...');
      process.exit(1);
    }
  });

  vs.start();

  process.on('SIGINT', () => {
    console.log('\nShutting down...');
    if (proxy) proxy.destroy();
    vs.destroy();
    process.exit(0);
  });

  process.on('SIGTERM', () => {
    console.log('\nShutting down...');
    if (proxy) proxy.destroy();
    vs.destroy();
    process.exit(0);
  });
}

if (require.main === module) {
  main();
}

module.exports = { main, parseArgs };
