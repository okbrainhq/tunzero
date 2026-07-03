// Unit tests for parseArgs functionality

const { describe, it } = require('node:test');
const assert = require('node:assert');
const { parseArgs: parseServerArgs } = require('../../apps/server/index.js');
const { parseArgs: parseClientArgs } = require('../../apps/client/index.js');
const { shouldUseSingleFlow } = require('../../apps/server/lib/http-router.js');

describe('parseArgs defaults', () => {
  it('should default stream timeout to 5 minutes', () => {
    const opts = parseServerArgs([]);
    assert.strictEqual(opts.streamTimeout, 300000);
  });
});

describe('parseArgs --max-body-size', () => {
  it('should default maxBodySize to undefined', () => {
    const opts = parseServerArgs([]);
    assert.strictEqual(opts.maxBodySize, undefined);
  });

  it('should parse a valid --max-body-size value', () => {
    const opts = parseServerArgs(['--max-body-size', '230686720']);
    assert.strictEqual(opts.maxBodySize, 230686720);
  });

  it('should reject zero', () => {
    // parseArgs calls process.exit(1) on invalid input; test via try/catch
    // but since process.exit won't be called by default in a test, 
    // we verify the behavior by checking that invalid input is guarded
    const exitSpy = { called: false };
    const origExit = process.exit;
    process.exit = (code) => { exitSpy.called = true; exitSpy.code = code; throw new Error('exit'); };
    try {
      try {
        parseServerArgs(['--max-body-size', '0']);
      } catch (e) {
        // expected - process.exit throws
      }
      assert.ok(exitSpy.called, 'Should call process.exit for 0');
      assert.strictEqual(exitSpy.code, 1);
    } finally {
      process.exit = origExit;
    }
  });

  it('should reject NaN / non-numeric values', () => {
    const exitSpy = { called: false };
    const origExit = process.exit;
    process.exit = (code) => { exitSpy.called = true; exitSpy.code = code; throw new Error('exit'); };
    try {
      try {
        parseServerArgs(['--max-body-size', 'abc']);
      } catch (e) {
        // expected
      }
      assert.ok(exitSpy.called, 'Should call process.exit for non-numeric');
      assert.strictEqual(exitSpy.code, 1);
    } finally {
      process.exit = origExit;
    }
  });

  it('should reject negative numbers', () => {
    const exitSpy = { called: false };
    const origExit = process.exit;
    process.exit = (code) => { exitSpy.called = true; exitSpy.code = code; throw new Error('exit'); };
    try {
      try {
        parseServerArgs(['--max-body-size', '-1']);
      } catch (e) {
        // expected
      }
      assert.ok(exitSpy.called, 'Should call process.exit for negative');
      assert.strictEqual(exitSpy.code, 1);
    } finally {
      process.exit = origExit;
    }
  });

  it('should reject floats', () => {
    const exitSpy = { called: false };
    const origExit = process.exit;
    process.exit = (code) => { exitSpy.called = true; exitSpy.code = code; throw new Error('exit'); };
    try {
      try {
        parseServerArgs(['--max-body-size', '1024.5']);
      } catch (e) {
        // expected
      }
      assert.ok(exitSpy.called, 'Should call process.exit for float');
      assert.strictEqual(exitSpy.code, 1);
    } finally {
      process.exit = origExit;
    }
  });

  it('should reject trailing garbage', () => {
    const exitSpy = { called: false };
    const origExit = process.exit;
    process.exit = (code) => { exitSpy.called = true; exitSpy.code = code; throw new Error('exit'); };
    try {
      try {
        parseServerArgs(['--max-body-size', '123abc']);
      } catch (e) {
        // expected
      }
      assert.ok(exitSpy.called, 'Should call process.exit for trailing garbage');
      assert.strictEqual(exitSpy.code, 1);
    } finally {
      process.exit = origExit;
    }
  });

  it('should accept large values like 220MB', () => {
    const opts = parseServerArgs(['--max-body-size', String(220 * 1024 * 1024)]);
    assert.strictEqual(opts.maxBodySize, 220 * 1024 * 1024);
  });

  it('should reject unsafe integers', () => {
    const exitSpy = { called: false };
    const origExit = process.exit;
    process.exit = (code) => { exitSpy.called = true; exitSpy.code = code; throw new Error('exit'); };
    try {
      try {
        parseServerArgs(['--max-body-size', String(Number.MAX_SAFE_INTEGER + 1)]);
      } catch (e) {
        // expected
      }
      // Number.MAX_SAFE_INTEGER + 1 cannot be represented exactly, so parseInt may give a different value
      // but the check uses Number.isSafeInteger which should catch it
      assert.ok(exitSpy.called, 'Should reject unsafe integers');
    } finally {
      process.exit = origExit;
    }
  });
});

describe('single-flow feature detection', () => {
  it('should not use path-only routing', () => {
    const opts = parseServerArgs([]);
    assert.strictEqual(Object.prototype.hasOwnProperty.call(opts, 'singleFlowPathPrefixes'), false);
    assert.ok(!shouldUseSingleFlow('/uploads/plain-json', { accept: '*/*' }, 'GET'));
  });

  it('should detect media playback/download requests', () => {
    assert.ok(shouldUseSingleFlow('/uploads/audio.m4a', { accept: '*/*' }, 'GET'));
    assert.ok(shouldUseSingleFlow('/songs/track.mp3', {}, 'GET'));
    assert.ok(shouldUseSingleFlow('/api/file', { accept: 'audio/*' }, 'GET'));
    assert.ok(shouldUseSingleFlow('/video', { 'sec-fetch-dest': 'video' }, 'GET'));
    assert.ok(shouldUseSingleFlow('/api/file', { range: 'bytes=0-' }, 'GET'));
  });

  it('should detect upload-style request bodies', () => {
    assert.ok(shouldUseSingleFlow('/api/upload', { 'content-type': 'multipart/form-data; boundary=abc' }, 'POST'));
    assert.ok(shouldUseSingleFlow('/api/upload', { 'content-type': 'application/octet-stream' }, 'PUT'));
    assert.ok(shouldUseSingleFlow('/api/resumable-upload/3817dd59-b331-400e-9213-e3dcf8f75c4c', {
      'content-type': 'application/octet-stream',
      'upload-offset': '3145728'
    }, 'PATCH'));
    assert.ok(shouldUseSingleFlow('/api/upload', { 'content-range': 'bytes 0-99/100' }, 'PATCH'));
    assert.ok(shouldUseSingleFlow('/api/upload', { 'upload-offset': '3145728' }, 'PATCH'));
    assert.ok(shouldUseSingleFlow('/api/upload', { 'tus-resumable': '1.0.0' }, 'PATCH'));
    assert.ok(!shouldUseSingleFlow('/api/upload', { 'content-type': 'application/json' }, 'POST'));
  });
});

describe('client parseArgs --parallel-sockets', () => {
  it('should default parallel sockets to 1', () => {
    const opts = parseClientArgs([]);
    assert.strictEqual(opts.parallelSockets, 1);
  });

  it('should parse a valid --parallel-sockets value', () => {
    const opts = parseClientArgs(['--parallel-sockets', '4']);
    assert.strictEqual(opts.parallelSockets, 4);
  });

  it('should reject invalid --parallel-sockets values', () => {
    const exitSpy = { called: false };
    const origExit = process.exit;
    process.exit = (code) => { exitSpy.called = true; exitSpy.code = code; throw new Error('exit'); };
    try {
      try {
        parseClientArgs(['--parallel-sockets', '0']);
      } catch (e) {
        // expected
      }
      assert.ok(exitSpy.called, 'Should call process.exit for 0');
      assert.strictEqual(exitSpy.code, 1);
    } finally {
      process.exit = origExit;
    }
  });
});
