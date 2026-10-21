'use strict';
/* eslint-disable no-console */
// A random API-sequence fuzzer for the reference implementation.
//
// Each run builds streams with randomly-behaving underlying sources/sinks/transformers, then performs a random sequence
// of public API calls (reads, writes, cancels, aborts, releaseLock, pipes, tees, controller calls, BYOB responses,
// resolving or rejecting the promises returned by user callbacks...), interleaved with microtask and macrotask
// flushes. User callbacks may also re-enter the API synchronously.
//
// The oracle is the reference implementation's own assertions (including the "promise is still pending" assertions in
// resolvePromise()/rejectPromise()), plus a few cheap invariants checked from the outside. Every failed assertion is
// recorded, even if the AssertionError is later swallowed by a promise chain.
//
// Usage: node fuzz/fuzz.js [--seed=N] [--runs=N] [--steps=N] [--verbose]
//   (run `node compile-idl.js` first so that ../generated/ exists)

const Module = require('module');
const path = require('path');
const realAssert = require('assert');

// Intercept require('assert') from the reference implementation so that we see every failed assertion.
let assertionFailures = [];
function recordingAssert(...args) {
  try {
    return realAssert(...args);
  } catch (e) {
    assertionFailures.push(e);
    throw e;
  }
}
Object.assign(recordingAssert, realAssert);
recordingAssert.AssertionError = realAssert.AssertionError;
const originalRequire = Module.prototype.require;
Module.prototype.require = function (id, ...rest) {
  if (id === 'assert') {
    return recordingAssert;
  }
  return originalRequire.call(this, id, ...rest);
};

const interfaces = [
  'ByteLengthQueuingStrategy', 'CountQueuingStrategy',
  'ReadableStream', 'ReadableStreamDefaultReader', 'ReadableStreamBYOBReader', 'ReadableStreamDefaultController',
  'ReadableByteStreamController', 'ReadableStreamBYOBRequest',
  'WritableStream', 'WritableStreamDefaultWriter', 'WritableStreamDefaultController',
  'TransformStream', 'TransformStreamDefaultController'
];
const W = {};
for (const name of interfaces) {
  delete globalThis[name];
  // eslint-disable-next-line global-require
  require(path.resolve(__dirname, '../generated', `${name}.js`)).install(globalThis, ['Window']);
  W[name] = globalThis[name];
}

// An uncaught exception or unhandled rejection is either one of our assertion failures re-thrown by
// rethrowAssertionErrorRejection(), or something else that is worth knowing about.
const strayErrors = [];
process.on('uncaughtException', e => {
  if (!(e instanceof realAssert.AssertionError)) {
    strayErrors.push(e);
  }
});
process.on('unhandledRejection', e => {
  strayErrors.push(e);
});

function mulberry32(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6D2B79F5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const noop = () => {};

class Run {
  constructor(seed, maxSteps, verbose) {
    this.random = mulberry32(seed);
    this.maxSteps = maxSteps;
    this.verbose = verbose;
    this.log = [];
    this.depth = 0;
    this.nextChunk = 1;
    this.counters = {};
    this.invariantFailures = [];

    this.readables = [];
    this.writables = [];
    this.readers = [];
    this.byobReaders = [];
    this.writers = [];
    this.iterators = [];
    this.rsControllers = [];
    this.byteControllers = [];
    this.wsControllers = [];
    this.tsControllers = [];
    this.deferreds = [];
    this.abortControllers = [];
  }

  // ---- randomness helpers ----

  int(n) {
    return Math.floor(this.random() * n);
  }

  chance(p) {
    return this.random() < p;
  }

  pick(array) {
    return array[this.int(array.length)];
  }

  name(prefix, obj) {
    this.counters[prefix] = (this.counters[prefix] || 0) + 1;
    const label = `${prefix}${this.counters[prefix]}`;
    obj.__label = label;
    return label;
  }

  say(line) {
    this.log.push(`${'  '.repeat(this.depth)}${line}`);
    if (this.verbose) {
      console.log(this.log[this.log.length - 1]);
    }
  }

  // Calls fn, logging what happened. Promises get handlers attached so they never count as unhandled.
  call(description, fn) {
    let result;
    try {
      result = fn();
    } catch (e) {
      this.say(`${description} -> throws ${e && e.name}`);
      return undefined;
    }
    if (result && typeof result.then === 'function') {
      const id = this.name('p', result);
      this.say(`${id} = ${description}`);
      result.then(v => this.say(`${id} fulfilled ${describe(v)}`), e => this.say(`${id} rejected ${e && e.name}`));
    } else {
      this.say(`${description} -> ${describe(result)}`);
    }
    return result;
  }

  // ---- behaviour of user callbacks ----

  // Decide what a user callback (start, pull, write, transform, ...) does: maybe re-enter the API, then return
  // undefined, return a promise that a later step resolves or rejects, throw, or return a rejected promise.
  callback(label, reenter) {
    this.say(`${label} called`);
    this.depth++;
    try {
      if (reenter && this.depth <= 2) {
        let n = this.chance(0.5) ? this.int(3) : 0;
        while (n-- > 0) {
          reenter();
        }
      }
      const r = this.random();
      if (r < 0.4) {
        return undefined;
      }
      if (r < 0.85) {
        let resolve;
        let reject;
        const promise = new Promise((res, rej) => {
          resolve = res;
          reject = rej;
        });
        const d = { resolve, reject, settled: false };
        const id = this.name('d', d);
        this.deferreds.push(d);
        this.say(`${label} returns pending ${id}`);
        return promise;
      }
      if (r < 0.93) {
        this.say(`${label} throws`);
        throw new Error(`${label} threw`);
      }
      this.say(`${label} returns rejected promise`);
      return Promise.reject(new Error(`${label} rejected`));
    } finally {
      this.depth--;
    }
  }

  chunk() {
    return this.nextChunk++;
  }

  strategy() {
    const strategy = { highWaterMark: this.pick([0, 0, 1, 1, 2, 4]) };
    if (this.chance(0.3)) {
      strategy.size = () => {
        const r = this.random();
        if (r < 0.05) {
          this.say('size() throws');
          throw new Error('size threw');
        }
        if (r < 0.08) {
          return NaN;
        }
        return this.int(3);
      };
    }
    return strategy;
  }

  // ---- stream creation ----

  newReadable() {
    let controller;
    const label = {};
    const source = {
      start: c => {
        controller = c;
        this.rsControllers.push(c);
        c.__label = `${label.__label}.controller`;
        return this.callback(`${label.__label}.start`, () => this.randomAction());
      },
      pull: () => this.callback(`${label.__label}.pull`, () => this.controllerAction(controller)),
      cancel: () => this.callback(`${label.__label}.cancel`, () => this.randomAction())
    };
    this.name('rs', label);
    this.say(`${label.__label} = new ReadableStream()`);
    let rs;
    try {
      rs = new W.ReadableStream(source, this.strategy());
    } catch (e) {
      this.say(`  -> throws ${e.name}`);
      return;
    }
    rs.__label = label.__label;
    this.readables.push(rs);
  }

  newByteReadable() {
    let controller;
    const label = {};
    const source = {
      type: 'bytes',
      start: c => {
        controller = c;
        this.byteControllers.push(c);
        c.__label = `${label.__label}.controller`;
        return this.callback(`${label.__label}.start`, () => this.randomAction());
      },
      pull: () => this.callback(`${label.__label}.pull`, () => this.byteControllerAction(controller)),
      cancel: () => this.callback(`${label.__label}.cancel`, () => this.randomAction())
    };
    if (this.chance(0.4)) {
      source.autoAllocateChunkSize = this.pick([1, 2, 4]);
    }
    this.name('brs', label);
    this.say(`${label.__label} = new ReadableStream({ type: 'bytes', autoAllocateChunkSize: ` +
             `${source.autoAllocateChunkSize} })`);
    let rs;
    try {
      rs = new W.ReadableStream(source, { highWaterMark: this.pick([0, 0, 1, 4]) });
    } catch (e) {
      this.say(`  -> throws ${e.name}`);
      return;
    }
    rs.__label = label.__label;
    rs.__bytes = true;
    this.readables.push(rs);
  }

  newWritable() {
    const label = {};
    const sink = {
      start: c => {
        this.wsControllers.push(c);
        c.__label = `${label.__label}.controller`;
        return this.callback(`${label.__label}.start`, () => this.randomAction());
      },
      write: chunk => this.callback(`${label.__label}.write(${chunk})`, () => this.randomAction()),
      close: () => this.callback(`${label.__label}.close`, () => this.randomAction()),
      abort: () => this.callback(`${label.__label}.abort`, () => this.randomAction())
    };
    this.name('ws', label);
    this.say(`${label.__label} = new WritableStream()`);
    let ws;
    try {
      ws = new W.WritableStream(sink, this.strategy());
    } catch (e) {
      this.say(`  -> throws ${e.name}`);
      return;
    }
    ws.__label = label.__label;
    this.writables.push(ws);
  }

  newTransform() {
    let controller;
    const label = {};
    const transformer = {
      start: c => {
        controller = c;
        this.tsControllers.push(c);
        c.__label = `${label.__label}.controller`;
        return this.callback(`${label.__label}.start`, () => this.randomAction());
      },
      transform: chunk => this.callback(`${label.__label}.transform(${chunk})`, () => {
        if (this.chance(0.7)) {
          this.call(`${label.__label}.controller.enqueue()`, () => controller.enqueue(this.chunk()));
        } else {
          this.randomAction();
        }
      }),
      flush: () => this.callback(`${label.__label}.flush`, () => this.tsControllerAction(controller)),
      cancel: () => this.callback(`${label.__label}.cancel`, () => this.randomAction())
    };
    this.name('ts', label);
    this.say(`${label.__label} = new TransformStream()`);
    let ts;
    try {
      ts = new W.TransformStream(transformer, this.strategy(), this.strategy());
    } catch (e) {
      this.say(`  -> throws ${e.name}`);
      return;
    }
    ts.readable.__label = `${label.__label}.readable`;
    ts.writable.__label = `${label.__label}.writable`;
    this.readables.push(ts.readable);
    this.writables.push(ts.writable);
  }

  // ---- actions ----

  controllerAction(c) {
    const l = c.__label;
    switch (this.int(4)) {
      case 0:
      case 1:
        this.call(`${l}.enqueue()`, () => c.enqueue(this.chunk()));
        break;
      case 2:
        this.call(`${l}.close()`, () => c.close());
        break;
      default:
        this.call(`${l}.error()`, () => c.error(new Error('controller.error')));
    }
  }

  byteControllerAction(c) {
    const l = c.__label;
    const req = c.byobRequest;
    switch (this.int(req ? 7 : 4)) {
      case 0:
      case 1:
        this.call(`${l}.enqueue()`, () => c.enqueue(new Uint8Array(1 + this.int(4))));
        break;
      case 2:
        this.call(`${l}.close()`, () => c.close());
        break;
      case 3:
        this.call(`${l}.error()`, () => c.error(new Error('controller.error')));
        break;
      case 4: {
        const n = this.int(req.view ? req.view.byteLength + 1 : 2);
        this.call(`${l}.byobRequest.respond(${n})`, () => req.respond(n));
        break;
      }
      case 5: {
        if (!req.view) {
          break;
        }
        const v = req.view;
        const n = this.int(v.byteLength + 1);
        this.call(`${l}.byobRequest.respondWithNewView(${n} bytes)`,
                  () => req.respondWithNewView(new Uint8Array(v.buffer, v.byteOffset, n)));
        break;
      }
      default:
        if (req.view && this.chance(0.3)) {
          this.call(`${l}.byobRequest.view.buffer.transfer()`, () => req.view.buffer.transfer());
        }
    }
  }

  tsControllerAction(c) {
    const l = c.__label;
    switch (this.int(3)) {
      case 0:
        this.call(`${l}.enqueue()`, () => c.enqueue(this.chunk()));
        break;
      case 1:
        this.call(`${l}.terminate()`, () => c.terminate());
        break;
      default:
        this.call(`${l}.error()`, () => c.error(new Error('ts controller.error')));
    }
  }

  checkedRead(reader, description, fn) {
    const p = this.call(description, fn);
    if (p) {
      p.then(result => {
        if (!result || typeof result !== 'object' || typeof result.done !== 'boolean') {
          this.invariantFailures.push(`${description} fulfilled with non-ReadableStreamReadResult`);
          return;
        }
        if (result.done) {
          reader.__done = true;
        } else if (reader.__done && !reader.__released) {
          this.invariantFailures.push(`${description} returned a chunk after the reader already saw done`);
        }
        if (typeof result.value === 'number') {
          if (reader.__last !== undefined && result.value <= reader.__last) {
            this.invariantFailures.push(`${description} returned ${result.value} after ${reader.__last}`);
          }
          reader.__last = result.value;
        }
      }, noop);
    }
  }

  randomAction() {
    const actions = [
      [1, () => this.newReadable()],
      [1, () => this.newByteReadable()],
      [1, () => this.newWritable()],
      [0.5, () => this.newTransform()]
    ];
    if (this.readables.length > 0) {
      actions.push([2, () => {
        const rs = this.pick(this.readables);
        const byob = rs.__bytes && this.chance(0.5);
        const reader = this.call(`${rs.__label}.getReader(${byob ? 'byob' : ''})`,
                                 () => rs.getReader(byob ? { mode: 'byob' } : undefined));
        if (reader) {
          this.name(byob ? 'br' : 'r', reader);
          this.say(`  (= ${reader.__label})`);
          (byob ? this.byobReaders : this.readers).push(reader);
        }
      }]);
      actions.push([0.5, () => {
        const rs = this.pick(this.readables);
        this.call(`${rs.__label}.cancel()`, () => rs.cancel(new Error('rs.cancel')));
      }]);
      actions.push([0.5, () => {
        const rs = this.pick(this.readables);
        const branches = this.call(`${rs.__label}.tee()`, () => rs.tee());
        if (branches) {
          branches.forEach((b, i) => {
            b.__label = `${rs.__label}.tee${i + 1}`;
            b.__bytes = rs.__bytes;
            this.readables.push(b);
          });
        }
      }]);
      actions.push([0.5, () => {
        const rs = this.pick(this.readables);
        const it = this.call(`${rs.__label}.values()`, () => rs.values({ preventCancel: this.chance(0.5) }));
        if (it) {
          this.name('it', it);
          this.iterators.push(it);
        }
      }]);
    }
    if (this.readables.length > 0 && this.writables.length > 0) {
      actions.push([1, () => {
        const rs = this.pick(this.readables);
        const ws = this.pick(this.writables);
        const options = {
          preventClose: this.chance(0.3),
          preventAbort: this.chance(0.3),
          preventCancel: this.chance(0.3)
        };
        if (this.chance(0.3)) {
          const ac = new AbortController();
          this.name('ac', ac);
          this.abortControllers.push(ac);
          options.signal = ac.signal;
        }
        this.call(`${rs.__label}.pipeTo(${ws.__label}, ${JSON.stringify(options)})`, () => rs.pipeTo(ws, options));
      }]);
    }
    if (this.readers.length > 0) {
      actions.push([3, () => {
        const r = this.pick(this.readers);
        this.checkedRead(r, `${r.__label}.read()`, () => r.read());
      }]);
      actions.push([1, () => {
        const r = this.pick(this.readers);
        r.__released = true;
        this.call(`${r.__label}.releaseLock()`, () => r.releaseLock());
      }]);
      actions.push([0.3, () => {
        const r = this.pick(this.readers);
        this.call(`${r.__label}.cancel()`, () => r.cancel(new Error('reader.cancel')));
      }]);
    }
    if (this.byobReaders.length > 0) {
      actions.push([3, () => {
        const r = this.pick(this.byobReaders);
        const n = 1 + this.int(4);
        const view = this.chance(0.7) ? new Uint8Array(n) : new Uint16Array(n);
        const options = this.chance(0.4) ? { min: 1 + this.int(n) } : undefined;
        this.checkedRead(r, `${r.__label}.read(${view.constructor.name}(${n}), ${JSON.stringify(options)})`,
                         () => r.read(view, options));
      }]);
      actions.push([1, () => {
        const r = this.pick(this.byobReaders);
        r.__released = true;
        this.call(`${r.__label}.releaseLock()`, () => r.releaseLock());
      }]);
      actions.push([0.3, () => {
        const r = this.pick(this.byobReaders);
        this.call(`${r.__label}.cancel()`, () => r.cancel(new Error('reader.cancel')));
      }]);
    }
    if (this.iterators.length > 0) {
      actions.push([1, () => {
        const it = this.pick(this.iterators);
        if (this.chance(0.8)) {
          this.call(`${it.__label}.next()`, () => it.next());
        } else {
          this.call(`${it.__label}.return()`, () => it.return());
        }
      }]);
    }
    if (this.writables.length > 0) {
      actions.push([2, () => {
        const ws = this.pick(this.writables);
        const w = this.call(`${ws.__label}.getWriter()`, () => ws.getWriter());
        if (w) {
          this.name('w', w);
          this.say(`  (= ${w.__label})`);
          this.writers.push(w);
          this.call(`${w.__label}.closed`, () => w.closed);
          this.call(`${w.__label}.ready`, () => w.ready);
        }
      }]);
      actions.push([0.5, () => {
        const ws = this.pick(this.writables);
        this.call(`${ws.__label}.abort()`, () => ws.abort(new Error('ws.abort')));
      }]);
      actions.push([0.3, () => {
        const ws = this.pick(this.writables);
        this.call(`${ws.__label}.close()`, () => ws.close());
      }]);
    }
    if (this.writers.length > 0) {
      actions.push([3, () => {
        const w = this.pick(this.writers);
        this.call(`${w.__label}.write()`, () => w.write(this.chunk()));
      }]);
      actions.push([1, () => {
        const w = this.pick(this.writers);
        this.call(`${w.__label}.releaseLock()`, () => w.releaseLock());
      }]);
      actions.push([0.5, () => {
        const w = this.pick(this.writers);
        this.call(`${w.__label}.close()`, () => w.close());
      }]);
      actions.push([0.5, () => {
        const w = this.pick(this.writers);
        this.call(`${w.__label}.abort()`, () => w.abort(new Error('writer.abort')));
      }]);
      actions.push([0.5, () => {
        const w = this.pick(this.writers);
        this.call(`${w.__label}.ready`, () => w.ready);
        this.call(`${w.__label}.desiredSize`, () => w.desiredSize);
      }]);
    }
    if (this.rsControllers.length > 0) {
      actions.push([2, () => this.controllerAction(this.pick(this.rsControllers))]);
    }
    if (this.byteControllers.length > 0) {
      actions.push([2, () => this.byteControllerAction(this.pick(this.byteControllers))]);
    }
    if (this.tsControllers.length > 0) {
      actions.push([1, () => this.tsControllerAction(this.pick(this.tsControllers))]);
    }
    if (this.wsControllers.length > 0) {
      actions.push([0.5, () => {
        const c = this.pick(this.wsControllers);
        this.call(`${c.__label}.error()`, () => c.error(new Error('ws controller.error')));
      }]);
    }
    if (this.abortControllers.length > 0) {
      actions.push([0.5, () => {
        const ac = this.pick(this.abortControllers);
        this.call(`${ac.__label}.abort()`, () => ac.abort(new Error('signal aborted')));
      }]);
    }
    const pending = this.deferreds.filter(d => !d.settled);
    if (pending.length > 0) {
      actions.push([3, () => {
        const d = this.pick(pending);
        d.settled = true;
        if (this.chance(0.8)) {
          this.say(`resolve ${d.__label}`);
          d.resolve();
        } else {
          this.say(`reject ${d.__label}`);
          d.reject(new Error(`${d.__label} rejected`));
        }
      }]);
    }

    let total = 0;
    for (const [weight] of actions) {
      total += weight;
    }
    let r = this.random() * total;
    for (const [weight, action] of actions) {
      r -= weight;
      if (r < 0) {
        action();
        return;
      }
    }
  }

  async execute() {
    for (let step = 0; step < this.maxSteps; step++) {
      const r = this.random();
      if (r < 0.25) {
        const n = 1 + this.int(4);
        this.say(`-- flush ${n} microtasks`);
        for (let i = 0; i < n; i++) {
          await undefined;
        }
      } else if (r < 0.3) {
        this.say('-- flush macrotask');
        await new Promise(resolve => setTimeout(resolve, 0));
      } else {
        this.randomAction();
      }
      if (assertionFailures.length > 0) {
        return step;
      }
    }
    // Let everything settle.
    for (let i = 0; i < 3; i++) {
      await new Promise(resolve => setTimeout(resolve, 0));
    }
    return this.maxSteps;
  }
}

function describe(v) {
  if (v === undefined || v === null || typeof v === 'number' || typeof v === 'boolean') {
    return String(v);
  }
  if (v.__label) {
    return v.__label;
  }
  if (typeof v === 'object' && 'done' in v) {
    return `{ value: ${describe(v.value)}, done: ${v.done} }`;
  }
  if (ArrayBuffer.isView(v)) {
    return `${v.constructor.name}(${v.length})`;
  }
  if (Array.isArray(v)) {
    return `[${v.map(describe).join(', ')}]`;
  }
  return typeof v;
}

function firstFrame(e) {
  const lines = String(e.stack).split('\n');
  const frame = lines.find(l => l.includes('/lib/'));
  return frame ? frame.trim().replace(/^at /, '').replace(/\(.*\/lib\//, '(lib/') : '?';
}

async function runOne(seed, steps, verbose) {
  assertionFailures = [];
  strayErrors.length = 0;
  const run = new Run(seed, steps, verbose);
  await run.execute();
  // Give rethrowAssertionErrorRejection()'s setTimeout a chance to fire.
  await new Promise(resolve => setTimeout(resolve, 0));
  return {
    run,
    assertions: assertionFailures.slice(),
    invariants: run.invariantFailures.slice(),
    stray: strayErrors.slice()
  };
}

function parseArgs() {
  const args = { seed: 1, runs: 1000, steps: 60, verbose: false };
  for (const a of process.argv.slice(2)) {
    const m = /^--(\w+)(?:=(.*))?$/.exec(a);
    if (!m) {
      continue;
    }
    args[m[1]] = m[2] === undefined ? true : Number(m[2]);
  }
  return args;
}

async function main() {
  const args = parseArgs();
  const signatures = new Map();
  for (let i = 0; i < args.runs; i++) {
    const seed = args.seed + i;
    const result = await runOne(seed, args.steps, args.verbose);
    const problems = [
      ...result.assertions.map(e => `assertion failed at ${firstFrame(e)}`),
      ...result.invariants.map(s => `invariant: ${s}`),
      ...result.stray.map(e => `stray error: ${e && e.name}: ${e && e.message} at ${firstFrame(e)}`)
    ];
    if (problems.length === 0) {
      continue;
    }
    const signature = problems[0];
    if (!signatures.has(signature)) {
      // Find the shortest prefix of this seed's action sequence that still fails, for a readable repro.
      let lo = 1;
      let hi = args.steps;
      while (lo < hi) {
        const mid = (lo + hi) >> 1;
        const r = await runOne(seed, mid, false);
        if (r.assertions.length + r.invariants.length + r.stray.length > 0) {
          hi = mid;
        } else {
          lo = mid + 1;
        }
      }
      const minimal = await runOne(seed, lo, false);
      const firstError = minimal.assertions[0] || minimal.stray[0];
      const stack = firstError ? String(firstError.stack).split('\n').filter(l => l.includes('/lib/')).slice(0, 8) : [];
      signatures.set(signature, { seed, steps: lo, count: 0, log: minimal.run.log, problems, stack });
    }
    signatures.get(signature).count++;
  }

  if (signatures.size === 0) {
    console.log(`No problems found in ${args.runs} runs of ${args.steps} steps.`);
    return;
  }
  for (const [signature, info] of signatures) {
    console.log(`\n=== ${signature}  (${info.count} runs; smallest repro: --seed=${info.seed} --runs=1 ` +
                `--steps=${info.steps})`);
    for (const p of info.problems.slice(1, 4)) {
      console.log(`    also: ${p}`);
    }
    for (const frame of info.stack) {
      console.log(`    ${frame.trim()}`);
    }
    console.log(info.log.join('\n'));
  }
  process.exitCode = 1;
}

main().catch(e => {
  console.error(e);
  process.exitCode = 1;
});
