# Writable Byte Streams Explainer


## Introduction

Readable byte streams let a consumer bring its own buffer: `reader.read(view)` hands a buffer to
the underlying source, the source fills it, and the consumer gets the same memory back. A consumer
can cycle a single buffer forever, with no copies and no allocation.

Writable streams have no equivalent. A producer that writes bytes has no defined moment at which its
buffer becomes safe to touch again, and an underlying sink that wants to take ownership of a chunk
has to transfer it — which detaches the producer's buffer at an unpredictable time and gives the
producer nothing back. This is [whatwg/streams#495](https://github.com/whatwg/streams/issues/495),
with earlier discussion in [#465](https://github.com/whatwg/streams/issues/465). The canonical
example:

```javascript
async function writeRandomBytesForever(writableStream) {
  const writer = writableStream.getWriter();
  while (true) {
    const bytes = new Uint8Array(1024);
    crypto.getRandomValues(bytes);
    const promise = writer.write(bytes);
    doStuffWith(bytes);
    await promise;
  }
}
```

Two things are wrong here. `doStuffWith(bytes)` may or may not be looking at memory the sink is
already reading, depending on how full the queue happened to be — a race that the standard can only
address with [an informal
contract](https://streams.spec.whatwg.org/#write-mutable-chunks) asking producers to behave. And a
fresh 1024-byte buffer is allocated on every iteration, because there is no way to get the previous
one back.

This explainer proposes **writable byte streams**: `new WritableStream({ type: "bytes" })`, plus a
BYOB writer, which together give bytes written to a stream the same ownership and reuse story that
readable byte streams give bytes read from one.


## Model: which way buffers flow

The useful way to think about the readable side is that *data flows forwards and empty buffers flow
backwards*. A BYOB read sends an empty buffer from the consumer to the underlying source; the source
fills it; the full buffer travels back. The buffer belongs to the data's *destination*, and it makes
a round trip.

Writable streams should work the same way, with the roles reversed: the destination is now the
underlying sink, so **empty buffers originate at the sink and flow backwards to the producer**, and
full buffers flow forwards. Concretely, a writable byte stream owns a queue of free buffers, and:

*   the sink puts buffers into that queue — either the ones it just finished writing, or ones it
    allocated itself;
*   the producer takes buffers out of it, fills them, and writes them, which puts them back in the
    stream's hands.

The 2016 discussion split this into two features, nicknamed KYOB ("keep your own buffer" — the
producer wants its buffer back) and PUTB ("please use this buffer" — the sink wants the producer to
write into memory the sink chose). They are the same mechanism seen from opposite ends: one queue of
free buffers, differing only in who allocated what is in it. Treating them as one thing is what
makes buffers compose across a pipe chain, rather than stopping at each stream boundary.


## The duality

Reversing the direction of data flow maps each readable-side role onto a writable-side one. The
consumer and the underlying sink are both *destinations*, so they play the same role; the
underlying source and the producer are both *origins*.

| Readable byte stream | Writable byte stream |
| --- | --- |
| `new ReadableStream({ type: "bytes" })` | `new WritableStream({ type: "bytes" })` |
| `stream.getReader({ mode: "byob" })` | `stream.getWriter({ mode: "byob" })` |
| **`ReadableStreamBYOBReader`** (destination) | **`WritableByteStreamController`** (destination) |
| `reader.read(view): Promise<View>`<br>Lends a buffer; returns the transferred view when done reading. | `sink.write(view): Promise<View \| undefined \| null>`<br>Receives a buffer; returns the (transferred) view when done writing. `undefined` means "take the original back"; `null` means "I kept it". |
| `reader.cancel(reason)` | `controller.signal`, `sink.abort(reason)` |
| **`ReadableByteStreamController`** (origin) | **`WritableStreamBYOBWriter`** (origin) |
| `source.pull()`<br>Called when more data is needed. | `writer.ready`<br>Resolves when more data is desired. |
| `controller.enqueue(view)`<br>Enqueues a non-BYOB chunk. Transfers the entire chunk. | `writer.write(view): Promise<undefined>`<br>Writes a chunk. Transfers the entire chunk. |
| `controller.byobRequest`<br>The destination's buffer, lent to the origin. | `writer.requestBuffer(minSize): Promise<Uint8Array>`<br>The destination's buffer, lent to the origin. |
| `byobRequest.respond(numBytes)`<br>Enqueues `numBytes` of the BYOB view. | `writer.write(view.subarray(0, numBytes))`<br>Writes `numBytes` of the lent view. |
| `byobRequest.respondWithNewView(view)` | `writer.write(view)`, where `view` is over the lent buffer |
| `source.autoAllocateChunkSize` | `sink.autoAllocateChunkSize` |
| `controller.desiredSize` | `writer.desiredSize` |
| `controller.close()` | `writer.close()` |
| `controller.error(e)` | `writer.abort(reason)`, `controller.error(e)` |

Two rows deserve comment, because they are where the writable side is deliberately *not* a
mechanical mirror.

The origin side of a readable byte stream is the underlying source, which gets the low-level,
synchronous `byobRequest`/`respond()` API. On the writable side the origin is the *producer* —
ordinary web developer code, the counterpart of `reader.read()` rather than of `pull()`. It gets a
promise-returning API instead: `requestBuffer()` to take a buffer out of the free-buffer queue, and
`write()` to hand it over. The synchronous `byobRequest`/`respond()` shape remains available as an
alternative (see [Alternatives](#alternatives)), and is a fair description of the underlying
machinery, but it is not what most producers should have to write.

Conversely the destination side of a writable byte stream is the underlying sink, so the buffer
round trip that the readable side expresses as `reader.read(view)` returning a view is expressed
here as `sink.write(view)` *fulfilling* with a view. The sink already returns a promise to say "I am
done with this chunk"; making that promise's value meaningful is enough, and no new method is
needed.


## API proposed

### Creating a writable byte stream

`UnderlyingSink`'s `type` member, [currently reserved for exactly this
purpose](https://streams.spec.whatwg.org/#dom-underlyingsink-type), accepts `"bytes"`:

```javascript
const writableStream = new WritableStream({
  type: "bytes",

  start(controller) {
    // Optional: seed the free-buffer queue with memory of the sink's choosing (PUTB).
    controller.recycle(new Uint8Array(65536));
    controller.recycle(new Uint8Array(65536));
  },

  async write(view, controller) {
    await sendSomewhere(view);
    // Returning undefined hands this buffer back to the producer.
  },

  // Optional: let the stream allocate buffers when the sink does not supply any.
  autoAllocateChunkSize: 65536
});
```

*   The `controller` passed to `start()` and `write()` is a `WritableByteStreamController` rather
    than a `WritableStreamDefaultController`, mirroring how `type: "bytes"` changes the controller on
    the readable side.
*   Only `ArrayBufferView` chunks may be written; anything else is a `TypeError`, as it already is
    for `ReadableByteStreamController.enqueue()`.
*   `autoAllocateChunkSize` means the same thing it means on `UnderlyingSource`: it lets the stream
    allocate buffers so that the other end always has one available. On the readable side that keeps
    `controller.byobRequest` populated for sources when the consumer uses a default reader; here it
    keeps `writer.requestBuffer()` answerable for producers when the sink does not lend memory.

### Writing with a BYOB writer

```javascript
const writer = writableStream.getWriter({ mode: "byob" });

const view = await writer.requestBuffer(1024);   // an empty buffer, at least 1024 bytes
crypto.getRandomValues(view);
await writer.write(view);                        // transfers it; `view` is now detached
```

`requestBuffer()` is where a producer waits: it settles when a buffer is available, which makes it
the natural backpressure signal, just as `read()` is on the readable side. `write()` keeps its
existing meaning and its existing return type, so a byte writable stream stays substitutable for an
ordinary one — a producer that does not care about reuse can use `getWriter()` and write its own
buffers, and everything still works, minus the recycling.

### Interface sketch

```webidl
dictionary UnderlyingSink {
  UnderlyingSinkStartCallback start;
  UnderlyingSinkWriteCallback write;
  UnderlyingSinkCloseCallback close;
  UnderlyingSinkAbortCallback abort;
  WritableStreamType type;
  [EnforceRange] unsigned long long autoAllocateChunkSize;
};

enum WritableStreamType { "bytes" };

// For byte sinks, `chunk` is a Uint8Array and the promise may fulfill with an ArrayBufferView
// (hand this buffer back), undefined (hand the original back), or null (nothing comes back).
callback UnderlyingSinkWriteCallback = Promise<any> (any chunk, WritableStreamController controller);

typedef (WritableStreamDefaultController or WritableByteStreamController) WritableStreamController;

[Exposed=*]
interface WritableByteStreamController {
  readonly attribute AbortSignal signal;
  readonly attribute unrestricted double? desiredSize;

  undefined recycle(ArrayBufferView view);
  undefined error(optional any e);
};

partial interface WritableStream {
  WritableStreamWriter getWriter(optional WritableStreamGetWriterOptions options = {});
};

typedef (WritableStreamDefaultWriter or WritableStreamBYOBWriter) WritableStreamWriter;

enum WritableStreamWriterMode { "byob" };

dictionary WritableStreamGetWriterOptions {
  WritableStreamWriterMode mode;
};

[Exposed=*]
interface WritableStreamBYOBWriter {
  constructor(WritableStream stream);

  readonly attribute Promise<undefined> closed;
  readonly attribute unrestricted double? desiredSize;
  readonly attribute Promise<undefined> ready;

  Promise<Uint8Array> requestBuffer(optional [EnforceRange] unsigned long long minSize = 1);
  Promise<undefined> write(ArrayBufferView view);

  Promise<undefined> abort(optional any reason);
  Promise<undefined> close();
  undefined releaseLock();
};
```

`controller.recycle(view)` exists for buffers that are not the natural return value of a `write()`
call: seeding a pool in `start()`, or handing back memory after the sink has already resolved. It is
the dual of `ReadableByteStreamController.enqueue()` — a push into the reverse-flowing channel.


## Buffer ownership

The rules, in full. They are what turn the current informal contract into something the standard can
actually enforce.

1.  **`writer.write(view)` transfers `view.buffer` immediately**, at call time — not when the sink
    eventually gets to it. The producer's view detaches synchronously, so the `doStuffWith(bytes)`
    race above stops being timing-dependent: whatever it does, it can no longer reach memory the
    sink is reading. What it does instead is what detaching always means — element writes are
    silently ignored, element reads give `undefined`, and any API that validates its argument
    (`crypto.getRandomValues()`, `TextEncoder.encodeInto()`, `TypedArray.prototype.set()`) throws a
    `TypeError`. This is the dual of `reader.read(view)`, which detaches immediately for the same
    reason.
2.  **The sink owns the view for the duration of its `write()` call.** It receives a fresh
    `Uint8Array` over the transferred memory, valid until the promise it returns settles.
3.  **The promise's fulfillment value says where the buffer goes.**
    *   `undefined` — the stream takes the original buffer back into the free-buffer queue. The view
        must not have been detached; if it has, that is a `TypeError` that errors the stream, in the
        same way that `byobRequest.respond()` refuses a detached view.
    *   an `ArrayBufferView` — that view's buffer is transferred out of the sink and queued instead.
        This is the `respondWithNewView()` analogue: it covers a sink that kept the written buffer
        and wants to lend different memory, or one that wants to hand back a different region.
    *   `null` — nothing comes back. This is the honest answer for a sink that transferred the chunk
        onwards, to a worker or to a platform API that takes ownership.
4.  **`writer.requestBuffer(minSize)`** fulfills with a `Uint8Array` over the first queued free
    buffer of at least `minSize` bytes, transferred to the producer. If none is available it
    allocates `autoAllocateChunkSize` bytes when the sink asked for that, and otherwise waits — the
    same way a BYOB read waits for a source that has not responded yet.
5.  **Partial fills are written as subarrays.** `writer.write(view.subarray(0, n))` transfers the
    whole underlying buffer and tells the sink that `n` bytes are meaningful, exactly as
    `respond(n)` does on the readable side.
6.  **On error or abort**, queued and in-flight buffers are dropped. Buffers the producer is
    currently holding stay with the producer; buffers the sink lent and never got back are lost to
    it until garbage collection. `writer.ready` and `writer.closed` reject, which is the producer's
    signal to stop and start over with fresh memory.
7.  **`releaseLock()` reclaims nothing.** A producer that took buffers out of the queue keeps them.
8.  **A default writer still works.** `getWriter()` on a byte writable stream writes views, which
    are still transferred (rule 1), but offers no way to get anything back.


## Examples

### Fixing the motivating example

The loop from the introduction, with no allocation after the first buffer and no race:

```javascript
async function writeRandomBytesForever(writableStream) {
  const writer = writableStream.getWriter({ mode: "byob" });
  while (true) {
    const bytes = await writer.requestBuffer(1024);
    crypto.getRandomValues(bytes);
    await writer.write(bytes);
    // `bytes` is detached here; the next requestBuffer() hands the same memory back.
  }
}
```

`doStuffWith(bytes)` after the `write()` can no longer reach the bytes the sink is reading, whatever
the queue happened to look like. Note that the loop is the same whether the sink lends its own
buffers or simply returns each one it is handed.

Awaiting each `write()` keeps exactly one buffer in flight. A producer that wants several in flight
at once just does not await it — `requestBuffer()` supplies the backpressure on its own, and a
failed write errors the stream, so `writer.ready` and `writer.closed` reject too. As with
`writer.write()` today, the ignored promise still wants a `.catch(() => {})` to avoid an
`unhandledrejection`.

### A sink that owns its memory

A sink writing into a fixed region — a ring buffer, memory shared with another thread, a
device-mapped page — can insist that producers write into that region, which is the PUTB half:

```javascript
function makeRingBufferStream(ring) {
  return new WritableStream({
    type: "bytes",

    start(controller) {
      for (const slot of ring.slots()) {
        controller.recycle(slot);   // hand the producer memory the device can read directly
      }
    },

    async write(view, controller) {
      await ring.commit(view);      // the bytes are already in the right place; nothing is copied
      return undefined;             // slot goes back into rotation
    }
  });
}
```

Because there is no `autoAllocateChunkSize`, `requestBuffer()` here can only ever hand out ring
slots: a producer cannot accidentally get memory the device cannot see, it just waits until a slot
frees up.

### Piping without copies

When a readable byte stream is piped to a writable byte stream, the pipe can source its read buffers
from the destination:

```javascript
await byteReadable.pipeTo(byteWritable);
```

Internally this becomes `requestBuffer()` → BYOB `read(view)` → `write(view)`, so a single set of
buffers circulates between the two underlying implementations for the life of the pipe, with no
copies and, once warm, no allocation.

`pipeTo()` already [chooses its reader at the user agent's
discretion](https://streams.spec.whatwg.org/#readable-stream-pipe-to) when the source is a byte
stream, and may already use a BYOB reader — but the buffers it reads into are the implementation's
own, so each one is handed to the sink and then dropped. The only new part here is where the pipe
gets those buffers from.

### Staying compatible

Nothing above is required. A byte writable stream accepts an ordinary writer:

```javascript
const writer = byteWritable.getWriter();
await writer.write(new Uint8Array([1, 2, 3]));
await writer.close();
```

The buffer is transferred (rule 1) and, when the sink is done, joins the free-buffer queue, where it
is available to any producer that later asks for one. Nothing is lost by not participating.


## Transform streams

`Transformer`'s `readableType` and `writableType` members are [reserved for this
purpose](https://streams.spec.whatwg.org/#dom-transformer-readabletype) too, and either side can opt
in independently:

*   `writableType: "bytes"` makes the writable half a writable byte stream. `transform(chunk,
    controller)` owns `chunk` for the duration of the call, and its promise's fulfillment value
    decides where that buffer goes, under the same three-way rule as a sink's `write()`.
*   `readableType: "bytes"` makes the readable half a readable byte stream, so
    `controller.byobRequest` is available and `controller.enqueue(view)` transfers.

### Can buffers cross from one side to the other?

Yes, in both directions, and this is where most of the value is.

**Forwards (writable side → readable side).** A transformer that works in place enqueues the buffer
it was given:

```javascript
const inPlaceCipher = new TransformStream({
  writableType: "bytes",
  readableType: "bytes",

  transform(chunk, controller) {
    for (let i = 0; i < chunk.byteLength; i++) {
      chunk[i] ^= 0x5a;
    }
    controller.enqueue(chunk);   // transfers; the buffer travels downstream
    return null;                 // ...so nothing goes back to the writable side's queue
  }
});
```

`enqueue()` transfers the buffer to the readable side, so it is no longer available to hand back to
the producer — which is exactly why `transform()` returns `null` here. That is correct but
one-directional: the buffer left the writable side's rotation, and unless something replenishes it,
the producer's next `requestBuffer()` has to allocate. (`enqueue()` is the right call here because
the readable side is still holding its own buffers; that changes below.)

**Backwards (readable side → writable side).** The replenishment is the readable side's own BYOB
requests. A buffer that the downstream consumer lends for a BYOB read is an empty buffer arriving at
the transform stream from the destination end — precisely what the writable side's free-buffer queue
is for. Offering it there closes the loop: the consumer's buffer is handed to the upstream producer,
the producer fills it, and it arrives back at the readable side already holding the bytes the
consumer asked for. One buffer, one circuit, no copies and no allocations anywhere in the chain.

The last step needs care, and it is the part that constrains the design. Once the pull-into buffer
has been lent out, the readable byte controller is holding a detached buffer — the same state it is
already in while an underlying source holds `byobRequest.view`, and in that state `enqueue()`
deliberately throws, because it has nowhere to copy the chunk into. So a returning buffer has to be
delivered the way a source delivers one: `byobRequest.respondWithNewView(view)`, or `respond(n)`
when the region is unchanged. An in-place transformer does that instead of calling `enqueue()`; an
identity transform stream can do it with no transformer code at all.

This should be opt-in per transformer rather than automatic, since a general transformer's two sides
need not agree about buffer sizes or lifetimes.

**Copying, when the sides do not line up.** When the transform is not in place, or the downstream
buffer is too small, the transformer copies into the BYOB view and keeps its own buffer — which then
goes back to the producer under rule 3. Adapted from the original sketch of this design:

```javascript
const identity = new TransformStream({
  writableType: "bytes",
  readableType: "bytes",

  transform(chunk, controller) {
    const byobRequest = controller.byobRequest;
    if (byobRequest && byobRequest.view.byteLength >= chunk.byteLength) {
      byobRequest.view.set(chunk, 0);
      byobRequest.respond(chunk.byteLength);
      return undefined;            // `chunk` was only read from, so it goes back to the producer
    }
    controller.enqueue(chunk);
    return null;
  }
});
```

The copy is still there, but the allocation is not: both buffers are recycled to their respective
ends, so a steady-state pipeline settles into a fixed set of buffers even when it cannot avoid
copying. Splitting a chunk across a too-small BYOB view and the queue is left as an open question
below.


## Goals

*   Give writable streams the buffer-reuse and zero-copy story that readable byte streams already
    have, as its dual rather than as a parallel invention.
*   Make chunk ownership after a write deterministic and enforced, replacing the current informal
    "please do not mutate this" contract.
*   Let underlying sinks lend memory of their own choosing to producers, so bytes can be written
    directly into a ring buffer, shared memory, or device-mapped memory.
*   Let buffers circulate through a whole pipe chain, including across transform streams, so that a
    steady-state pipeline neither allocates nor copies.
*   Change nothing for existing producers, sinks, and pipes.


## Non-goals

*   Non-byte chunks. Streams of objects, strings, or video frames are served by default writable
    streams, and by [transferring-ownership streams](./streams-for-raw-video.md) for chunks that
    need explicit lifetime management.
*   `SharedArrayBuffer`, for the same reason readable byte streams exclude it: the design rests on
    detaching, and shared buffers cannot be detached.
*   Guaranteeing zero copies. The design makes them possible and makes the copying paths explicit;
    it does not promise that any given pair of endpoints avoids them.
*   Synchronous draining of the write queue, as explored in
    [#465](https://github.com/whatwg/streams/issues/465). It is a compatible but separate concern.


## User benefits

*   Sites that stream binary data out — uploads, sockets, WebTransport, WebCodecs output, file
    writes — stop allocating a buffer per chunk, which is the dominant source of GC churn in these
    pipelines and the usual cause of jank in real-time ones.
*   Bytes can be produced directly into the memory the sink will actually use, removing a copy per
    chunk on the path to the network or to another thread.
*   A class of timing-dependent bugs — mutating a buffer that a sink is concurrently reading —
    becomes a deterministic failure: at worst a write that goes nowhere, never corruption of the
    bytes the sink is in the middle of reading.


## Alternatives

*   **`write()` fulfilling with the recycled buffer** (`writer.write(view): Promise<View>`), so one
    call both hands a buffer over and returns the next one. This is the tightest mirror of
    `reader.read(view)` and reads well in a loop, but it overloads the meaning of `write()`'s
    promise, diverges from the default writer, still needs a priming call to get the first buffer,
    and gives `pipeTo()` no way to ask for a buffer without also writing one. `requestBuffer()`
    keeps the two operations separate; the fused form could be added later as sugar.
*   **A `WritableStreamBYOBRequest` on the writer**, mirroring `ReadableStreamBYOBRequest` exactly:
    `await writer.ready`, then `writer.byobRequest.view` and `writer.byobRequest.respond(n)`. This is
    the strict dual, and is a good description of the internal machinery, but it puts a synchronous
    request/respond protocol in front of ordinary producer code, which is the half of the API that
    should look like `read()`.
*   **A `recycle()`-only design with no return value from `write()`**, requiring every sink to
    explicitly hand each buffer back. More uniform, but it makes the common case — a sink that reads
    the chunk and is done — into boilerplate, and makes forgetting the call a silent leak.
*   **Transfer without a return path**, i.e. `type: "owning"` from the [transferring-ownership
    streams explainer](./streams-for-raw-video.md). That solves ownership but not reuse: the
    producer still allocates every chunk. The two designs agree on transferring at hand-off, and a
    writable byte stream is essentially the byte-specific specialization that adds the return trip.
*   **Doing nothing, and pooling on the producer side** behind `await writer.write()`. This is what
    careful code does today. It fails whenever the sink transfers the chunk, it serializes the
    producer against the queue rather than against the sink, and it cannot express a sink that owns
    the memory.


## Open questions

*   **Queuing strategy.** Readable byte streams size their queue in bytes and disallow a custom
    `size` function; writable streams default to counting chunks with a high water mark of 1. Bytes
    seem right for consistency, but then what is the default high water mark — one
    `autoAllocateChunkSize` chunk? — and is the number of buffers in circulation the more meaningful
    control anyway, given that it already bounds how much can be in flight?
*   **`requestBuffer()` and mismatched sizes.** If the first free buffer is smaller than `minSize`,
    should the stream skip it, wait, coalesce, or allocate? Should a larger one be split?
*   **The strict-versus-lenient detach check.** Rule 3 makes returning `undefined` after detaching
    the view an error, which catches a real class of sink bug at the cost of requiring sinks to be
    explicit with `null`. The lenient alternative — silently recycle nothing — is friendlier and
    less diagnosable.
*   **Whether `pipeTo()` opts in automatically** when both ends are byte streams. It is a pure win
    for allocation, and `pipeTo()` already picks its reader at the user agent's discretion for byte
    sources — but it does change which buffers the underlying source is handed, which is observable.
*   **Whether cross-side buffer reuse in a transform stream is opt-in, automatic, or automatic only
    for identity transforms.**
*   **Splitting a chunk** that does not fit the downstream BYOB view, as in the transform example
    above: part into the view, the rest into the queue.
*   **Naming**: `mode: "byob"` on a writer, when in the PUTB case the buffer is emphatically not
    your own; `recycle()` versus `provide()`; `WritableByteStreamController` versus
    `WritableStreamByteController`.
*   **The symmetric gap on the readable side.** There is no way for a consumer to hand a buffer back
    to an underlying source, which is the mirror image of the problem this explainer solves. If
    buffers are to circulate through a pipe chain in general, that side may need the same treatment.
