# Resizable Buffers for BYOB Readers Explainer


## Introduction

The streams APIs provide ubiquitous, interoperable primitives for creating, composing, and consuming streams of data.
For streams representing bytes, readable byte streams are an extended version of readable streams which are provided to
handle bytes efficiently. These readable byte streams allow for BYOB (bring-your-own-buffer) readers to be acquired,
where a buffer can be reused for multiple reads to reduce garbage collection and to minimize copies.

JavaScript has since gained
[resizable `ArrayBuffer`s](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/ArrayBuffer/resizable):
an `ArrayBuffer` constructed with a `maxByteLength` option can be grown or shrunk in place with `resize()`, up to that
maximum, without allocating a new buffer and copying the old contents into it.

Today, BYOB readers reject such buffers: `ReadableStreamBYOBReader.read(view)` returns a promise rejected with a
`TypeError` if `view` is backed by a resizable `ArrayBuffer`. This change extends BYOB readers to accept resizable
`ArrayBuffer`s, allowing the consumer to adjust the buffer's size without copying to a new buffer.

## API Proposed

*   [`ReadableStreamBYOBReader.read(view, opts)`](https://streams.spec.whatwg.org/#byob-reader-read)
    and [`ReadableStreamBYOBRequest.respondWithNewView(view)`](https://streams.spec.whatwg.org/#rs-byob-request-respond-with-new-view)
    will also accept an `ArrayBufferView` backed by a resizable `ArrayBuffer`.
    *   In Web IDL terms, the `view` argument becomes `[AllowResizable] ArrayBufferView`, where `[AllowResizable]` is a
        new extended attribute permitting views backed by both resizable and non-resizable buffers.
    *   The backing buffer will still become detached as usual,
        but now the resizability of that buffer will be preserved when it is returned by the reader.
        Its `maxByteLength` is preserved as well, so the returned buffer can be resized within the same bounds as the
        buffer that was passed in.
    *   Because the buffer is detached for the duration of the read, it cannot be resized *while* a read is pending.
        The consumer resizes the buffer that `read()` gives back, in between reads.
    *   A
        [length-tracking](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/ArrayBuffer/resizable#length-tracking_typedarrays)
        view (such as `new Uint8Array(buffer)` on a resizable `buffer`, which has no fixed length) is accepted, and its
        length is *snapshotted* at the time of the `read()` or `respondWithNewView()` call. The view returned by the
        reader always has a fixed length: the number of bytes that were actually filled.
*   [`ReadableByteStreamController.enqueue(chunk)`](https://streams.spec.whatwg.org/#rbs-controller-enqueue)
    will be left unchanged, and continues to reject chunks backed by a resizable `ArrayBuffer`.
    The bytes of an enqueued chunk are copied into the stream's internal queue (or into a pending BYOB request's
    buffer), so the chunk's own resizability would not be observable to the consumer anyway.

### Underlying sources must not resize the buffer

While a BYOB request is outstanding, the underlying source is handed a
[`byobRequest.view`](https://streams.spec.whatwg.org/#rs-byob-request-view) that is backed by (a transferred copy of)
the consumer's buffer. If the consumer passed in a resizable buffer, then that view's buffer is resizable too, and
nothing in the language stops the underlying source from calling `resize()` on it. That would be surprising for the
consumer: it would silently change the size of a buffer it believes it owns, and it can invalidate the bookkeeping the
stream does to figure out how many bytes were filled.

Ideally the source would simply be handed a *non-resizable* view onto the same memory. That does not appear to be
expressible: there is no way to expose a resizable `ArrayBuffer`'s storage as a non-resizable `ArrayBuffer` and then
turn it back into a resizable one when handing it to the consumer. (This is the same problem as the
[alternative](#non-resizable-arraybuffer-subarrays-of-a-resizable-arraybuffer) discussed below.)

Instead, the proposal takes a validating approach: the stream remembers the buffer's byte length and maximum byte
length when it creates the BYOB request, and re-checks them in
[`respond(bytesWritten)`](https://streams.spec.whatwg.org/#rs-byob-request-respond),
`respondWithNewView(view)` and `enqueue(chunk)`. If the underlying source resized the buffer, the check fails and the
stream is errored, rather than the consumer receiving a buffer of an unexpected size.

## Examples

### Grow buffer for large read

The code starts out reading into a small buffer of 1024 bytes.
If that is not large enough to hold the entire response, we grow the buffer
so it can hold the additional bytes from subsequent reads.

```javascript
const reader = readableStream.getReader({ mode: "byob" });
let buffer = new ArrayBuffer(1024, { maxByteLength: 8192 });
let offset = 0;
while (true) {
  const { value: view, done } =
    await reader.read(new Uint8Array(buffer, offset, buffer.byteLength - offset));
  // The buffer was transferred by `read()`, but the returned buffer is still
  // resizable and has the same `maxByteLength`.
  buffer = view.buffer;
  offset += view.byteLength;
  if (done) {
    return new Uint8Array(buffer, 0, offset);
  }
  if (offset === buffer.byteLength) {
    // Buffer is full, grow it *without copying* if possible.
    if (buffer.byteLength >= buffer.maxByteLength) {
      throw new RangeError("Response is too large!");
    }
    buffer.resize(Math.min(buffer.byteLength * 2, buffer.maxByteLength));
  }
}
```

### Shrink buffer after reading

The code reads a response that can be up to 1024 bytes long into a single `ArrayBuffer`.
If the response ends up being smaller than 1024 bytes, we resize the buffer to match the exact response size
and free up the unused bytes of that buffer.

```javascript
const MAX_SIZE = 1024;
const reader = readableStream.getReader({ mode: "byob" });
// Create a buffer that can fit a complete response (at most MAX_SIZE bytes).
// It starts out at its maximum size, so it can only ever shrink.
let buffer = new ArrayBuffer(MAX_SIZE, { maxByteLength: MAX_SIZE });
// Read the whole response. Using `min` means the read only resolves once the
// buffer is completely filled, or the stream closes before that happens.
const { value: view, done } =
  await reader.read(new Uint8Array(buffer, 0, buffer.byteLength), { min: buffer.byteLength });
buffer = view.buffer;
if (!done) {
  // We filled the entire buffer, so there may be more bytes still to come
  // and we have no room left to read them into.
  throw new RangeError(`Response is larger than ${MAX_SIZE} bytes!`);
}
// The response was smaller, so shrink the backing buffer *without copying*.
buffer.resize(view.byteLength);
```

### Feature detection

Until this is widely supported, code can detect it by performing a BYOB read with a resizable buffer on a stream that
is already closed:

```javascript
async function supportsResizableBuffersForBYOB() {
  const stream = new ReadableStream({ type: "bytes", start(c) { c.close(); } });
  const reader = stream.getReader({ mode: "byob" });
  try {
    // Reading from an already-closed byte stream resolves immediately.
    await reader.read(new Uint8Array(new ArrayBuffer(1, { maxByteLength: 8 })));
    return true;
  } catch {
    // Without support, the read rejects with a `TypeError` for the resizable buffer.
    return false;
  }
}
```

## Goals

*   Allow buffers for BYOB to grow or shrink between reads without copying.
*   Make the resizability and maximum size of a consumer-provided buffer survive the transfer that a BYOB read
    performs, so a buffer can be reused across many reads.

## Non-Goals

*   Growable shared array buffers are not part of this proposal.
    Adding BYOB support for `SharedArrayBuffer` in general can become its own separate proposal.
*   Allowing the underlying source to resize the consumer's buffer. See
    [above](#underlying-sources-must-not-resize-the-buffer).
*   Accepting resizable buffers in `ReadableByteStreamController.enqueue(chunk)`.

## Use Cases

*   **Reading a response of unknown length.** The consumer starts with a modest buffer and grows it as the response
    turns out to be bigger, without repeatedly reallocating and copying (the "Grow buffer for large read" example).
*   **Reading a response with a known upper bound.** The consumer allocates for the worst case and then shrinks to the
    actual size, returning the unused memory instead of holding onto it (the "Shrink buffer after reading" example).
*   **Framed or length-prefixed protocols.** The consumer reads a small header, learns the size of the payload, and
    resizes the buffer to fit exactly that payload before reading it.
*   **Reading into WebAssembly memory.** A longer-term goal is to let a stream be read directly into a WebAssembly
    module's memory, avoiding a copy across the JavaScript/Wasm boundary. Since a Wasm memory can grow, any such
    integration will need BYOB reads to cope with a buffer whose size can change; accepting resizable `ArrayBuffer`s is
    a prerequisite for that.

## End-user Benefits

*   Allows web developers to use resizable `ArrayBuffer`s in their stream processing,
    which can help to make their code more memory efficient.
    *   Without resizable buffers, developers need to repeatedly allocate a new buffer
        and copy the old data into it, which can lead to memory fragmentation.
    *   With a resizable buffer, developers can grow their existing buffer and make better use of
        the available memory (which could be very limited).
*   Lower memory usage and fewer copies mean less garbage collection pressure, which in turn means fewer janky frames
    on memory-constrained devices.

## Alternatives

### Keep copying between fixed-size buffers

Today, a consumer that needs a differently sized buffer must allocate a new non-resizable `ArrayBuffer` and copy the
bytes it has already read into it. This works, but it costs a copy of everything read so far each time the buffer
grows, and it leaves a trail of dead buffers of increasing size behind, which can fragment the heap.

The alternative of allocating a maximum-size buffer up front avoids the copies, but wastes memory whenever the actual
response is much smaller than the worst case — and it is not always possible to know a worst case at all.

### Non-resizable `ArrayBuffer` subarrays of a resizable `ArrayBuffer`

Rather than changing the Streams standard to accept a resizable buffer,
we could extend `ArrayBuffer` itself to allow creating a non-resizable `ArrayBuffer` view
on part of its data, while still allowing the original resizable `ArrayBuffer` to be resized.

```javascript
const buffer = new ArrayBuffer(1024, { maxByteLength: 8192 });
// Create a non-resizable `ArrayBuffer` that is backed by the same buffer,
// but cannot be resized.
// (This API does not currently exist.)
const readBuffer = buffer.subarray(0, 1024);
console.assert(readBuffer.resizable === false);
const { value, done } = await reader.read(new Uint8Array(readBuffer, 0, readBuffer.byteLength));
```

However, this raises a lot more questions:
*   `ArrayBuffer`s generally "own" their backing data, but now multiple `ArrayBuffer`s
    may share (parts of) their backing data. This raises questions about transferability:
    *   What happens to the subarray buffer if the parent buffer is transferred? Or vice versa?
*   Can the parent buffer still shrink to a smaller size, and what happens to its subarray buffers?
*   What makes an `ArrayBuffer` backed by a larger `ArrayBuffer` different from
    an `ArrayBufferView` backed by an `ArrayBuffer`?

## Future Work

*   **Resizable buffers for `autoAllocateChunkSize`.** When a readable byte stream is constructed with
    [`autoAllocateChunkSize`](https://streams.spec.whatwg.org/#dom-underlyingsource-autoallocatechunksize), the stream
    allocates the buffers itself. Allocating those as resizable buffers, and shrinking them to the number of bytes
    actually filled before handing them to the consumer, could avoid handing out mostly-empty buffers when the source
    produces less data than the chunk size.
*   **Growable `SharedArrayBuffer`s**, as part of a broader proposal for `SharedArrayBuffer` support in BYOB readers.
*   **Reading directly into WebAssembly memory**, as described under [Use Cases](#use-cases).
