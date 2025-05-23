'use strict';
const assert = require('assert');

const { promiseResolvedWith, promiseRejectedWith, newPromise, resolvePromise, rejectPromise, uponPromise,
        setPromiseIsHandledToTrue, waitForAllPromise, transformPromiseWith, uponFulfillment, uponRejection } =
  require('../helpers/webidl.js');
const { CanTransferArrayBuffer, Call, CopyDataBlockBytes, CreateArrayFromList, GetIterator, GetMethod, IsDetachedBuffer,
        IteratorComplete, IteratorNext, IteratorValue, TransferArrayBuffer, typeIsObject } = require('./ecmascript.js');
const { CanCopyDataBlockBytes, CloneAsUint8Array, IsNonNegativeNumber } = require('./miscellaneous.js');
const { EnqueueValueWithSize, ResetQueue } = require('./queue-with-sizes.js');
const { AcquireWritableStreamDefaultWriter, IsWritableStreamLocked, WritableStreamAbort,
        WritableStreamDefaultWriterCloseWithErrorPropagation, WritableStreamDefaultWriterRelease,
        WritableStreamDefaultWriterWrite, WritableStreamCloseQueuedOrInFlight } = require('./writable-streams.js');
const { CancelSteps, PullSteps, ReleaseSteps } = require('./internal-methods.js');

const ReadableByteStreamController = require('../../generated/ReadableByteStreamController.js');
const ReadableStreamBYOBReader = require('../../generated/ReadableStreamBYOBReader.js');
const ReadableStreamBYOBRequest = require('../../generated/ReadableStreamBYOBRequest.js');
const ReadableStreamDefaultReader = require('../../generated/ReadableStreamDefaultReader.js');
const ReadableStreamDefaultController = require('../../generated/ReadableStreamDefaultController.js');
const ReadableStream = require('../../generated/ReadableStream.js');
const WritableStream = require('../../generated/WritableStream.js');

Object.assign(exports, {
  AcquireReadableStreamBYOBReader,
  AcquireReadableStreamDefaultReader,
  CreateReadableByteStream,
  CreateReadableStream,
  InitializeReadableStream,
  IsReadableStreamLocked,
  ReadableByteStreamControllerCallPullIfNeeded,
  ReadableByteStreamControllerClearAlgorithms,
  ReadableByteStreamControllerClearPendingPullIntos,
  ReadableByteStreamControllerClose,
  ReadableByteStreamControllerEnqueue,
  ReadableByteStreamControllerError,
  ReadableByteStreamControllerFillReadRequestFromQueue,
  ReadableByteStreamControllerGetBYOBRequest,
  ReadableByteStreamControllerGetDesiredSize,
  ReadableByteStreamControllerHandleQueueDrain,
  ReadableByteStreamControllerRespond,
  ReadableByteStreamControllerRespondWithNewView,
  ReadableStreamAddReadRequest,
  ReadableStreamBYOBReaderRead,
  ReadableStreamBYOBReaderRelease,
  ReadableStreamCancel,
  ReadableStreamClose,
  ReadableStreamDefaultControllerCallPullIfNeeded,
  ReadableStreamDefaultControllerCanCloseOrEnqueue,
  ReadableStreamDefaultControllerClearAlgorithms,
  ReadableStreamDefaultControllerClose,
  ReadableStreamDefaultControllerEnqueue,
  ReadableStreamDefaultControllerError,
  ReadableStreamDefaultControllerGetDesiredSize,
  ReadableStreamDefaultControllerHasBackpressure,
  ReadableStreamDefaultReaderRead,
  ReadableStreamDefaultReaderRelease,
  ReadableStreamFromIterable,
  ReadableStreamGetNumReadRequests,
  ReadableStreamHasDefaultReader,
  ReadableStreamPipeTo,
  ReadableStreamReaderGenericCancel,
  ReadableStreamTee,
  SetUpReadableByteStreamControllerFromUnderlyingSource,
  SetUpReadableStreamBYOBReader,
  SetUpReadableStreamDefaultControllerFromUnderlyingSource,
  SetUpReadableStreamDefaultReader
});

// Working with readable streams

function AcquireReadableStreamBYOBReader(stream) {
  const reader = ReadableStreamBYOBReader.new(globalThis);
  SetUpReadableStreamBYOBReader(reader, stream);
  return reader;
}

function AcquireReadableStreamDefaultReader(stream) {
  const reader = ReadableStreamDefaultReader.new(globalThis);
  SetUpReadableStreamDefaultReader(reader, stream);
  return reader;
}

function CreateReadableStream(startAlgorithm, pullAlgorithm, cancelAlgorithm, highWaterMark = 1,
                              sizeAlgorithm = () => 1) {
  assert(IsNonNegativeNumber(highWaterMark) === true);

  const stream = ReadableStream.new(globalThis);
  InitializeReadableStream(stream);

  const controller = ReadableStreamDefaultController.new(globalThis);
  SetUpReadableStreamDefaultController(
    stream, controller, startAlgorithm, pullAlgorithm, cancelAlgorithm, highWaterMark, sizeAlgorithm
  );

  return stream;
}

function CreateReadableByteStream(startAlgorithm, pullAlgorithm, cancelAlgorithm) {
  const stream = ReadableStream.new(globalThis);
  InitializeReadableStream(stream);

  const controller = ReadableByteStreamController.new(globalThis);
  SetUpReadableByteStreamController(
    stream, controller, startAlgorithm, pullAlgorithm, cancelAlgorithm, 0, undefined
  );

  return stream;
}

function InitializeReadableStream(stream) {
  stream._state = 'readable';
  stream._reader = undefined;
  stream._storedError = undefined;
  stream._disturbed = false;
}

// IsReadableStreamDisturbed is not implemented since it is only meant for external specs.

function IsReadableStreamLocked(stream) {
  if (stream._reader === undefined) {
    return false;
  }

  return true;
}

function ReadableStreamPipeTo(source, dest, preventClose, preventAbort, preventCancel, signal) {
  assert(ReadableStream.isImpl(source));
  assert(WritableStream.isImpl(dest));
  assert(typeof preventClose === 'boolean');
  assert(typeof preventAbort === 'boolean');
  assert(typeof preventCancel === 'boolean');
  assert(signal === undefined || signal.constructor.name === 'AbortSignal');
  assert(IsReadableStreamLocked(source) === false);
  assert(IsWritableStreamLocked(dest) === false);

  const reader = AcquireReadableStreamDefaultReader(source);
  const writer = AcquireWritableStreamDefaultWriter(dest);

  source._disturbed = true;

  let shuttingDown = false;

  // This is used to keep track of the spec's requirement that we wait for ongoing writes during shutdown.
  let currentWrite = promiseResolvedWith(undefined);

  return new Promise((resolve, reject) => {
    let abortAlgorithm;
    if (signal !== undefined) {
      abortAlgorithm = () => {
        const error = signal.reason;
        const actions = [];
        if (preventAbort === false) {
          actions.push(() => {
            if (dest._state === 'writable') {
              return WritableStreamAbort(dest, error);
            }
            return promiseResolvedWith(undefined);
          });
        }
        if (preventCancel === false) {
          actions.push(() => {
            if (source._state === 'readable') {
              return ReadableStreamCancel(source, error);
            }
            return promiseResolvedWith(undefined);
          });
        }
        shutdownWithAction(() => waitForAllPromise(actions.map(action => action())), true, error);
      };

      if (signal.aborted === true) {
        abortAlgorithm();
        return;
      }

      signal.addEventListener('abort', abortAlgorithm);
    }

    // Using reader and writer, read all chunks from this and write them to dest
    // - Backpressure must be enforced
    // - Shutdown must stop all activity
    function pipeLoop() {
      return new Promise((resolveLoop, rejectLoop) => {
        function next(done) {
          if (done) {
            resolveLoop();
          } else {
            uponPromise(pipeStep(), next, rejectLoop);
          }
        }

        next(false);
      });
    }

    function pipeStep() {
      if (shuttingDown === true) {
        return promiseResolvedWith(true);
      }

      return transformPromiseWith(writer._readyPromise, () => {
        return new Promise((resolveRead, rejectRead) => {
          ReadableStreamDefaultReaderRead(
            reader,
            {
              chunkSteps: chunk => {
                currentWrite = transformPromiseWith(
                  WritableStreamDefaultWriterWrite(writer, chunk), undefined, () => {}
                );
                resolveRead(false);
              },
              closeSteps: () => resolveRead(true),
              errorSteps: rejectRead
            }
          );
        });
      });
    }

    // Errors must be propagated forward
    isOrBecomesErrored(source, reader._closedPromise, storedError => {
      if (preventAbort === false) {
        shutdownWithAction(() => WritableStreamAbort(dest, storedError), true, storedError);
      } else {
        shutdown(true, storedError);
      }
    });

    // Errors must be propagated backward
    isOrBecomesErrored(dest, writer._closedPromise, storedError => {
      if (preventCancel === false) {
        shutdownWithAction(() => ReadableStreamCancel(source, storedError), true, storedError);
      } else {
        shutdown(true, storedError);
      }
    });

    // Closing must be propagated forward
    isOrBecomesClosed(source, reader._closedPromise, () => {
      if (preventClose === false) {
        shutdownWithAction(() => WritableStreamDefaultWriterCloseWithErrorPropagation(writer));
      } else {
        shutdown();
      }
    });

    // Closing must be propagated backward
    if (WritableStreamCloseQueuedOrInFlight(dest) === true || dest._state === 'closed') {
      const destClosed = new TypeError('the destination writable stream closed before all data could be piped to it');

      if (preventCancel === false) {
        shutdownWithAction(() => ReadableStreamCancel(source, destClosed), true, destClosed);
      } else {
        shutdown(true, destClosed);
      }
    }

    setPromiseIsHandledToTrue(pipeLoop());

    function waitForWritesToFinish() {
      // Another write may have started while we were waiting on this currentWrite, so we have to be sure to wait
      // for that too.
      const oldCurrentWrite = currentWrite;
      return transformPromiseWith(
        currentWrite,
        () => oldCurrentWrite !== currentWrite ? waitForWritesToFinish() : undefined
      );
    }

    function isOrBecomesErrored(stream, promise, action) {
      if (stream._state === 'errored') {
        action(stream._storedError);
      } else {
        uponRejection(promise, action);
      }
    }

    function isOrBecomesClosed(stream, promise, action) {
      if (stream._state === 'closed') {
        action();
      } else {
        uponFulfillment(promise, action);
      }
    }

    function shutdownWithAction(action, originalIsError, originalError) {
      if (shuttingDown === true) {
        return;
      }
      shuttingDown = true;

      if (dest._state === 'writable' && WritableStreamCloseQueuedOrInFlight(dest) === false) {
        uponFulfillment(waitForWritesToFinish(), doTheRest);
      } else {
        doTheRest();
      }

      function doTheRest() {
        uponPromise(
          action(),
          () => finalize(originalIsError, originalError),
          newError => finalize(true, newError)
        );
      }
    }

    function shutdown(isError, error) {
      if (shuttingDown === true) {
        return;
      }
      shuttingDown = true;

      if (dest._state === 'writable' && WritableStreamCloseQueuedOrInFlight(dest) === false) {
        uponFulfillment(waitForWritesToFinish(), () => finalize(isError, error));
      } else {
        finalize(isError, error);
      }
    }

    function finalize(isError, error) {
      WritableStreamDefaultWriterRelease(writer);
      ReadableStreamDefaultReaderRelease(reader);

      if (signal !== undefined) {
        signal.removeEventListener('abort', abortAlgorithm);
      }
      if (isError) {
        reject(error);
      } else {
        resolve(undefined);
      }
    }
  });
}

function ReadableStreamTee(stream, cloneForBranch2) {
  assert(ReadableStream.isImpl(stream));
  assert(typeof cloneForBranch2 === 'boolean');
  if (ReadableByteStreamController.isImpl(stream._controller)) {
    return ReadableByteStreamTee(stream);
  }
  return ReadableStreamDefaultTee(stream, cloneForBranch2);
}

function ReadableStreamDefaultTee(stream, cloneForBranch2) {
  assert(ReadableStream.isImpl(stream));
  assert(typeof cloneForBranch2 === 'boolean');

  const reader = AcquireReadableStreamDefaultReader(stream);

  let reading = false;
  let readAgain = false;
  let canceled1 = false;
  let canceled2 = false;
  let reason1;
  let reason2;
  let branch1;
  let branch2;

  const cancelPromise = newPromise();

  function pullAlgorithm() {
    if (reading === true) {
      readAgain = true;
      return promiseResolvedWith(undefined);
    }

    reading = true;

    const readRequest = {
      chunkSteps: chunk => {
        // This needs to be delayed a microtask because it takes at least a microtask to detect errors (using
        // reader._closedPromise below), and we want errors in stream to error both branches immediately. We cannot let
        // successful synchronously-available reads get ahead of asynchronously-available errors.
        queueMicrotask(() => {
          readAgain = false;
          const chunk1 = chunk;
          const chunk2 = chunk;

          // There is no way to access the cloning code right now in the reference implementation.
          // If we add one then we'll need an implementation for serializable objects.
          // if (canceled2 === false && cloneForBranch2 === true) {
          //   try {
          //     chunk2 = StructuredClone(chunk2);
          //   } catch (cloneE) {
          //     ReadableByteStreamControllerError(branch1._controller, cloneE);
          //     ReadableByteStreamControllerError(branch2._controller, cloneE);
          //     resolvePromise(cancelPromise, ReadableStreamCancel(stream, cloneE));
          //     return;
          //   }
          // }

          if (canceled1 === false) {
            ReadableStreamDefaultControllerEnqueue(branch1._controller, chunk1);
          }
          if (canceled2 === false) {
            ReadableStreamDefaultControllerEnqueue(branch2._controller, chunk2);
          }

          reading = false;
          if (readAgain === true) {
            pullAlgorithm();
          }
        });
      },
      closeSteps: () => {
        reading = false;
        if (canceled1 === false) {
          ReadableStreamDefaultControllerClose(branch1._controller);
        }
        if (canceled2 === false) {
          ReadableStreamDefaultControllerClose(branch2._controller);
        }
        if (canceled1 === false || canceled2 === false) {
          resolvePromise(cancelPromise, undefined);
        }
      },
      errorSteps: () => {
        reading = false;
      }
    };
    ReadableStreamDefaultReaderRead(reader, readRequest);

    return promiseResolvedWith(undefined);
  }

  function cancel1Algorithm(reason) {
    canceled1 = true;
    reason1 = reason;
    if (canceled2 === true) {
      const compositeReason = CreateArrayFromList([reason1, reason2]);
      const cancelResult = ReadableStreamCancel(stream, compositeReason);
      resolvePromise(cancelPromise, cancelResult);
    }
    return cancelPromise;
  }

  function cancel2Algorithm(reason) {
    canceled2 = true;
    reason2 = reason;
    if (canceled1 === true) {
      const compositeReason = CreateArrayFromList([reason1, reason2]);
      const cancelResult = ReadableStreamCancel(stream, compositeReason);
      resolvePromise(cancelPromise, cancelResult);
    }
    return cancelPromise;
  }

  function startAlgorithm() {}

  branch1 = CreateReadableStream(startAlgorithm, pullAlgorithm, cancel1Algorithm);
  branch2 = CreateReadableStream(startAlgorithm, pullAlgorithm, cancel2Algorithm);

  uponRejection(reader._closedPromise, r => {
    ReadableStreamDefaultControllerError(branch1._controller, r);
    ReadableStreamDefaultControllerError(branch2._controller, r);
    if (canceled1 === false || canceled2 === false) {
      resolvePromise(cancelPromise, undefined);
    }
  });

  return [branch1, branch2];
}

function ReadableByteStreamTee(stream) {
  assert(ReadableStream.isImpl(stream));
  assert(ReadableByteStreamController.isImpl(stream._controller));

  let reader = AcquireReadableStreamDefaultReader(stream);
  let readAgainForBranch1 = false;
  let readAgainForBranch2 = false;
  let reading = false;
  let canceled1 = false;
  let canceled2 = false;
  let reason1;
  let reason2;
  let branch1;
  let branch2;

  const cancelPromise = newPromise();

  function forwardReaderError(thisReader) {
    uponRejection(thisReader._closedPromise, r => {
      if (thisReader !== reader) {
        return;
      }
      ReadableByteStreamControllerError(branch1._controller, r);
      ReadableByteStreamControllerError(branch2._controller, r);
      if (canceled1 === false || canceled2 === false) {
        resolvePromise(cancelPromise, undefined);
      }
    });
  }

  function pullWithDefaultReader() {
    if (ReadableStreamBYOBReader.isImpl(reader)) {
      assert(reader._readIntoRequests.length === 0);
      ReadableStreamBYOBReaderRelease(reader);

      reader = AcquireReadableStreamDefaultReader(stream);
      forwardReaderError(reader);
    }

    const readRequest = {
      chunkSteps: chunk => {
        // This needs to be delayed a microtask because it takes at least a microtask to detect errors (using
        // reader._closedPromise below), and we want errors in stream to error both branches immediately. We cannot let
        // successful synchronously-available reads get ahead of asynchronously-available errors.
        queueMicrotask(() => {
          readAgainForBranch1 = false;
          readAgainForBranch2 = false;

          const chunk1 = chunk;
          let chunk2 = chunk;
          if (canceled1 === false && canceled2 === false) {
            try {
              chunk2 = CloneAsUint8Array(chunk);
            } catch (cloneE) {
              ReadableByteStreamControllerError(branch1._controller, cloneE);
              ReadableByteStreamControllerError(branch2._controller, cloneE);
              resolvePromise(cancelPromise, ReadableStreamCancel(stream, cloneE));
              return;
            }
          }

          if (canceled1 === false) {
            ReadableByteStreamControllerEnqueue(branch1._controller, chunk1);
          }
          if (canceled2 === false) {
            ReadableByteStreamControllerEnqueue(branch2._controller, chunk2);
          }

          reading = false;
          if (readAgainForBranch1 === true) {
            pull1Algorithm();
          } else if (readAgainForBranch2 === true) {
            pull2Algorithm();
          }
        });
      },
      closeSteps: () => {
        reading = false;
        if (canceled1 === false) {
          ReadableByteStreamControllerClose(branch1._controller);
        }
        if (canceled2 === false) {
          ReadableByteStreamControllerClose(branch2._controller);
        }
        if (branch1._controller._pendingPullIntos.length > 0) {
          ReadableByteStreamControllerRespond(branch1._controller, 0);
        }
        if (branch2._controller._pendingPullIntos.length > 0) {
          ReadableByteStreamControllerRespond(branch2._controller, 0);
        }
        if (canceled1 === false || canceled2 === false) {
          resolvePromise(cancelPromise, undefined);
        }
      },
      errorSteps: () => {
        reading = false;
      }
    };
    ReadableStreamDefaultReaderRead(reader, readRequest);
  }

  function pullWithBYOBReader(view, forBranch2) {
    if (ReadableStreamDefaultReader.isImpl(reader)) {
      assert(reader._readRequests.length === 0);
      ReadableStreamDefaultReaderRelease(reader);

      reader = AcquireReadableStreamBYOBReader(stream);
      forwardReaderError(reader);
    }

    const byobBranch = forBranch2 ? branch2 : branch1;
    const otherBranch = forBranch2 ? branch1 : branch2;

    const readIntoRequest = {
      chunkSteps: chunk => {
        // This needs to be delayed a microtask because it takes at least a microtask to detect errors (using
        // reader._closedPromise below), and we want errors in stream to error both branches immediately. We cannot let
        // successful synchronously-available reads get ahead of asynchronously-available errors.
        queueMicrotask(() => {
          readAgainForBranch1 = false;
          readAgainForBranch2 = false;

          const byobCanceled = forBranch2 ? canceled2 : canceled1;
          const otherCanceled = forBranch2 ? canceled1 : canceled2;

          if (otherCanceled === false) {
            let clonedChunk;
            try {
              clonedChunk = CloneAsUint8Array(chunk);
            } catch (cloneE) {
              ReadableByteStreamControllerError(byobBranch._controller, cloneE);
              ReadableByteStreamControllerError(otherBranch._controller, cloneE);
              resolvePromise(cancelPromise, ReadableStreamCancel(stream, cloneE));
              return;
            }
            if (byobCanceled === false) {
              ReadableByteStreamControllerRespondWithNewView(byobBranch._controller, chunk);
            }
            ReadableByteStreamControllerEnqueue(otherBranch._controller, clonedChunk);
          } else if (byobCanceled === false) {
            ReadableByteStreamControllerRespondWithNewView(byobBranch._controller, chunk);
          }

          reading = false;
          if (readAgainForBranch1 === true) {
            pull1Algorithm();
          } else if (readAgainForBranch2 === true) {
            pull2Algorithm();
          }
        });
      },
      closeSteps: chunk => {
        reading = false;

        const byobCanceled = forBranch2 ? canceled2 : canceled1;
        const otherCanceled = forBranch2 ? canceled1 : canceled2;

        if (byobCanceled === false) {
          ReadableByteStreamControllerClose(byobBranch._controller);
        }
        if (otherCanceled === false) {
          ReadableByteStreamControllerClose(otherBranch._controller);
        }

        if (chunk !== undefined) {
          assert(chunk.byteLength === 0);

          if (byobCanceled === false) {
            ReadableByteStreamControllerRespondWithNewView(byobBranch._controller, chunk);
          }
          if (otherCanceled === false && otherBranch._controller._pendingPullIntos.length > 0) {
            ReadableByteStreamControllerRespond(otherBranch._controller, 0);
          }
        }

        if (byobCanceled === false || otherCanceled === false) {
          resolvePromise(cancelPromise, undefined);
        }
      },
      errorSteps: () => {
        reading = false;
      }
    };
    ReadableStreamBYOBReaderRead(reader, view, 1, readIntoRequest);
  }

  function pull1Algorithm() {
    if (reading === true) {
      readAgainForBranch1 = true;
      return promiseResolvedWith(undefined);
    }

    reading = true;

    const byobRequest = ReadableByteStreamControllerGetBYOBRequest(branch1._controller);
    if (byobRequest === null) {
      pullWithDefaultReader();
    } else {
      pullWithBYOBReader(byobRequest._view, false);
    }

    return promiseResolvedWith(undefined);
  }

  function pull2Algorithm() {
    if (reading === true) {
      readAgainForBranch2 = true;
      return promiseResolvedWith(undefined);
    }

    reading = true;

    const byobRequest = ReadableByteStreamControllerGetBYOBRequest(branch2._controller);
    if (byobRequest === null) {
      pullWithDefaultReader();
    } else {
      pullWithBYOBReader(byobRequest._view, true);
    }

    return promiseResolvedWith(undefined);
  }

  function cancel1Algorithm(reason) {
    canceled1 = true;
    reason1 = reason;
    if (canceled2 === true) {
      const compositeReason = CreateArrayFromList([reason1, reason2]);
      const cancelResult = ReadableStreamCancel(stream, compositeReason);
      resolvePromise(cancelPromise, cancelResult);
    }
    return cancelPromise;
  }

  function cancel2Algorithm(reason) {
    canceled2 = true;
    reason2 = reason;
    if (canceled1 === true) {
      const compositeReason = CreateArrayFromList([reason1, reason2]);
      const cancelResult = ReadableStreamCancel(stream, compositeReason);
      resolvePromise(cancelPromise, cancelResult);
    }
    return cancelPromise;
  }

  function startAlgorithm() {}

  branch1 = CreateReadableByteStream(startAlgorithm, pull1Algorithm, cancel1Algorithm);
  branch2 = CreateReadableByteStream(startAlgorithm, pull2Algorithm, cancel2Algorithm);

  forwardReaderError(reader);

  return [branch1, branch2];
}

// Interfacing with controllers

function ReadableStreamAddReadIntoRequest(stream, readRequest) {
  assert(ReadableStreamBYOBReader.isImpl(stream._reader));
  assert(stream._state === 'readable' || stream._state === 'closed');

  stream._reader._readIntoRequests.push(readRequest);
}

function ReadableStreamAddReadRequest(stream, readRequest) {
  assert(ReadableStreamDefaultReader.isImpl(stream._reader));
  assert(stream._state === 'readable');

  stream._reader._readRequests.push(readRequest);
}

function ReadableStreamCancel(stream, reason) {
  stream._disturbed = true;

  if (stream._state === 'closed') {
    return promiseResolvedWith(undefined);
  }
  if (stream._state === 'errored') {
    return promiseRejectedWith(stream._storedError);
  }

  ReadableStreamClose(stream);

  const reader = stream._reader;
  if (reader !== undefined && ReadableStreamBYOBReader.isImpl(reader)) {
    const readIntoRequests = reader._readIntoRequests;
    reader._readIntoRequests = [];
    for (const readIntoRequest of readIntoRequests) {
      readIntoRequest.closeSteps(undefined);
    }
  }

  const sourceCancelPromise = stream._controller[CancelSteps](reason);
  return transformPromiseWith(sourceCancelPromise, () => undefined);
}

function ReadableStreamClose(stream) {
  assert(stream._state === 'readable');

  stream._state = 'closed';

  const reader = stream._reader;

  if (reader === undefined) {
    return;
  }

  resolvePromise(reader._closedPromise, undefined);

  if (ReadableStreamDefaultReader.isImpl(reader)) {
    const readRequests = reader._readRequests;
    reader._readRequests = [];
    for (const readRequest of readRequests) {
      readRequest.closeSteps();
    }
  }
}

function ReadableStreamError(stream, e) {
  assert(stream._state === 'readable');

  stream._state = 'errored';
  stream._storedError = e;

  const reader = stream._reader;

  if (reader === undefined) {
    return;
  }

  rejectPromise(reader._closedPromise, e);
  setPromiseIsHandledToTrue(reader._closedPromise);

  if (ReadableStreamDefaultReader.isImpl(reader)) {
    ReadableStreamDefaultReaderErrorReadRequests(reader, e);
  } else {
    assert(ReadableStreamBYOBReader.isImpl(reader));
    ReadableStreamBYOBReaderErrorReadIntoRequests(reader, e);
  }
}

function ReadableStreamFulfillReadIntoRequest(stream, chunk, done) {
  const reader = stream._reader;

  assert(reader._readIntoRequests.length > 0);

  const readIntoRequest = reader._readIntoRequests.shift();
  if (done) {
    readIntoRequest.closeSteps(chunk);
  } else {
    readIntoRequest.chunkSteps(chunk);
  }
}

function ReadableStreamFulfillReadRequest(stream, chunk, done) {
  const reader = stream._reader;

  assert(reader._readRequests.length > 0);

  const readRequest = reader._readRequests.shift();
  if (done) {
    readRequest.closeSteps();
  } else {
    readRequest.chunkSteps(chunk);
  }
}

function ReadableStreamGetNumReadIntoRequests(stream) {
  return stream._reader._readIntoRequests.length;
}

function ReadableStreamGetNumReadRequests(stream) {
  return stream._reader._readRequests.length;
}

function ReadableStreamHasBYOBReader(stream) {
  const reader = stream._reader;

  if (reader === undefined) {
    return false;
  }

  if (ReadableStreamBYOBReader.isImpl(reader)) {
    return true;
  }

  return false;
}

function ReadableStreamHasDefaultReader(stream) {
  const reader = stream._reader;

  if (reader === undefined) {
    return false;
  }

  if (ReadableStreamDefaultReader.isImpl(reader)) {
    return true;
  }

  return false;
}

// Readers

function ReadableStreamReaderGenericCancel(reader, reason) {
  const stream = reader._stream;
  assert(stream !== undefined);
  return ReadableStreamCancel(stream, reason);
}

function ReadableStreamReaderGenericInitialize(reader, stream) {
  reader._stream = stream;
  stream._reader = reader;

  if (stream._state === 'readable') {
    reader._closedPromise = newPromise();
  } else if (stream._state === 'closed') {
    reader._closedPromise = promiseResolvedWith(undefined);
  } else {
    assert(stream._state === 'errored');

    reader._closedPromise = promiseRejectedWith(stream._storedError);
    setPromiseIsHandledToTrue(reader._closedPromise);
  }
}

function ReadableStreamReaderGenericRelease(reader) {
  const stream = reader._stream;
  assert(stream !== undefined);
  assert(stream._reader === reader);

  if (stream._state === 'readable') {
    rejectPromise(
      reader._closedPromise,
      new TypeError('Reader was released and can no longer be used to monitor the stream\'s closedness')
    );
  } else {
    reader._closedPromise = promiseRejectedWith(
      new TypeError('Reader was released and can no longer be used to monitor the stream\'s closedness')
    );
  }
  setPromiseIsHandledToTrue(reader._closedPromise);

  stream._controller[ReleaseSteps]();

  stream._reader = undefined;
  reader._stream = undefined;
}

function ReadableStreamBYOBReaderRead(reader, view, min, readIntoRequest) {
  const stream = reader._stream;

  assert(stream !== undefined);

  stream._disturbed = true;

  if (stream._state === 'errored') {
    readIntoRequest.errorSteps(stream._storedError);
  } else {
    ReadableByteStreamControllerPullInto(stream._controller, view, min, readIntoRequest);
  }
}

function ReadableStreamBYOBReaderRelease(reader) {
  ReadableStreamReaderGenericRelease(reader);
  const e = new TypeError('Reader was released');
  ReadableStreamBYOBReaderErrorReadIntoRequests(reader, e);
}

function ReadableStreamBYOBReaderErrorReadIntoRequests(reader, e) {
  const readIntoRequests = reader._readIntoRequests;
  reader._readIntoRequests = [];
  for (const readIntoRequest of readIntoRequests) {
    readIntoRequest.errorSteps(e);
  }
}

function ReadableStreamDefaultReaderRead(reader, readRequest) {
  const stream = reader._stream;

  assert(stream !== undefined);

  stream._disturbed = true;

  if (stream._state === 'closed') {
    readRequest.closeSteps();
  } else if (stream._state === 'errored') {
    readRequest.errorSteps(stream._storedError);
  } else {
    assert(stream._state === 'readable');
    stream._controller[PullSteps](readRequest);
  }
}

function ReadableStreamDefaultReaderRelease(reader) {
  ReadableStreamReaderGenericRelease(reader);
  const e = new TypeError('Reader was released');
  ReadableStreamDefaultReaderErrorReadRequests(reader, e);
}

function ReadableStreamDefaultReaderErrorReadRequests(reader, e) {
  const readRequests = reader._readRequests;
  reader._readRequests = [];
  for (const readRequest of readRequests) {
    readRequest.errorSteps(e);
  }
}

function SetUpReadableStreamBYOBReader(reader, stream) {
  if (IsReadableStreamLocked(stream) === true) {
    throw new TypeError('This stream has already been locked for exclusive reading by another reader');
  }

  if (!ReadableByteStreamController.isImpl(stream._controller)) {
    throw new TypeError('Cannot construct a ReadableStreamBYOBReader for a stream not constructed with a byte source');
  }

  ReadableStreamReaderGenericInitialize(reader, stream);

  reader._readIntoRequests = [];
}

function SetUpReadableStreamDefaultReader(reader, stream) {
  if (IsReadableStreamLocked(stream) === true) {
    throw new TypeError('This stream has already been locked for exclusive reading by another reader');
  }

  ReadableStreamReaderGenericInitialize(reader, stream);

  reader._readRequests = [];
}

// Default controllers

function ReadableStreamDefaultControllerCallPullIfNeeded(controller) {
  const shouldPull = ReadableStreamDefaultControllerShouldCallPull(controller);
  if (shouldPull === false) {
    return;
  }

  if (controller._pulling === true) {
    controller._pullAgain = true;
    return;
  }

  assert(controller._pullAgain === false);

  controller._pulling = true;

  const pullPromise = controller._pullAlgorithm();
  uponPromise(
    pullPromise,
    () => {
      controller._pulling = false;

      if (controller._pullAgain === true) {
        controller._pullAgain = false;
        ReadableStreamDefaultControllerCallPullIfNeeded(controller);
      }
    },
    e => {
      ReadableStreamDefaultControllerError(controller, e);
    }
  );
}

function ReadableStreamDefaultControllerShouldCallPull(controller) {
  const stream = controller._stream;

  if (ReadableStreamDefaultControllerCanCloseOrEnqueue(controller) === false) {
    return false;
  }

  if (controller._started === false) {
    return false;
  }

  if (IsReadableStreamLocked(stream) === true && ReadableStreamGetNumReadRequests(stream) > 0) {
    return true;
  }

  const desiredSize = ReadableStreamDefaultControllerGetDesiredSize(controller);
  assert(desiredSize !== null);
  if (desiredSize > 0) {
    return true;
  }

  return false;
}

function ReadableStreamDefaultControllerClearAlgorithms(controller) {
  controller._pullAlgorithm = undefined;
  controller._cancelAlgorithm = undefined;
  controller._strategySizeAlgorithm = undefined;
}

function ReadableStreamDefaultControllerClose(controller) {
  if (ReadableStreamDefaultControllerCanCloseOrEnqueue(controller) === false) {
    return;
  }

  const stream = controller._stream;

  controller._closeRequested = true;

  if (controller._queue.length === 0) {
    ReadableStreamDefaultControllerClearAlgorithms(controller);
    ReadableStreamClose(stream);
  }
}

function ReadableStreamDefaultControllerEnqueue(controller, chunk) {
  if (ReadableStreamDefaultControllerCanCloseOrEnqueue(controller) === false) {
    return;
  }

  const stream = controller._stream;

  if (IsReadableStreamLocked(stream) === true && ReadableStreamGetNumReadRequests(stream) > 0) {
    ReadableStreamFulfillReadRequest(stream, chunk, false);
  } else {
    let chunkSize;
    try {
      chunkSize = controller._strategySizeAlgorithm(chunk);
    } catch (chunkSizeE) {
      ReadableStreamDefaultControllerError(controller, chunkSizeE);
      throw chunkSizeE;
    }

    try {
      EnqueueValueWithSize(controller, chunk, chunkSize);
    } catch (enqueueE) {
      ReadableStreamDefaultControllerError(controller, enqueueE);
      throw enqueueE;
    }
  }

  ReadableStreamDefaultControllerCallPullIfNeeded(controller);
}

function ReadableStreamDefaultControllerError(controller, e) {
  const stream = controller._stream;

  if (stream._state !== 'readable') {
    return;
  }

  ResetQueue(controller);

  ReadableStreamDefaultControllerClearAlgorithms(controller);
  ReadableStreamError(stream, e);
}

function ReadableStreamDefaultControllerGetDesiredSize(controller) {
  const state = controller._stream._state;

  if (state === 'errored') {
    return null;
  }
  if (state === 'closed') {
    return 0;
  }

  return controller._strategyHWM - controller._queueTotalSize;
}

function ReadableStreamDefaultControllerHasBackpressure(controller) {
  if (ReadableStreamDefaultControllerShouldCallPull(controller) === true) {
    return false;
  }

  return true;
}

function ReadableStreamDefaultControllerCanCloseOrEnqueue(controller) {
  const state = controller._stream._state;

  if (controller._closeRequested === false && state === 'readable') {
    return true;
  }

  return false;
}

function SetUpReadableStreamDefaultController(
  stream, controller, startAlgorithm, pullAlgorithm, cancelAlgorithm, highWaterMark, sizeAlgorithm) {
  assert(stream._controller === undefined);

  controller._stream = stream;

  // Need to set the slots so that the assert doesn't fire. In the spec the slots already exist implicitly.
  controller._queue = undefined;
  controller._queueTotalSize = undefined;
  ResetQueue(controller);

  controller._started = false;
  controller._closeRequested = false;
  controller._pullAgain = false;
  controller._pulling = false;

  controller._strategySizeAlgorithm = sizeAlgorithm;
  controller._strategyHWM = highWaterMark;

  controller._pullAlgorithm = pullAlgorithm;
  controller._cancelAlgorithm = cancelAlgorithm;

  stream._controller = controller;

  const startResult = startAlgorithm();
  uponPromise(
    promiseResolvedWith(startResult),
    () => {
      controller._started = true;

      assert(controller._pulling === false);
      assert(controller._pullAgain === false);

      ReadableStreamDefaultControllerCallPullIfNeeded(controller);
    },
    r => {
      ReadableStreamDefaultControllerError(controller, r);
    }
  );
}

function SetUpReadableStreamDefaultControllerFromUnderlyingSource(
  stream, underlyingSource, underlyingSourceDict, highWaterMark, sizeAlgorithm) {
  const controller = ReadableStreamDefaultController.new(globalThis);

  let startAlgorithm = () => undefined;
  let pullAlgorithm = () => promiseResolvedWith(undefined);
  let cancelAlgorithm = () => promiseResolvedWith(undefined);

  if ('start' in underlyingSourceDict) {
    startAlgorithm = () => underlyingSourceDict.start.call(underlyingSource, controller);
  }
  if ('pull' in underlyingSourceDict) {
    pullAlgorithm = () => underlyingSourceDict.pull.call(underlyingSource, controller);
  }
  if ('cancel' in underlyingSourceDict) {
    cancelAlgorithm = reason => underlyingSourceDict.cancel.call(underlyingSource, reason);
  }

  SetUpReadableStreamDefaultController(
    stream, controller, startAlgorithm, pullAlgorithm, cancelAlgorithm, highWaterMark, sizeAlgorithm
  );
}

// Byte stream controllers

function ReadableByteStreamControllerCallPullIfNeeded(controller) {
  const shouldPull = ReadableByteStreamControllerShouldCallPull(controller);
  if (shouldPull === false) {
    return;
  }

  if (controller._pulling === true) {
    controller._pullAgain = true;
    return;
  }

  assert(controller._pullAgain === false);

  controller._pulling = true;

  // TODO: Test controller argument
  const pullPromise = controller._pullAlgorithm();
  uponPromise(
    pullPromise,
    () => {
      controller._pulling = false;

      if (controller._pullAgain === true) {
        controller._pullAgain = false;
        ReadableByteStreamControllerCallPullIfNeeded(controller);
      }
    },
    e => {
      ReadableByteStreamControllerError(controller, e);
    }
  );
}

function ReadableByteStreamControllerClearAlgorithms(controller) {
  controller._pullAlgorithm = undefined;
  controller._cancelAlgorithm = undefined;
}

function ReadableByteStreamControllerClearPendingPullIntos(controller) {
  ReadableByteStreamControllerInvalidateBYOBRequest(controller);
  controller._pendingPullIntos = [];
}

function ReadableByteStreamControllerClose(controller) {
  const stream = controller._stream;

  if (controller._closeRequested === true || stream._state !== 'readable') {
    return;
  }

  if (controller._queueTotalSize > 0) {
    controller._closeRequested = true;

    return;
  }

  if (controller._pendingPullIntos.length > 0) {
    const firstPendingPullInto = controller._pendingPullIntos[0];
    if (firstPendingPullInto.bytesFilled % firstPendingPullInto.elementSize !== 0) {
      const e = new TypeError('Insufficient bytes to fill elements in the given buffer');
      ReadableByteStreamControllerError(controller, e);

      throw e;
    }
  }

  ReadableByteStreamControllerClearAlgorithms(controller);
  ReadableStreamClose(stream);
}

function ReadableByteStreamControllerCommitPullIntoDescriptor(stream, pullIntoDescriptor) {
  assert(stream._state !== 'errored');
  assert(pullIntoDescriptor.readerType !== 'none');

  let done = false;
  if (stream._state === 'closed') {
    assert(pullIntoDescriptor.bytesFilled % pullIntoDescriptor.elementSize === 0);
    done = true;
  }

  const filledView = ReadableByteStreamControllerConvertPullIntoDescriptor(pullIntoDescriptor);
  if (pullIntoDescriptor.readerType === 'default') {
    ReadableStreamFulfillReadRequest(stream, filledView, done);
  } else {
    assert(pullIntoDescriptor.readerType === 'byob');
    ReadableStreamFulfillReadIntoRequest(stream, filledView, done);
  }
}

function ReadableByteStreamControllerConvertPullIntoDescriptor(pullIntoDescriptor) {
  const bytesFilled = pullIntoDescriptor.bytesFilled;
  const elementSize = pullIntoDescriptor.elementSize;

  assert(bytesFilled <= pullIntoDescriptor.byteLength);
  assert(bytesFilled % elementSize === 0);

  const buffer = TransferArrayBuffer(pullIntoDescriptor.buffer);
  return new pullIntoDescriptor.viewConstructor(buffer, pullIntoDescriptor.byteOffset, bytesFilled / elementSize);
}

function ReadableByteStreamControllerEnqueue(controller, chunk) {
  const stream = controller._stream;

  if (controller._closeRequested === true || stream._state !== 'readable') {
    return;
  }

  const buffer = chunk.buffer;
  const byteOffset = chunk.byteOffset;
  const byteLength = chunk.byteLength;
  if (IsDetachedBuffer(buffer) === true) {
    throw new TypeError('chunk\'s buffer is detached and so cannot be enqueued');
  }
  const transferredBuffer = TransferArrayBuffer(buffer);

  if (controller._pendingPullIntos.length > 0) {
    const firstPendingPullInto = controller._pendingPullIntos[0];
    if (IsDetachedBuffer(firstPendingPullInto.buffer) === true) {
      throw new TypeError(
        'The BYOB request\'s buffer has been detached and so cannot be filled with an enqueued chunk'
      );
    }
    ReadableByteStreamControllerInvalidateBYOBRequest(controller);
    firstPendingPullInto.buffer = TransferArrayBuffer(firstPendingPullInto.buffer);
    if (firstPendingPullInto.readerType === 'none') {
      ReadableByteStreamControllerEnqueueDetachedPullIntoToQueue(controller, firstPendingPullInto);
    }
  }

  if (ReadableStreamHasDefaultReader(stream) === true) {
    ReadableByteStreamControllerProcessReadRequestsUsingQueue(controller);
    if (ReadableStreamGetNumReadRequests(stream) === 0) {
      assert(controller._pendingPullIntos.length === 0);
      ReadableByteStreamControllerEnqueueChunkToQueue(controller, transferredBuffer, byteOffset, byteLength);
    } else {
      assert(controller._queue.length === 0);
      if (controller._pendingPullIntos.length > 0) {
        assert(controller._pendingPullIntos[0].readerType === 'default');
        ReadableByteStreamControllerShiftPendingPullInto(controller);
      }
      const transferredView = new Uint8Array(transferredBuffer, byteOffset, byteLength);
      ReadableStreamFulfillReadRequest(stream, transferredView, false);
    }
  } else if (ReadableStreamHasBYOBReader(stream) === true) {
    // TODO: Ideally in this branch detaching should happen only if the buffer is not consumed fully.
    ReadableByteStreamControllerEnqueueChunkToQueue(controller, transferredBuffer, byteOffset, byteLength);
    const filledPullIntos = ReadableByteStreamControllerProcessPullIntoDescriptorsUsingQueue(controller);
    for (const filledPullInto of filledPullIntos) {
      ReadableByteStreamControllerCommitPullIntoDescriptor(stream, filledPullInto);
    }
  } else {
    assert(IsReadableStreamLocked(stream) === false);
    ReadableByteStreamControllerEnqueueChunkToQueue(controller, transferredBuffer, byteOffset, byteLength);
  }

  ReadableByteStreamControllerCallPullIfNeeded(controller);
}

function ReadableByteStreamControllerEnqueueChunkToQueue(controller, buffer, byteOffset, byteLength) {
  controller._queue.push({ buffer, byteOffset, byteLength });
  controller._queueTotalSize += byteLength;
}

function ReadableByteStreamControllerEnqueueClonedChunkToQueue(controller, buffer, byteOffset, byteLength) {
  let clonedChunk;
  try {
    clonedChunk = buffer.slice(byteOffset, byteOffset + byteLength);
  } catch (cloneE) {
    ReadableByteStreamControllerError(controller, cloneE);
    throw cloneE;
  }
  ReadableByteStreamControllerEnqueueChunkToQueue(controller, clonedChunk, 0, byteLength);
}

function ReadableByteStreamControllerEnqueueDetachedPullIntoToQueue(controller, firstDescriptor) {
  assert(firstDescriptor.readerType === 'none');
  if (firstDescriptor.bytesFilled > 0) {
    ReadableByteStreamControllerEnqueueClonedChunkToQueue(
      controller,
      firstDescriptor.buffer,
      firstDescriptor.byteOffset,
      firstDescriptor.bytesFilled
    );
  }
  ReadableByteStreamControllerShiftPendingPullInto(controller);
}

function ReadableByteStreamControllerError(controller, e) {
  const stream = controller._stream;

  if (stream._state !== 'readable') {
    return;
  }

  ReadableByteStreamControllerClearPendingPullIntos(controller);

  ResetQueue(controller);
  ReadableByteStreamControllerClearAlgorithms(controller);
  ReadableStreamError(stream, e);
}

function ReadableByteStreamControllerFillHeadPullIntoDescriptor(controller, size, pullIntoDescriptor) {
  assert(controller._pendingPullIntos.length === 0 || controller._pendingPullIntos[0] === pullIntoDescriptor);
  assert(controller._byobRequest === null);
  pullIntoDescriptor.bytesFilled += size;
}

function ReadableByteStreamControllerFillPullIntoDescriptorFromQueue(controller, pullIntoDescriptor) {
  const maxBytesToCopy = Math.min(controller._queueTotalSize,
                                  pullIntoDescriptor.byteLength - pullIntoDescriptor.bytesFilled);
  const maxBytesFilled = pullIntoDescriptor.bytesFilled + maxBytesToCopy;

  let totalBytesToCopyRemaining = maxBytesToCopy;
  let ready = false;
  assert(!IsDetachedBuffer(pullIntoDescriptor.buffer));
  assert(pullIntoDescriptor.bytesFilled < pullIntoDescriptor.minimumFill);
  const remainderBytes = maxBytesFilled % pullIntoDescriptor.elementSize;
  const maxAlignedBytes = maxBytesFilled - remainderBytes;
  // A descriptor for a read() request that is not yet filled up to its minimum length will stay at the head
  // of the queue, so the underlying source can keep filling it.
  if (maxAlignedBytes >= pullIntoDescriptor.minimumFill) {
    totalBytesToCopyRemaining = maxAlignedBytes - pullIntoDescriptor.bytesFilled;
    ready = true;
  }

  const queue = controller._queue;

  while (totalBytesToCopyRemaining > 0) {
    const headOfQueue = queue[0];

    const bytesToCopy = Math.min(totalBytesToCopyRemaining, headOfQueue.byteLength);

    const destStart = pullIntoDescriptor.byteOffset + pullIntoDescriptor.bytesFilled;

    const descriptorBuffer = pullIntoDescriptor.buffer;
    const queueBuffer = headOfQueue.buffer;
    const queueByteOffset = headOfQueue.byteOffset;
    assert(CanCopyDataBlockBytes(descriptorBuffer, destStart, queueBuffer, queueByteOffset,
                                 bytesToCopy));
    CopyDataBlockBytes(descriptorBuffer, destStart, queueBuffer, queueByteOffset, bytesToCopy);

    if (headOfQueue.byteLength === bytesToCopy) {
      queue.shift();
    } else {
      headOfQueue.byteOffset += bytesToCopy;
      headOfQueue.byteLength -= bytesToCopy;
    }
    controller._queueTotalSize -= bytesToCopy;

    ReadableByteStreamControllerFillHeadPullIntoDescriptor(controller, bytesToCopy, pullIntoDescriptor);

    totalBytesToCopyRemaining -= bytesToCopy;
  }

  if (ready === false) {
    assert(controller._queueTotalSize === 0);
    assert(pullIntoDescriptor.bytesFilled > 0);
    assert(pullIntoDescriptor.bytesFilled < pullIntoDescriptor.minimumFill);
  }

  return ready;
}

function ReadableByteStreamControllerFillReadRequestFromQueue(controller, readRequest) {
  assert(controller._queueTotalSize > 0);

  const entry = controller._queue.shift();
  controller._queueTotalSize -= entry.byteLength;

  ReadableByteStreamControllerHandleQueueDrain(controller);

  const view = new Uint8Array(entry.buffer, entry.byteOffset, entry.byteLength);
  readRequest.chunkSteps(view);
}

function ReadableByteStreamControllerGetBYOBRequest(controller) {
  if (controller._byobRequest === null && controller._pendingPullIntos.length > 0) {
    const firstDescriptor = controller._pendingPullIntos[0];
    const view = new Uint8Array(firstDescriptor.buffer,
                                firstDescriptor.byteOffset + firstDescriptor.bytesFilled,
                                firstDescriptor.byteLength - firstDescriptor.bytesFilled);

    const byobRequest = ReadableStreamBYOBRequest.new(globalThis);
    byobRequest._controller = controller;
    byobRequest._view = view;
    controller._byobRequest = byobRequest;
  }

  return controller._byobRequest;
}

function ReadableByteStreamControllerGetDesiredSize(controller) {
  const state = controller._stream._state;

  if (state === 'errored') {
    return null;
  }
  if (state === 'closed') {
    return 0;
  }

  return controller._strategyHWM - controller._queueTotalSize;
}

function ReadableByteStreamControllerHandleQueueDrain(controller) {
  assert(controller._stream._state === 'readable');

  if (controller._queueTotalSize === 0 && controller._closeRequested === true) {
    ReadableByteStreamControllerClearAlgorithms(controller);
    ReadableStreamClose(controller._stream);
  } else {
    ReadableByteStreamControllerCallPullIfNeeded(controller);
  }
}

function ReadableByteStreamControllerInvalidateBYOBRequest(controller) {
  if (controller._byobRequest === null) {
    return;
  }

  controller._byobRequest._controller = undefined;
  controller._byobRequest._view = null;
  controller._byobRequest = null;
}

function ReadableByteStreamControllerProcessPullIntoDescriptorsUsingQueue(controller) {
  assert(controller._closeRequested === false);

  const filledPullIntos = [];
  while (controller._pendingPullIntos.length > 0) {
    if (controller._queueTotalSize === 0) {
      break;
    }

    const pullIntoDescriptor = controller._pendingPullIntos[0];
    assert(pullIntoDescriptor.readerType !== 'none');

    if (ReadableByteStreamControllerFillPullIntoDescriptorFromQueue(controller, pullIntoDescriptor) === true) {
      ReadableByteStreamControllerShiftPendingPullInto(controller);
      filledPullIntos.push(pullIntoDescriptor);
    }
  }

  return filledPullIntos;
}

function ReadableByteStreamControllerProcessReadRequestsUsingQueue(controller) {
  const reader = controller._stream._reader;
  assert(ReadableStreamDefaultReader.isImpl(reader));
  while (reader._readRequests.length > 0) {
    if (controller._queueTotalSize === 0) {
      return;
    }
    const readRequest = reader._readRequests.shift();
    ReadableByteStreamControllerFillReadRequestFromQueue(controller, readRequest);
  }
}

function ReadableByteStreamControllerPullInto(controller, view, min, readIntoRequest) {
  const stream = controller._stream;

  let elementSize = 1;
  if (view.constructor !== DataView) {
    elementSize = view.constructor.BYTES_PER_ELEMENT;
  }

  const minimumFill = min * elementSize;
  assert(minimumFill >= elementSize && minimumFill <= view.byteLength);
  assert(minimumFill % elementSize === 0);

  const ctor = view.constructor;

  let buffer;
  try {
    buffer = TransferArrayBuffer(view.buffer);
  } catch (e) {
    readIntoRequest.errorSteps(e);
    return;
  }

  const pullIntoDescriptor = {
    buffer,
    bufferByteLength: buffer.byteLength,
    byteOffset: view.byteOffset,
    byteLength: view.byteLength,
    bytesFilled: 0,
    minimumFill,
    elementSize,
    viewConstructor: ctor,
    readerType: 'byob'
  };

  if (controller._pendingPullIntos.length > 0) {
    controller._pendingPullIntos.push(pullIntoDescriptor);

    // No ReadableByteStreamControllerCallPullIfNeeded() call since:
    // - No change happens on desiredSize
    // - The source has already been notified of that there's at least 1 pending read(view)

    ReadableStreamAddReadIntoRequest(stream, readIntoRequest);
    return;
  }

  if (stream._state === 'closed') {
    const emptyView = new view.constructor(pullIntoDescriptor.buffer, pullIntoDescriptor.byteOffset, 0);
    readIntoRequest.closeSteps(emptyView);
    return;
  }

  if (controller._queueTotalSize > 0) {
    if (ReadableByteStreamControllerFillPullIntoDescriptorFromQueue(controller, pullIntoDescriptor) === true) {
      const filledView = ReadableByteStreamControllerConvertPullIntoDescriptor(pullIntoDescriptor);

      ReadableByteStreamControllerHandleQueueDrain(controller);

      readIntoRequest.chunkSteps(filledView);
      return;
    }

    if (controller._closeRequested === true) {
      const e = new TypeError('Insufficient bytes to fill elements in the given buffer');
      ReadableByteStreamControllerError(controller, e);

      readIntoRequest.errorSteps(e);
      return;
    }
  }

  controller._pendingPullIntos.push(pullIntoDescriptor);

  ReadableStreamAddReadIntoRequest(stream, readIntoRequest);
  ReadableByteStreamControllerCallPullIfNeeded(controller);
}

function ReadableByteStreamControllerRespond(controller, bytesWritten) {
  assert(controller._pendingPullIntos.length > 0);

  const firstDescriptor = controller._pendingPullIntos[0];
  const state = controller._stream._state;

  if (state === 'closed') {
    if (bytesWritten !== 0) {
      throw new TypeError('bytesWritten must be 0 when calling respond() on a closed stream');
    }
  } else {
    assert(state === 'readable');
    if (bytesWritten === 0) {
      throw new TypeError('bytesWritten must be greater than 0 when calling respond() on a readable stream');
    }
    if (firstDescriptor.bytesFilled + bytesWritten > firstDescriptor.byteLength) {
      throw new RangeError('bytesWritten out of range');
    }
  }

  firstDescriptor.buffer = TransferArrayBuffer(firstDescriptor.buffer);

  ReadableByteStreamControllerRespondInternal(controller, bytesWritten);
}

function ReadableByteStreamControllerRespondInClosedState(controller, firstDescriptor) {
  assert(firstDescriptor.bytesFilled % firstDescriptor.elementSize === 0);

  if (firstDescriptor.readerType === 'none') {
    ReadableByteStreamControllerShiftPendingPullInto(controller);
  }

  const stream = controller._stream;
  if (ReadableStreamHasBYOBReader(stream) === true) {
    const filledPullIntos = [];
    while (filledPullIntos.length < ReadableStreamGetNumReadIntoRequests(stream)) {
      const pullIntoDescriptor = ReadableByteStreamControllerShiftPendingPullInto(controller);
      filledPullIntos.push(pullIntoDescriptor);
    }
    for (const filledPullInto of filledPullIntos) {
      ReadableByteStreamControllerCommitPullIntoDescriptor(stream, filledPullInto);
    }
  }
}

function ReadableByteStreamControllerRespondInReadableState(controller, bytesWritten, pullIntoDescriptor) {
  assert(pullIntoDescriptor.bytesFilled + bytesWritten <= pullIntoDescriptor.byteLength);

  ReadableByteStreamControllerFillHeadPullIntoDescriptor(controller, bytesWritten, pullIntoDescriptor);

  if (pullIntoDescriptor.readerType === 'none') {
    ReadableByteStreamControllerEnqueueDetachedPullIntoToQueue(controller, pullIntoDescriptor);
    const filledPullIntos = ReadableByteStreamControllerProcessPullIntoDescriptorsUsingQueue(controller);
    for (const filledPullInto of filledPullIntos) {
      ReadableByteStreamControllerCommitPullIntoDescriptor(controller._stream, filledPullInto);
    }
    return;
  }

  if (pullIntoDescriptor.bytesFilled < pullIntoDescriptor.minimumFill) {
    // A descriptor for a read() request that is not yet filled up to its minimum length will stay at the head
    // of the queue, so the underlying source can keep filling it.
    return;
  }

  ReadableByteStreamControllerShiftPendingPullInto(controller);

  const remainderSize = pullIntoDescriptor.bytesFilled % pullIntoDescriptor.elementSize;
  if (remainderSize > 0) {
    const end = pullIntoDescriptor.byteOffset + pullIntoDescriptor.bytesFilled;
    ReadableByteStreamControllerEnqueueClonedChunkToQueue(
      controller,
      pullIntoDescriptor.buffer,
      end - remainderSize,
      remainderSize
    );
  }

  pullIntoDescriptor.bytesFilled -= remainderSize;
  const filledPullIntos = ReadableByteStreamControllerProcessPullIntoDescriptorsUsingQueue(controller);

  ReadableByteStreamControllerCommitPullIntoDescriptor(controller._stream, pullIntoDescriptor);
  for (const filledPullInto of filledPullIntos) {
    ReadableByteStreamControllerCommitPullIntoDescriptor(controller._stream, filledPullInto);
  }
}

function ReadableByteStreamControllerRespondInternal(controller, bytesWritten) {
  const firstDescriptor = controller._pendingPullIntos[0];
  assert(CanTransferArrayBuffer(firstDescriptor.buffer) === true);

  ReadableByteStreamControllerInvalidateBYOBRequest(controller);

  const state = controller._stream._state;
  if (state === 'closed') {
    assert(bytesWritten === 0);
    ReadableByteStreamControllerRespondInClosedState(controller, firstDescriptor);
  } else {
    assert(state === 'readable');
    assert(bytesWritten > 0);
    ReadableByteStreamControllerRespondInReadableState(controller, bytesWritten, firstDescriptor);
  }

  ReadableByteStreamControllerCallPullIfNeeded(controller);
}

function ReadableByteStreamControllerRespondWithNewView(controller, view) {
  assert(controller._pendingPullIntos.length > 0);
  assert(IsDetachedBuffer(view.buffer) === false);

  const firstDescriptor = controller._pendingPullIntos[0];
  const state = controller._stream._state;

  if (state === 'closed') {
    if (view.byteLength !== 0) {
      throw new TypeError('The view\'s length must be 0 when calling respondWithNewView() on a closed stream');
    }
  } else {
    assert(state === 'readable');
    if (view.byteLength === 0) {
      throw new TypeError(
        'The view\'s length must be greater than 0 when calling respondWithNewView() on a readable stream'
      );
    }
  }

  if (firstDescriptor.byteOffset + firstDescriptor.bytesFilled !== view.byteOffset) {
    throw new RangeError('The region specified by view does not match byobRequest');
  }
  if (firstDescriptor.bufferByteLength !== view.buffer.byteLength) {
    throw new RangeError('The buffer of view has different capacity than byobRequest');
  }
  if (firstDescriptor.bytesFilled + view.byteLength > firstDescriptor.byteLength) {
    throw new RangeError('The region specified by view is larger than byobRequest');
  }

  const viewByteLength = view.byteLength;

  firstDescriptor.buffer = TransferArrayBuffer(view.buffer);

  ReadableByteStreamControllerRespondInternal(controller, viewByteLength);
}

function ReadableByteStreamControllerShiftPendingPullInto(controller) {
  assert(controller._byobRequest === null);
  const descriptor = controller._pendingPullIntos.shift();
  return descriptor;
}

function ReadableByteStreamControllerShouldCallPull(controller) {
  const stream = controller._stream;

  if (stream._state !== 'readable') {
    return false;
  }

  if (controller._closeRequested === true) {
    return false;
  }

  if (controller._started === false) {
    return false;
  }

  if (ReadableStreamHasDefaultReader(stream) === true && ReadableStreamGetNumReadRequests(stream) > 0) {
    return true;
  }

  if (ReadableStreamHasBYOBReader(stream) === true && ReadableStreamGetNumReadIntoRequests(stream) > 0) {
    return true;
  }

  const desiredSize = ReadableByteStreamControllerGetDesiredSize(controller);
  assert(desiredSize !== null);
  if (desiredSize > 0) {
    return true;
  }

  return false;
}

function SetUpReadableByteStreamController(stream, controller, startAlgorithm, pullAlgorithm, cancelAlgorithm,
                                           highWaterMark, autoAllocateChunkSize) {
  assert(stream._controller === undefined);
  if (autoAllocateChunkSize !== undefined) {
    assert(Number.isInteger(autoAllocateChunkSize) === true);
    assert(autoAllocateChunkSize > 0);
  }

  controller._stream = stream;

  controller._pullAgain = false;
  controller._pulling = false;

  controller._byobRequest = null;

  // Need to set the slots so that the assert doesn't fire. In the spec the slots already exist implicitly.
  controller._queue = controller._queueTotalSize = undefined;
  ResetQueue(controller);

  controller._closeRequested = false;
  controller._started = false;

  controller._strategyHWM = highWaterMark;

  controller._pullAlgorithm = pullAlgorithm;
  controller._cancelAlgorithm = cancelAlgorithm;

  controller._autoAllocateChunkSize = autoAllocateChunkSize;

  controller._pendingPullIntos = [];

  stream._controller = controller;

  const startResult = startAlgorithm();
  uponPromise(
    promiseResolvedWith(startResult),
    () => {
      controller._started = true;

      assert(controller._pulling === false);
      assert(controller._pullAgain === false);

      ReadableByteStreamControllerCallPullIfNeeded(controller);
    },
    r => {
      ReadableByteStreamControllerError(controller, r);
    }
  );
}

function SetUpReadableByteStreamControllerFromUnderlyingSource(
  stream, underlyingSource, underlyingSourceDict, highWaterMark) {
  const controller = ReadableByteStreamController.new(globalThis);

  let startAlgorithm = () => undefined;
  let pullAlgorithm = () => promiseResolvedWith(undefined);
  let cancelAlgorithm = () => promiseResolvedWith(undefined);

  if ('start' in underlyingSourceDict) {
    startAlgorithm = () => underlyingSourceDict.start.call(underlyingSource, controller);
  }
  if ('pull' in underlyingSourceDict) {
    pullAlgorithm = () => underlyingSourceDict.pull.call(underlyingSource, controller);
  }
  if ('cancel' in underlyingSourceDict) {
    cancelAlgorithm = reason => underlyingSourceDict.cancel.call(underlyingSource, reason);
  }

  const autoAllocateChunkSize = underlyingSourceDict.autoAllocateChunkSize;
  if (autoAllocateChunkSize === 0) {
    throw new TypeError('autoAllocateChunkSize must be greater than 0');
  }

  SetUpReadableByteStreamController(
    stream, controller, startAlgorithm, pullAlgorithm, cancelAlgorithm, highWaterMark, autoAllocateChunkSize
  );
}

function ReadableStreamFromIterable(asyncIterable) {
  let stream;
  const iteratorRecord = GetIterator(asyncIterable, 'async');

  const startAlgorithm = () => undefined;

  function pullAlgorithm() {
    let nextResult;
    try {
      nextResult = IteratorNext(iteratorRecord);
    } catch (e) {
      return promiseRejectedWith(e);
    }
    const nextPromise = promiseResolvedWith(nextResult);
    return transformPromiseWith(nextPromise, iterResult => {
      if (!typeIsObject(iterResult)) {
        throw new TypeError('The promise returned by the iterator.next() method must fulfill with an object');
      }
      const done = IteratorComplete(iterResult);
      if (done === true) {
        ReadableStreamDefaultControllerClose(stream._controller);
      } else {
        const value = IteratorValue(iterResult);
        ReadableStreamDefaultControllerEnqueue(stream._controller, value);
      }
    });
  }

  function cancelAlgorithm(reason) {
    const iterator = iteratorRecord.iterator;
    let returnMethod;
    try {
      returnMethod = GetMethod(iterator, 'return');
    } catch (e) {
      return promiseRejectedWith(e);
    }
    if (returnMethod === undefined) {
      return promiseResolvedWith(undefined);
    }
    let returnResult;
    try {
      returnResult = Call(returnMethod, iterator, [reason]);
    } catch (e) {
      return promiseRejectedWith(e);
    }
    const returnPromise = promiseResolvedWith(returnResult);
    return transformPromiseWith(returnPromise, iterResult => {
      if (!typeIsObject(iterResult)) {
        throw new TypeError('The promise returned by the iterator.return() method must fulfill with an object');
      }
      return undefined;
    });
  }

  stream = CreateReadableStream(startAlgorithm, pullAlgorithm, cancelAlgorithm, 0);
  return stream;
}
