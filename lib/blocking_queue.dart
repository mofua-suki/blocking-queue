/// Asynchronous queues with FIFO or latest-item buffering.
library;

import 'dart:async';
import 'dart:collection';

/// A queue whose [dequeue] future waits when its buffer is empty.
///
/// A queue is used within one Dart isolate and delivers each non-null item to
/// one consumer. Waiting consumers are served in request order. Choose
/// [UnboundedBlockingQueue] for FIFO buffering or [SingleCapacityBlockingQueue]
/// for a buffer that keeps the latest pending item.
sealed class BlockingQueue<T extends Object> {
  /// Initializes an empty queue for a concrete buffering policy.
  BlockingQueue();

  final _waiters = Queue<Completer<T>>();
  bool _isDisposed = false;

  /// Delivers [item] to the oldest waiting consumer or stores it in the buffer.
  ///
  /// This method completes synchronously. The buffering policy applies only
  /// when no consumer is waiting. Throws [AlreadyDisposedException] if the
  /// queue has been disposed.
  void enqueue(T item) {
    if (_isDisposed) {
      throw const AlreadyDisposedException(
        'Cannot enqueue on a disposed queue.',
      );
    }

    if (_waiters.isNotEmpty) {
      // Preserve Future-valued items as data.
      _waiters.removeFirst().complete(Future<T>.syncValue(item));
    } else {
      _storeItem(item);
    }
  }

  /// Returns a buffered item or waits for a producer to enqueue one.
  ///
  /// If several consumers are waiting, subsequent items go to those consumers
  /// in the order they called [dequeue]. The returned future completes with
  /// [AlreadyDisposedException] if the queue is disposed while waiting, or was
  /// already disposed.
  ///
  /// Timing out the returned future with [Future.timeout] leaves this dequeue
  /// pending until the queue delivers an item or is disposed.
  Future<T> dequeue() {
    if (_isDisposed) {
      return Future<T>.error(
        const AlreadyDisposedException('Cannot dequeue from a disposed queue.'),
        StackTrace.current,
      );
    }

    final item = _takeItem();
    if (item != null) return Future<T>.syncValue(item);

    final waiter = Completer<T>();
    _waiters.addLast(waiter);
    return waiter.future;
  }

  /// Discards buffered items and fails every pending [dequeue] with
  /// [AlreadyDisposedException].
  ///
  /// Items already handed to consumers are unaffected. Repeated calls have no
  /// effect.
  void dispose() {
    if (_isDisposed) return;
    _isDisposed = true;
    _clearItems();

    while (_waiters.isNotEmpty) {
      _waiters.removeFirst().completeError(
        const AlreadyDisposedException(
          'Queue disposed while waiting for an item.',
        ),
      );
    }
  }

  void _storeItem(T item);
  T? _takeItem();
  void _clearItems();
}

/// Buffers all items and dequeues them in the order they were enqueued.
///
/// The buffer grows as needed. Producers can outpace consumers because
/// [BlockingQueue.enqueue] does not wait for buffer space.
final class UnboundedBlockingQueue<T extends Object> extends BlockingQueue<T> {
  /// Creates an empty FIFO queue.
  UnboundedBlockingQueue();

  final _items = Queue<T>();

  @override
  void _storeItem(T item) => _items.addLast(item);

  @override
  T? _takeItem() => _items.isEmpty ? null : _items.removeFirst();

  @override
  void _clearItems() => _items.clear();
}

/// Buffers one item, replacing it when a newer item arrives.
///
/// Items that can be delivered to waiting consumers bypass the buffer.
/// A replacement affects only the pending value, so consumers can still
/// finish processing previously delivered items.
final class SingleCapacityBlockingQueue<T extends Object>
    extends BlockingQueue<T> {
  /// Creates an empty queue with room for one buffered item.
  SingleCapacityBlockingQueue();

  T? _latestItem;

  @override
  void _storeItem(T item) => _latestItem = item;

  @override
  T? _takeItem() {
    final item = _latestItem;
    _latestItem = null;
    return item;
  }

  @override
  void _clearItems() => _latestItem = null;
}

/// A queue was disposed before the requested operation could complete.
final class AlreadyDisposedException implements Exception {
  /// Creates an exception with an operation-specific [message].
  const AlreadyDisposedException(this.message);

  /// Describes the operation that could not complete.
  final String message;

  @override
  String toString() => 'AlreadyDisposedException: $message';
}
