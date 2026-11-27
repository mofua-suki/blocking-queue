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

  final _waiters = Queue<_Waiter<T>>();
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
      _waiters.removeFirst().deliver(item);
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
  /// A [timeout] removes the pending request and reports [TimeoutException].
  /// Applying [Future.timeout] to the result only limits the caller's wait.
  Future<T> dequeue({Duration? timeout}) =>
      dequeueCancellable(timeout: timeout).future;

  /// Starts a dequeue with a handle that can cancel it while it is pending.
  ///
  /// After cancellation, the future fails with [DequeueCancelledException].
  /// Once a value is assigned, calling cancel has no effect.
  ({Future<T> future, void Function() cancel}) dequeueCancellable({
    Duration? timeout,
  }) {
    if (_isDisposed) {
      return (
        future: Future<T>.error(
          const AlreadyDisposedException(
            'Cannot dequeue from a disposed queue.',
          ),
          StackTrace.current,
        ),
        cancel: _doNothing,
      );
    }

    final item = _takeItem();
    if (item != null) {
      return (future: Future<T>.syncValue(item), cancel: _doNothing);
    }

    final waiter = _Waiter<T>();
    _waiters.addLast(waiter);

    void withdraw(Object error) {
      if (_waiters.remove(waiter)) waiter.fail(error);
    }

    if (timeout != null) {
      try {
        waiter.timer = Timer(timeout, () {
          withdraw(
            TimeoutException('No item arrived before the timeout.', timeout),
          );
        });
      } catch (_) {
        _waiters.remove(waiter);
        rethrow;
      }
    }
    return (
      future: waiter.completer.future,
      cancel: () => withdraw(const DequeueCancelledException()),
    );
  }

  static void _doNothing() {}

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
      _waiters.removeFirst().fail(
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

final class _Waiter<T> {
  final completer = Completer<T>();
  Timer? timer;

  void deliver(T item) {
    timer?.cancel();
    completer.complete(Future<T>.syncValue(item));
  }

  void fail(Object error) {
    timer?.cancel();
    completer.completeError(error);
  }
}

/// A pending dequeue was cancelled before an item was assigned to it.
final class DequeueCancelledException implements Exception {
  const DequeueCancelledException();

  @override
  String toString() => 'DequeueCancelledException: Dequeue was cancelled.';
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
