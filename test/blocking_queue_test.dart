import 'dart:async';

import 'package:blocking_queue/blocking_queue.dart';
import 'package:test/test.dart';

void main() {
  test('the unbounded queue buffers every item in FIFO order', () async {
    final queue = UnboundedBlockingQueue<int>();
    queue.enqueue(1);
    queue.enqueue(2);

    expect(await queue.dequeue(), 1);
    expect(await queue.dequeue(), 2);
  });

  test('the single-capacity queue buffers only the most recent item', () async {
    final queue = SingleCapacityBlockingQueue<int>();
    queue.enqueue(1);
    queue.enqueue(2);

    expect(await queue.dequeue(), 2);

    final next = queue.dequeue();
    queue.enqueue(3);
    expect(await next, 3);
  });

  test('future items are delivered as values', () async {
    final queue = UnboundedBlockingQueue<Object>();
    final item = Future<int>.value(1);

    queue.enqueue(item);
    expect(identical(await queue.dequeue(), item), isTrue);

    final waiting = queue.dequeue();
    queue.enqueue(item);
    expect(identical(await waiting, item), isTrue);
  });

  final queueFactories = <String, BlockingQueue<int> Function()>{
    'unbounded': UnboundedBlockingQueue<int>.new,
    'single-capacity': SingleCapacityBlockingQueue<int>.new,
  };

  for (final MapEntry(key: name, value: createQueue)
      in queueFactories.entries) {
    group(name, () {
      late BlockingQueue<int> queue;

      setUp(() => queue = createQueue());

      test('an empty queue waits for a producer', () async {
        var delivered = false;
        final waiting = queue.dequeue().then((item) {
          delivered = true;
          return item;
        });

        await Future<void>.delayed(Duration.zero);
        expect(delivered, isFalse);
        queue.enqueue(1);
        expect(await waiting, 1);
      });

      test('waiting consumers receive items in request order', () async {
        final first = queue.dequeue();
        final second = queue.dequeue();

        queue.enqueue(1);
        queue.enqueue(2);

        expect(await first, 1);
        expect(await second, 2);
      });

      test(
        'an expired request leaves the next item for the next waiter',
        () async {
          final expired = queue.dequeue(timeout: Duration.zero);
          final next = queue.dequeue();

          await expectLater(expired, throwsA(isA<TimeoutException>()));
          queue.enqueue(42);
          expect(await next, 42);
        },
      );

      test('a failed timer registration removes its waiter', () async {
        final error = StateError('Timer registration failed');
        expect(
          () => runZoned(
            () => queue.dequeueCancellable(timeout: Duration.zero),
            zoneSpecification: ZoneSpecification(
              createTimer: (self, parent, zone, duration, callback) =>
                  throw error,
            ),
          ),
          throwsA(same(error)),
        );

        final next = queue.dequeue();
        queue.enqueue(1);
        queue.enqueue(2);
        expect(await next, 1);
      });

      test('cancelling one waiter preserves the order of the others', () async {
        final first = queue.dequeue();
        final abandoned = queue.dequeueCancellable(timeout: Duration.zero);
        final last = queue.dequeue();
        final cancelled = expectLater(
          abandoned.future,
          throwsA(isA<DequeueCancelledException>()),
        );
        abandoned.cancel();
        abandoned.cancel();
        queue.enqueue(42);
        queue.enqueue(43);

        await cancelled;
        expect(await first, 42);
        expect(await last, 43);
      });

      test(
        'cancellation leaves buffered and assigned values available',
        () async {
          final pending = queue.dequeueCancellable(timeout: Duration.zero);
          queue.enqueue(42);
          pending.cancel();
          expect(await pending.future, 42);

          queue.enqueue(43);
          final buffered = queue.dequeueCancellable(timeout: Duration.zero);
          buffered.cancel();
          expect(await buffered.future, 43);
        },
      );

      test('disposal fails every waiter and can be repeated', () async {
        final first = expectLater(
          queue.dequeue(),
          throwsA(isA<AlreadyDisposedException>()),
        );
        final second = expectLater(
          queue.dequeue(),
          throwsA(isA<AlreadyDisposedException>()),
        );

        queue.dispose();
        queue.dispose();

        await Future.wait([first, second]);
      });

      test('disposal rejects new work even with buffered items', () async {
        queue.enqueue(1);
        queue.dispose();

        expect(
          () => queue.enqueue(2),
          throwsA(isA<AlreadyDisposedException>()),
        );
        await expectLater(
          queue.dequeue(),
          throwsA(isA<AlreadyDisposedException>()),
        );
      });

      test('an item handed to a waiter survives disposal', () async {
        final waiting = queue.dequeue();
        queue.enqueue(1);
        queue.dispose();

        expect(await waiting, 1);
      });
    });
  }
}
