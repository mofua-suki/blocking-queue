# Blocking Queue

Hand off items from a producer to one or more asynchronous consumers in the
same Dart isolate. Call `dequeue()` to wait for the next item when the buffer is
empty.

## Choose a buffer

| Queue | When to use it | What happens with a slow consumer |
| --- | --- | --- |
| `UnboundedBlockingQueue<T>` | Jobs or events that must all be delivered | Items accumulate in FIFO order. Size and memory use can grow. |
| `SingleCapacityBlockingQueue<T>` | State snapshots or refresh signals that supersede earlier values | One item can stay pending. Each new buffered item replaces the previous one. |

## Process every queued item

```dart
import 'package:blocking_queue/blocking_queue.dart';

Future<void> main() async {
  final queue = UnboundedBlockingQueue<String>();
  try {
    queue.enqueue('first');
    queue.enqueue('second');
    print(await queue.dequeue()); // first
    print(await queue.dequeue()); // second
  } finally {
    queue.dispose();
  }
}
```

## Keep the latest pending value

```dart
import 'package:blocking_queue/blocking_queue.dart';

Future<void> main() async {
  final queue = SingleCapacityBlockingQueue<String>();

  final waiting = queue.dequeue();
  queue.enqueue('Starting');
  print(await waiting); // Starting

  queue.enqueue('Refreshing');
  queue.enqueue('Ready');
  print(await queue.dequeue()); // Ready

  queue.dispose();
}
```

## Delivery and lifecycle

- `enqueue()` is synchronous. It hands the item directly to the oldest waiter
  when there is one. This applies to both queues, including when several
  consumers are waiting on a single-capacity queue.
- `dequeue()` returns a `Future<T>`. Each item goes to one consumer, and
  waiting consumers receive items in the order they called `dequeue()`.
  Consumers can finish processing at different times.
- Queues are for a single Dart isolate, and `T` is non-nullable. An empty
  queue waits until an item arrives or the owner disposes it. A caller-side
  `Future.timeout` leaves the underlying dequeue pending, so that waiter can
  still consume a later item.
- `dispose()` is idempotent. It discards buffered items, fails outstanding
  waits with `AlreadyDisposedException`, and leaves delivered items with
  their consumers. To finish buffered work, drain it before disposal.
- After disposal, `enqueue()` throws synchronously and `dequeue()` returns a
  failed future. Handle errors from outstanding dequeue futures during
  shutdown.
