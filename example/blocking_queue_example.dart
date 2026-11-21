import 'package:blocking_queue/blocking_queue.dart';

Future<void> main() async {
  final updates = SingleCapacityBlockingQueue<String>();

  final first = updates.dequeue();
  updates.enqueue('Starting');
  print(await first); // Starting

  updates.enqueue('Refreshing');
  updates.enqueue('Ready');
  print(await updates.dequeue()); // Ready

  updates.dispose();
}
