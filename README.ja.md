# Blocking Queue

[English](https://github.com/mofua-suki/blocking-queue/blob/main/README.md) · [pub.dev](https://pub.dev/packages/blocking_queue)

同じ Dart isolate 内で、生産者から一つ以上の非同期コンシューマーに項目を渡せます。バッファが空のときに `dequeue()` を呼ぶと、次の項目が届くまで待ちます。

## インストール

対応する Dart SDK は 3.10.0 以上 4.0.0 未満です。

```sh
dart pub add blocking_queue
```

## バッファの選び方

| キュー | 用途 | コンシューマーの処理が遅いとき |
| --- | --- | --- |
| `UnboundedBlockingQueue<T>` | すべて届けたいジョブやイベント | 項目を到着順に蓄えます。件数とメモリ使用量は増えることがあります。 |
| `SingleCapacityBlockingQueue<T>` | 最新の状態や更新通知 | 保持するのは未配信の1件です。新しい項目が前の項目を上書きします。 |

## すべての項目を順番に処理する

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

## 未配信の値を最新の1件に保つ

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

## 配信と終了処理

キュー側でのタイムアウトと待機のキャンセルは `blocking_queue` 0.1.2 以降で使えます。

- `enqueue()` は同期メソッドです。待機中のコンシューマーがいれば、最も早く待ち始めたものに直接渡します。容量1のキューで複数のコンシューマーが待っている場合も同じです。
- `dequeue()` は `Future<T>` を返します。各項目は一つのコンシューマーが受け取ります。配信順は `dequeue()` の呼び出し順ですが、処理の完了順はコンシューマーによって異なります。
- キューは単一の isolate 内で使います。`T` に `null` は使えません。空のキューは項目の到着または破棄まで待機します。`dequeue(timeout: duration)` を使うと、時間切れで待機を取り消し、`TimeoutException` が返ります。呼び出し側の `Future.timeout` では元の待機が残ります。
- 手動で取り消す場合は `final request = queue.dequeueCancellable()` でハンドルを保持し、キャンセルボタンの押下など、別のイベントのハンドラーに `request.cancel` を登録します。`try` 内で `request.future` を `await` し、待機が取り消されたときの `DequeueCancelledException` を処理してください。引き渡し後の `cancel()` で項目の行き先は変わりません。
- `dispose()` は繰り返し呼べます。バッファの項目を破棄し、待機中の Future を `AlreadyDisposedException` で終了します。すでに引き渡した項目はそのまま処理できます。バッファ内の仕事を完了させたい場合は、破棄する前に取り出してください。
- 破棄後の `enqueue()` は同期的に例外を投げ、`dequeue()` はエラーで完了する Future を返します。終了処理では待機中の Future のエラーも処理してください。

## 開発

このリポジトリのルートで次のコマンドを実行します。

```sh
dart pub get
dart analyze
dart test
```

使用例は `dart run example/blocking_queue_example.dart` で実行できます。
