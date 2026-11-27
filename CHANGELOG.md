## 0.1.2

- Add timeouts and manual cancellation that remove pending dequeue requests.
- Remove the pending request if a custom zone throws during timer registration.

## 0.1.1

- Add a Japanese README and link both language editions to pub.dev.

## 0.1.0

- Add an awaitable FIFO queue and a queue that retains the latest buffered item.
- Discard buffered items and release waiting consumers on disposal.
