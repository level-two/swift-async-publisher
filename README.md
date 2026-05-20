# AsyncPublisher

`AsyncPublisher` bridges Swift concurrency into Combine with a demand-aware publisher named `Publishers.Async`.

## Requirements

- Swift 6.3 package manifest
- Combine
- macOS 15, iOS 18, tvOS 18, or watchOS 11

The higher platform minimums are required because the package uses typed `AsyncSequence.Failure`.

## Installation

Add this package to another SwiftPM package:

```swift
dependencies: [
    .package(path: "../AsyncPublisher")
]
```

Then add the product to your target:

```swift
.target(
    name: "YourTarget",
    dependencies: [
        .product(name: "AsyncPublisher", package: "AsyncPublisher")
    ]
)
```

Import both Combine and AsyncPublisher where you use it:

```swift
import Combine
import AsyncPublisher
```

## Behavior

`Publishers.Async` creates one producer task per subscription.

For single-value async operations, the operation starts only after downstream requests demand. For async sequences and streams, the publisher waits for demand before calling `next()` for the next element. This keeps the bridge pull-based from Combine's point of view and avoids reading ahead from an `AsyncSequence`.

When the subscription is cancelled or its `AnyCancellable` is destroyed, the producer task is cancelled.

## Async Closure

Use this for non-throwing async work. The publisher has `Failure == Never`.

```swift
let publisher = Publishers.Async<Int, Never> {
    await loadCount()
}

let cancellable = publisher.sink { value in
    print(value)
}
```

## Async Throwing Closure

Use this for throwing async work. The publisher has `Failure == Error`.

```swift
let publisher = Publishers.Async<User, Error> {
    try await api.loadCurrentUser()
}

let cancellable = publisher.sink(
    receiveCompletion: { completion in
        if case .failure(let error) = completion {
            print("Failed:", error)
        }
    },
    receiveValue: { user in
        print(user)
    }
)
```

## Task

Wrap an existing `Task`.

```swift
let task = Task<Data, Error> {
    try await downloadData()
}

let publisher = Publishers.Async<Data, Error>(task: task)

let cancellable = publisher.sink(
    receiveCompletion: { print($0) },
    receiveValue: { print($0.count) }
)
```

Cancelling the Combine subscription cancels the wrapped task:

```swift
cancellable.cancel()
```

## AsyncSequence

Wrap any typed-failure `AsyncSequence`.

```swift
let stream = AsyncStream<Int> { continuation in
    continuation.yield(1)
    continuation.yield(2)
    continuation.finish()
}

let publisher = Publishers.Async<Int, Never>(stream)

let cancellable = publisher.sink { value in
    print(value)
}
```

The publisher waits for downstream demand before asking the sequence for the next value.

## AsyncStream-Like Continuation

Create a non-throwing stream inline.

```swift
let publisher = Publishers.Async<String, Never> { continuation in
    continuation.yield("ready")
    continuation.yield("done")
    continuation.finish()
}

let cancellable = publisher.sink { value in
    print(value)
}
```

You can also pass a buffering policy:

```swift
let publisher = Publishers.Async<Int, Never>(
    bufferingPolicy: .bufferingNewest(1)
) { continuation in
    continuation.yield(1)
    continuation.finish()
}
```

## AsyncThrowingStream-Like Continuation

Create a throwing stream inline. This initializer currently exposes `Failure == Error`.

```swift
let publisher = Publishers.Async<Int, Error> { continuation in
    continuation.yield(1)
    continuation.finish(throwing: URLError(.badServerResponse))
}

let cancellable = publisher.sink(
    receiveCompletion: { print($0) },
    receiveValue: { print($0) }
)
```

## Backpressure

The bridge is demand-aware:

1. It waits until downstream requests at least one item.
2. It reserves one demand unit.
3. It awaits the async operation or pulls one element from the async sequence.
4. It sends the value downstream.
5. It repeats only after more demand is available.

This means a subscriber that requests `.max(1)` from an async sequence receives one element and the sequence is not pulled again until the subscriber requests more demand.

## Async Sink

The package also adds async overloads for Combine's `sink`.

Use this when value handling itself needs `await`:

```swift
let cancellable = publisher.sink { value in
    await database.save(value)
}
```

For publishers that can fail, use the completion overload:

```swift
let cancellable = publisher.sink(
    receiveCompletion: { completion in
        await logger.record(completion)
    },
    receiveValue: { value in
        await database.save(value)
    }
)
```

The async sink requests one value at a time. It awaits the value handler before requesting the next value, so it preserves backpressure when upstream honors demand. If upstream sends completion while a value handler is still running, completion is delivered after that handler finishes.

Cancelling the returned `AnyCancellable` cancels the upstream subscription and the currently running async value handler:

```swift
cancellable.cancel()
```

## Running Tests

```sh
swift test
```

The test suite covers:

- non-throwing async closures
- throwing async closures
- task cancellation
- async sequence backpressure
- non-throwing continuation streams
- throwing continuation streams
- stream termination on cancellation
- async sink demand handling
- async sink completion ordering
- async sink cancellation
