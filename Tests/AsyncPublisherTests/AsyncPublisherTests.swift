import Combine
import Foundation
import Testing
@testable import AsyncPublisher

@Test func asyncClosurePublishesValueAndCompletes() async {
    let subscriber = RecordingSubscriber<Int, Never>(initialDemand: .max(1))
    let publisher = Publishers.Async<Int, Never> {
        42
    }

    publisher.subscribe(subscriber)

    #expect(await subscriber.values(count: 1) == [42])
    #expect(await subscriber.completion() == .finished)
}

@Test func throwingAsyncClosurePublishesFailure() async {
    let subscriber = RecordingSubscriber<Int, Error>(initialDemand: .max(1))
    let publisher = Publishers.Async<Int, Error> {
        throw TestError.failed
    }

    publisher.subscribe(subscriber)

    let completion = await subscriber.completion()

    guard case .failure(let error as TestError) = completion else {
        Issue.record("Expected TestError.failed, got \(String(describing: completion))")
        return
    }

    #expect(error == .failed)
    #expect(await subscriber.values(count: 0).isEmpty)
}

@Test func taskSubscriptionIsCancelledWhenCancellableIsDestroyed() async {
    let cancellation = AsyncFlag()
    let task = Task<Int, Never> {
        await withTaskCancellationHandler {
            try? await Task.sleep(nanoseconds: 10_000_000_000)
            return 1
        } onCancel: {
            Task {
                await cancellation.fulfill()
            }
        }
    }

    let publisher = Publishers.Async<Int, Never>(task: task)
    var cancellable: AnyCancellable? = publisher.sink { _ in
        Issue.record("Unexpected value")
    }

    #expect(cancellable != nil)
    cancellable = nil

    #expect(await cancellation.wait())
}

@Test func asyncSequencePullsOnlyWhenThereIsDemand() async throws {
    let pulls = PullCounter()
    let sequence = PullTrackedSequence(values: [1, 2, 3], pulls: pulls)
    let subscriber = RecordingSubscriber<Int, Never>(initialDemand: .max(1))
    let publisher = Publishers.Async<Int, Never>(sequence)

    publisher.subscribe(subscriber)

    #expect(await subscriber.values(count: 1) == [1])
    try await Task.sleep(nanoseconds: 100_000_000)
    #expect(await pulls.count == 1)

    await subscriber.request(.max(1))

    #expect(await subscriber.values(count: 2) == [1, 2])
    try await Task.sleep(nanoseconds: 100_000_000)
    #expect(await pulls.count == 2)
}

@Test func asyncStreamContinuationPublishesValuesAndCompletion() async {
    let subscriber = RecordingSubscriber<Int, Never>(initialDemand: .unlimited)
    let publisher = Publishers.Async<Int, Never> { continuation in
        continuation.yield(1)
        continuation.yield(2)
        continuation.finish()
    }

    publisher.subscribe(subscriber)

    #expect(await subscriber.values(count: 2) == [1, 2])
    #expect(await subscriber.completion() == .finished)
}

@Test func throwingAsyncStreamContinuationPublishesFailure() async {
    let subscriber = RecordingSubscriber<Int, Error>(initialDemand: .unlimited)
    let publisher = Publishers.Async<Int, Error> { continuation in
        continuation.yield(1)
        continuation.finish(throwing: TestError.failed)
    }

    publisher.subscribe(subscriber)

    #expect(await subscriber.values(count: 1) == [1])

    guard case .failure(let error as TestError) = await subscriber.completion(), error == .failed else {
        Issue.record("Expected failed completion")
        return
    }
}

@Test func asyncStreamIsCancelledWithSubscription() async {
    let termination = AsyncValue<AsyncStream<Int>.Continuation.Termination>()
    let subscriber = RecordingSubscriber<Int, Never>(initialDemand: .unlimited)
    let publisher = Publishers.Async<Int, Never> { continuation in
        continuation.onTermination = { terminationReason in
            Task {
                await termination.set(terminationReason)
            }
        }

        continuation.yield(1)
    }

    publisher.subscribe(subscriber)

    #expect(await subscriber.values(count: 1) == [1])
    await subscriber.cancel()

    #expect(await termination.value() == .cancelled)
}

@Test func asyncSinkAwaitsValueHandlerBeforeRequestingNextSequenceElement() async throws {
    let pulls = PullCounter()
    let gate = AsyncGate()
    let values = AsyncList<Int>()
    let sequence = PullTrackedSequence(values: [1, 2], pulls: pulls)
    let publisher = Publishers.Async<Int, Never>(sequence)

    let cancellable = publisher.sink { value in
        await values.append(value)
        await gate.wait()
    }

    #expect(await values.values(count: 1) == [1])
    try await Task.sleep(nanoseconds: 100_000_000)
    #expect(await pulls.count == 1)

    await gate.open()

    #expect(await values.values(count: 2) == [1, 2])
    cancellable.cancel()
}

@Test func asyncSinkDelaysCompletionUntilValueHandlerFinishes() async throws {
    let gate = AsyncGate()
    let events = AsyncList<String>()
    let publisher = Publishers.Async<Int, Never> {
        1
    }

    let cancellable = publisher.sink(
        receiveCompletion: { _ in
            await events.append("completion")
        },
        receiveValue: { _ in
            await events.append("value-start")
            await gate.wait()
            await events.append("value-end")
        }
    )

    #expect(await events.values(count: 1) == ["value-start"])
    try await Task.sleep(nanoseconds: 100_000_000)
    #expect(await events.snapshot() == ["value-start"])

    await gate.open()

    #expect(await events.values(count: 3) == ["value-start", "value-end", "completion"])
    cancellable.cancel()
}

@Test func asyncSinkCancellationCancelsInFlightValueHandler() async {
    let subject = PassthroughSubject<Int, Never>()
    let started = AsyncFlag()
    let cancelled = AsyncFlag()

    let cancellable = subject.sink { _ in
        await withTaskCancellationHandler {
            await started.fulfill()
            try? await Task.sleep(nanoseconds: 10_000_000_000)
        } onCancel: {
            Task {
                await cancelled.fulfill()
            }
        }
    }

    subject.send(1)

    #expect(await started.wait())
    cancellable.cancel()
    #expect(await cancelled.wait())
}

private enum TestError: Error, Equatable {
    case failed
}

private final class RecordingSubscriber<Input: Equatable, Failure: Error>: Subscriber, @unchecked Sendable {
    private let lock = NSLock()
    private let initialDemand: Subscribers.Demand
    private let nextDemand: (Input) -> Subscribers.Demand

    private var subscription: Subscription?
    private var recordedValues: [Input] = []
    private var recordedCompletion: Subscribers.Completion<Failure>?
    private var valueWaiters: [(Int, CheckedContinuation<[Input], Never>)] = []
    private var completionWaiters: [CheckedContinuation<Subscribers.Completion<Failure>, Never>] = []

    init(
        initialDemand: Subscribers.Demand,
        nextDemand: @escaping (Input) -> Subscribers.Demand = { _ in .none }
    ) {
        self.initialDemand = initialDemand
        self.nextDemand = nextDemand
    }

    func receive(subscription: Subscription) {
        lock.lock()
        self.subscription = subscription
        lock.unlock()

        subscription.request(initialDemand)
    }

    func receive(_ input: Input) -> Subscribers.Demand {
        let waitersToResume: [(Int, CheckedContinuation<[Input], Never>)]

        lock.lock()
        recordedValues.append(input)
        waitersToResume = valueWaiters.filter { recordedValues.count >= $0.0 }
        valueWaiters.removeAll { recordedValues.count >= $0.0 }
        let values = recordedValues
        lock.unlock()

        waitersToResume.forEach { waiter in
            waiter.1.resume(returning: Array(values.prefix(waiter.0)))
        }

        return nextDemand(input)
    }

    func receive(completion: Subscribers.Completion<Failure>) {
        let waiters: [CheckedContinuation<Subscribers.Completion<Failure>, Never>]

        lock.lock()
        recordedCompletion = completion
        waiters = completionWaiters
        completionWaiters.removeAll()
        lock.unlock()

        waiters.forEach { $0.resume(returning: completion) }
    }

    func values(count: Int) async -> [Input] {
        await withCheckedContinuation { continuation in
            lock.lock()

            if recordedValues.count >= count {
                let values = Array(recordedValues.prefix(count))
                lock.unlock()
                continuation.resume(returning: values)
            } else {
                valueWaiters.append((count, continuation))
                lock.unlock()
            }
        }
    }

    func completion() async -> Subscribers.Completion<Failure> {
        await withCheckedContinuation { continuation in
            lock.lock()

            if let recordedCompletion {
                lock.unlock()
                continuation.resume(returning: recordedCompletion)
            } else {
                completionWaiters.append(continuation)
                lock.unlock()
            }
        }
    }

    func request(_ demand: Subscribers.Demand) async {
        subscriptionSnapshot()?.request(demand)
    }

    func cancel() async {
        takeSubscription()?.cancel()
    }

    private func subscriptionSnapshot() -> Subscription? {
        lock.lock()
        defer {
            lock.unlock()
        }

        return subscription
    }

    private func takeSubscription() -> Subscription? {
        lock.lock()
        defer {
            lock.unlock()
        }

        let subscription = subscription
        self.subscription = nil

        return subscription
    }
}

private actor AsyncFlag {
    private var isFulfilled = false
    private var waiters: [CheckedContinuation<Bool, Never>] = []

    func fulfill() {
        guard !isFulfilled else {
            return
        }

        isFulfilled = true
        waiters.forEach { $0.resume(returning: true) }
        waiters.removeAll()
    }

    func wait() async -> Bool {
        if isFulfilled {
            return true
        }

        return await withCheckedContinuation { continuation in
            waiters.append(continuation)
        }
    }
}

private actor AsyncValue<Value> {
    private var storedValue: Value?
    private var waiters: [CheckedContinuation<Value, Never>] = []

    func set(_ value: Value) {
        guard storedValue == nil else {
            return
        }

        storedValue = value
        waiters.forEach { $0.resume(returning: value) }
        waiters.removeAll()
    }

    func value() async -> Value {
        if let storedValue {
            return storedValue
        }

        return await withCheckedContinuation { continuation in
            waiters.append(continuation)
        }
    }
}

private actor AsyncGate {
    private var isOpen = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func open() {
        guard !isOpen else {
            return
        }

        isOpen = true
        waiters.forEach { $0.resume() }
        waiters.removeAll()
    }

    func wait() async {
        if isOpen {
            return
        }

        await withCheckedContinuation { continuation in
            waiters.append(continuation)
        }
    }
}

private actor AsyncList<Value: Equatable> {
    private var storedValues: [Value] = []
    private var waiters: [(Int, CheckedContinuation<[Value], Never>)] = []

    func append(_ value: Value) {
        storedValues.append(value)

        let waitersToResume = waiters.filter { storedValues.count >= $0.0 }
        waiters.removeAll { storedValues.count >= $0.0 }

        waitersToResume.forEach { waiter in
            waiter.1.resume(returning: Array(storedValues.prefix(waiter.0)))
        }
    }

    func values(count: Int) async -> [Value] {
        if storedValues.count >= count {
            return Array(storedValues.prefix(count))
        }

        return await withCheckedContinuation { continuation in
            waiters.append((count, continuation))
        }
    }

    func snapshot() -> [Value] {
        storedValues
    }
}

private actor PullCounter {
    private(set) var count = 0

    func increment() {
        count += 1
    }
}

private struct PullTrackedSequence: AsyncSequence {
    typealias Element = Int
    typealias Failure = Never

    let values: [Int]
    let pulls: PullCounter

    func makeAsyncIterator() -> Iterator {
        Iterator(values: values, pulls: pulls)
    }

    struct Iterator: AsyncIteratorProtocol {
        let values: [Int]
        let pulls: PullCounter
        var index = 0

        mutating func next() async -> Int? {
            await pulls.increment()

            guard index < values.count else {
                return nil
            }

            defer {
                index += 1
            }

            return values[index]
        }
    }
}
