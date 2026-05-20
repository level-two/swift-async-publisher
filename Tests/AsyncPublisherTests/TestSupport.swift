import Combine
import Foundation

enum TestError: Error, Equatable {
    case failed
}

final class RecordingSubscriber<Input: Equatable, Failure: Error>: Subscriber, @unchecked Sendable {
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

actor AsyncFlag {
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

actor AsyncValue<Value> {
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

actor AsyncGate {
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

actor AsyncList<Value: Equatable> {
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

actor PullCounter {
    private(set) var count = 0

    func increment() {
        count += 1
    }
}

struct PullTrackedSequence: AsyncSequence {
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
