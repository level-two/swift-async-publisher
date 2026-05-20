import Combine
import Foundation

extension Publishers {
    /// A Combine publisher that bridges Swift concurrency work into demand-aware subscriptions.
    ///
    /// `Async` waits for downstream demand before starting a single async operation or before
    /// pulling the next element from an async sequence. Cancelling the subscription cancels the
    /// producer task owned by that subscription.
    public struct Async<Output, Failure: Error>: Publisher {
        private let operation: (AsyncSubscription<Output, Failure>) async -> Void
        private let onCancel: () -> Void

        private init(
            operation: @escaping (AsyncSubscription<Output, Failure>) async -> Void,
            onCancel: @escaping () -> Void = {}
        ) {
            self.operation = operation
            self.onCancel = onCancel
        }

        public init(_ operation: @escaping () async -> Output) where Failure == Never {
            let operationBox = UncheckedSendableBox(operation)

            self.init { subscription in
                guard await subscription.reserveDemand() else {
                    return
                }

                let value = await operationBox.value()

                guard await subscription.deliverReserved(value) else {
                    return
                }

                await subscription.finish(.finished)
            }
        }

        public init(_ operation: @escaping () async throws -> Output) where Failure == Error {
            let operationBox = UncheckedSendableBox(operation)

            self.init { subscription in
                guard await subscription.reserveDemand() else {
                    return
                }

                do {
                    let value = try await operationBox.value()

                    guard await subscription.deliverReserved(value) else {
                        return
                    }

                    await subscription.finish(.finished)
                } catch {
                    await subscription.finish(.failure(error))
                }
            }
        }

        public init(task: Task<Output, Failure>) {
            let taskBox = UncheckedSendableBox(task)

            self.init(
                operation: { subscription in
                    guard await subscription.reserveDemand() else {
                        return
                    }

                    do {
                        let value = try await taskBox.value.value

                        guard await subscription.deliverReserved(value) else {
                            return
                        }

                        await subscription.finish(.finished)
                    } catch {
                        await subscription.finish(.failure(error as! Failure))
                    }
                },
                onCancel: {
                    taskBox.value.cancel()
                }
            )
        }

        public init<Sequence: AsyncSequence>(
            _ sequence: Sequence
        ) where Sequence.Element == Output, Sequence.Failure == Failure {
            let sequenceBox = UncheckedSendableBox(sequence)

            self.init { subscription in
                await Self.run(sequenceBox.value, on: subscription)
            }
        }

        public init(
            bufferingPolicy: AsyncStream<Output>.Continuation.BufferingPolicy = .unbounded,
            _ build: @escaping (AsyncStream<Output>.Continuation) -> Void
        ) where Failure == Never {
            let buildBox = UncheckedSendableBox(build)

            self.init { subscription in
                let stream = AsyncStream(
                    Output.self,
                    bufferingPolicy: bufferingPolicy,
                    buildBox.value
                )

                await Self.run(stream, on: subscription)
            }
        }

        public init(
            bufferingPolicy: AsyncThrowingStream<Output, Error>.Continuation.BufferingPolicy = .unbounded,
            _ build: @escaping (AsyncThrowingStream<Output, Error>.Continuation) -> Void
        ) where Failure == Error {
            let buildBox = UncheckedSendableBox(build)

            self.init { subscription in
                let stream = AsyncThrowingStream(
                    Output.self,
                    bufferingPolicy: bufferingPolicy,
                    buildBox.value
                )

                await Self.run(stream, on: subscription)
            }
        }

        public func receive<Subscriber: Combine.Subscriber>(
            subscriber: Subscriber
        ) where Subscriber.Input == Output, Subscriber.Failure == Failure {
            let subscription = AsyncSubscription(
                downstream: AnySubscriber(subscriber),
                operation: operation,
                onCancel: onCancel
            )

            subscriber.receive(subscription: subscription)
            subscription.start()
        }

        private static func run<Sequence: AsyncSequence>(
            _ sequence: Sequence,
            on subscription: AsyncSubscription<Output, Failure>
        ) async where Sequence.Element == Output, Sequence.Failure == Failure {
            var iterator = sequence.makeAsyncIterator()

            while await subscription.reserveDemand() {
                do {
                    guard let value = try await iterator.next() else {
                        await subscription.finish(.finished)
                        return
                    }

                    guard await subscription.deliverReserved(value) else {
                        return
                    }
                } catch {
                    await subscription.finish(.failure(error as! Failure))
                    return
                }
            }
        }
    }
}

extension Publisher {
    /// Attaches an async subscriber that awaits each value before requesting the next one.
    ///
    /// Completion is delivered after any in-flight value handler finishes. Cancelling the returned
    /// cancellable cancels both the upstream subscription and the currently running value task.
    public func sink(
        receiveCompletion: @escaping (Subscribers.Completion<Failure>) async -> Void,
        receiveValue: @escaping (Output) async -> Void
    ) -> AnyCancellable {
        let subscriber = AsyncSink(
            receiveCompletion: receiveCompletion,
            receiveValue: receiveValue
        )

        subscribe(subscriber)

        return AnyCancellable(subscriber)
    }
}

extension Publisher where Failure == Never {
    /// Attaches an async subscriber that awaits each value before requesting the next one.
    ///
    /// Cancelling the returned cancellable cancels both the upstream subscription and the currently
    /// running value task.
    public func sink(
        receiveValue: @escaping (Output) async -> Void
    ) -> AnyCancellable {
        sink(
            receiveCompletion: { _ in },
            receiveValue: receiveValue
        )
    }
}

private final class AsyncSubscription<Output, Failure: Error>: Subscription, @unchecked Sendable {
    private let lock = NSLock()
    private var downstream: AnySubscriber<Output, Failure>?
    private var task: Task<Void, Never>?
    private var pendingDemand: Subscribers.Demand = .none
    private var demandWaiters: [CheckedContinuation<Bool, Never>] = []
    private var isTerminated = false

    private let operation: (AsyncSubscription<Output, Failure>) async -> Void
    private let onCancel: () -> Void

    init(
        downstream: AnySubscriber<Output, Failure>,
        operation: @escaping (AsyncSubscription<Output, Failure>) async -> Void,
        onCancel: @escaping () -> Void
    ) {
        self.downstream = downstream
        self.operation = operation
        self.onCancel = onCancel
    }

    func start() {
        lock.lock()

        guard task == nil, !isTerminated else {
            lock.unlock()
            return
        }

        let operation = operation
        task = Task {
            await operation(self)
        }

        lock.unlock()
    }

    func request(_ demand: Subscribers.Demand) {
        guard demand > .none else {
            return
        }

        let waiters: [CheckedContinuation<Bool, Never>]

        lock.lock()

        if isTerminated {
            lock.unlock()
            return
        }

        pendingDemand += demand

        var resumedWaiters: [CheckedContinuation<Bool, Never>] = []

        while pendingDemand > .none, !demandWaiters.isEmpty {
            pendingDemand -= .max(1)
            resumedWaiters.append(demandWaiters.removeFirst())
        }

        waiters = resumedWaiters
        lock.unlock()

        waiters.forEach { $0.resume(returning: true) }
    }

    func cancel() {
        terminate(completion: nil, shouldCancelTask: true)
    }

    func reserveDemand() async -> Bool {
        await withCheckedContinuation { continuation in
            var immediateResult: Bool?

            lock.lock()

            if isTerminated || downstream == nil {
                immediateResult = false
            } else if pendingDemand > .none {
                pendingDemand -= .max(1)
                immediateResult = true
            } else {
                demandWaiters.append(continuation)
            }

            lock.unlock()

            if let immediateResult {
                continuation.resume(returning: immediateResult)
            }
        }
    }

    func deliverReserved(_ value: Output) async -> Bool {
        guard let subscriber = reservedSubscriber() else {
            return false
        }

        let additionalDemand = subscriber.receive(value)

        if additionalDemand > .none {
            request(additionalDemand)
        }

        return isActive
    }

    private func reservedSubscriber() -> AnySubscriber<Output, Failure>? {
        lock.lock()
        defer {
            lock.unlock()
        }

        guard !isTerminated, let downstream else {
            return nil
        }

        return downstream
    }

    private var isActive: Bool {
        lock.lock()
        let isActive = !isTerminated && downstream != nil
        lock.unlock()

        return isActive
    }

    func finish(_ completion: Subscribers.Completion<Failure>) async {
        terminate(completion: completion, shouldCancelTask: false)
    }

    private func terminate(
        completion: Subscribers.Completion<Failure>?,
        shouldCancelTask: Bool
    ) {
        let subscriber: AnySubscriber<Output, Failure>?
        let taskToCancel: Task<Void, Never>?
        let waiters: [CheckedContinuation<Bool, Never>]

        lock.lock()

        guard !isTerminated else {
            lock.unlock()
            return
        }

        isTerminated = true
        subscriber = downstream
        downstream = nil
        taskToCancel = task
        task = nil
        waiters = demandWaiters
        demandWaiters.removeAll()

        lock.unlock()

        waiters.forEach { $0.resume(returning: false) }

        if shouldCancelTask {
            onCancel()
            taskToCancel?.cancel()
        }

        if let completion, let subscriber {
            subscriber.receive(completion: completion)
        }
    }
}

private final class AsyncSink<Input, Failure: Error>: Subscriber, Cancellable, @unchecked Sendable {
    private let lock = NSLock()
    private let receiveCompletion: (Subscribers.Completion<Failure>) async -> Void
    private let receiveValue: (Input) async -> Void

    private var subscription: Subscription?
    private var valueTask: Task<Void, Never>?
    private var completionTask: Task<Void, Never>?
    private var pendingCompletion: Subscribers.Completion<Failure>?
    private var isTerminated = false

    init(
        receiveCompletion: @escaping (Subscribers.Completion<Failure>) async -> Void,
        receiveValue: @escaping (Input) async -> Void
    ) {
        self.receiveCompletion = receiveCompletion
        self.receiveValue = receiveValue
    }

    func receive(subscription: Subscription) {
        lock.lock()

        guard self.subscription == nil, !isTerminated else {
            lock.unlock()
            subscription.cancel()
            return
        }

        self.subscription = subscription
        lock.unlock()

        subscription.request(.max(1))
    }

    func receive(_ input: Input) -> Subscribers.Demand {
        let task: Task<Void, Never>

        lock.lock()

        guard !isTerminated else {
            lock.unlock()
            return .none
        }

        task = Task { [weak self] in
            guard let self else {
                return
            }

            await receiveValue(input)
            await valueTaskDidFinish()
        }

        valueTask = task
        lock.unlock()

        return .none
    }

    func receive(completion: Subscribers.Completion<Failure>) {
        let shouldStartCompletion: Bool

        lock.lock()

        guard !isTerminated else {
            lock.unlock()
            return
        }

        subscription = nil

        if valueTask == nil {
            isTerminated = true
            shouldStartCompletion = true
        } else {
            pendingCompletion = completion
            shouldStartCompletion = false
        }

        lock.unlock()

        if shouldStartCompletion {
            startCompletionTask(completion)
        }
    }

    func cancel() {
        let subscription: Subscription?
        let valueTask: Task<Void, Never>?
        let completionTask: Task<Void, Never>?

        lock.lock()

        guard !isTerminated else {
            lock.unlock()
            return
        }

        isTerminated = true
        subscription = self.subscription
        self.subscription = nil
        valueTask = self.valueTask
        self.valueTask = nil
        completionTask = self.completionTask
        self.completionTask = nil
        pendingCompletion = nil

        lock.unlock()

        subscription?.cancel()
        valueTask?.cancel()
        completionTask?.cancel()
    }

    private func valueTaskDidFinish() async {
        let (subscription, completion) = finishValueTask()

        if let completion {
            await receiveCompletion(completion)
        } else {
            subscription?.request(.max(1))
        }
    }

    private func finishValueTask() -> (Subscription?, Subscribers.Completion<Failure>?) {
        let subscription: Subscription?
        let completion: Subscribers.Completion<Failure>?

        lock.lock()

        valueTask = nil

        if isTerminated {
            lock.unlock()
            return (nil, nil)
        }

        if let pendingCompletion {
            isTerminated = true
            self.pendingCompletion = nil
            self.subscription = nil
            subscription = nil
            completion = pendingCompletion
        } else {
            subscription = self.subscription
            completion = nil
        }

        lock.unlock()

        return (subscription, completion)
    }

    private func startCompletionTask(_ completion: Subscribers.Completion<Failure>) {
        let task = Task { [receiveCompletion] in
            await receiveCompletion(completion)
        }

        lock.lock()
        completionTask = task
        lock.unlock()
    }
}

private final class UncheckedSendableBox<Value>: @unchecked Sendable {
    let value: Value

    init(_ value: Value) {
        self.value = value
    }
}
