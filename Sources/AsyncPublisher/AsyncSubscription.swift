import Combine
import Foundation

final class AsyncSubscription<Output, Failure: Error>: Subscription, @unchecked Sendable {
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
