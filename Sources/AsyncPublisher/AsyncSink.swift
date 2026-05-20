import Combine
import Foundation

final class AsyncSink<Input, Failure: Error>: Subscriber, Cancellable, @unchecked Sendable {
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
