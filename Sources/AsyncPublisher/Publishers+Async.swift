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
