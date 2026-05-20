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
