import Combine
import Foundation
import Testing
@testable import AsyncPublisher

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
