import Combine

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
