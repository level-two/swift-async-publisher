// swift-tools-version: 6.3
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "AsyncPublisher",
    platforms: [
        .macOS(.v15),
        .iOS(.v18),
        .tvOS(.v18),
        .watchOS(.v11),
    ],
    products: [
        .library(
            name: "AsyncPublisher",
            targets: ["AsyncPublisher"]
        ),
    ],
    targets: [
        .target(
            name: "AsyncPublisher"
        ),
        .testTarget(
            name: "AsyncPublisherTests",
            dependencies: ["AsyncPublisher"]
        ),
    ],
    swiftLanguageModes: [.v5]
)
