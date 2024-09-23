// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "sebbu-message-queue",
    platforms: [.macOS(.v13)],
    products: [
        // Products define the executables and libraries a package produces, making them visible to other packages.
        .library(
            name: "SebbuMessageQueue",
            targets: ["SebbuMessageQueue"]),
    ],
    dependencies: [.package(url: "https://github.com/MarSe32m/sebbu-networking.git", branch: "main"),
                   .package(url: "https://github.com/apple/swift-collections.git", branch: "main")],
    targets: [
        // Targets are the basic building blocks of a package, defining a module or a test suite.
        // Targets can depend on other targets in this package and products from dependencies.
        .target(name: "SebbuMessageQueue",
                dependencies: [.product(name: "SebbuNetworking", package: "sebbu-networking"),
                               .product(name: "DequeModule", package: "swift-collections")]),
        .executableTarget(name: "Development",
                            dependencies: ["SebbuMessageQueue"]
        )       
    ]
)
