import SebbuNetworking
import Atomics

public final class AsyncMessageQueueClient: @unchecked Sendable {
    @usableFromInline
    internal let client: MessageQueueClient

    @usableFromInline
    internal var loop: EventLoop {
        client.client.eventLoop
    }

    @usableFromInline
    let running: ManagedAtomic<Bool>

    @inlinable
    internal init(client: MessageQueueClient) {
        self.client = client
        let running = ManagedAtomic<Bool>(true)
        self.running = running
        _ = Thread {
            while running.load(ordering: .relaxed) { 
                do {
                    try client.update(wait: true) 
                } catch {
                    running.store(false, ordering: .relaxed)
                }
            }
        }
    }

    public static func connect(host: String, port: Int, username: String, password: String) async throws -> AsyncMessageQueueClient {
        guard let address = IPAddress(host: host, port: port) else { fatalError("Invalid address: \(host):\(port)") }
        let loop = EventLoop()
        let client = TCPClientChannel(loop: loop)
        try client.connect(remoteAddress: address, nodelay: false, keepAlive: 60)
        return try await withCheckedThrowingContinuation { continuation in
            _ = Thread {
                let startTime = ContinuousClock.now
                while client.state != .connected && client.state != .closed && .now - startTime < .seconds(10) {
                    loop.run(.nowait)
                }
                do {
                    let syncClient = try MessageQueueClient(client: client, username: username, password: password)
                    let asyncClient = AsyncMessageQueueClient(client: syncClient)
                    continuation.resume(returning: asyncClient)
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    public func push(queue: String, _ data: [UInt8], timeout: Duration? = nil) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in 
            loop.execute {
                self.client.push(queue: queue, data, timeout: timeout) { error in 
                    if let error {
                        continuation.resume(throwing: error)
                    } else {
                        continuation.resume()
                    }
                }
            }
        }
    }

    public func tryPush(queue: String, _ data: [UInt8]) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in 
            loop.execute {
                self.client.tryPush(queue: queue, data) { error in 
                    if let error {
                        continuation.resume(throwing: error)
                    } else {
                        continuation.resume()
                    }
                }
            }
        }
    }

    public func pop(queue: String, timeout: Duration? = nil) async throws -> [UInt8] {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<[UInt8], Error>) in 
            loop.execute {
                self.client.pop(queue: queue, timeout: timeout) { data, error in 
                    if let error {
                        continuation.resume(throwing: error)
                    } else {
                        continuation.resume(returning: data)
                    }
                }
            }
        }
    }

    public func tryPop(queue: String) async throws -> [UInt8] {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<[UInt8], Error>) in 
            loop.execute {
                self.client.tryPop(queue: queue) { data, error in 
                    if let error {
                        continuation.resume(throwing: error)
                    } else {
                        continuation.resume(returning: data)
                    }
                }
            }
        }
    }

    deinit {
        running.store(false, ordering: .relaxed)
        client.loop.notify()
    }
}