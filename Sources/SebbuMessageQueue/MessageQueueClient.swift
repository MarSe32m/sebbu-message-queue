import SebbuNetworking

public final class MessageQueueClient {
    @usableFromInline
    internal var pushRequests: [Int: (PushError?) -> Void] = [:]

    @usableFromInline
    internal var popRequests: [Int: ([UInt8], PopError?) -> Void] = [:]

    @usableFromInline
    internal let client: TCPClientChannel

    @usableFromInline
    internal let reader: PacketReader

    @usableFromInline
    internal let writer: PacketWriter

    @usableFromInline
    internal let loop: EventLoop

    @usableFromInline
    internal var currentID: Int = 0

    @usableFromInline
    internal var authorized: Bool = false

    @usableFromInline
    internal var disconnected: Bool = false

    @inlinable
    internal init(client: TCPClientChannel, username: String, password: String) throws {
        self.client = client
        self.reader = PacketReader(client: client)
        self.writer = PacketWriter(client: client)
        self.loop = client.eventLoop
        let request = MessageQueuePacket.connectionRequest(.init(username: [UInt8](username.utf8), password: [UInt8](password.utf8)))
        do {
            try writer.send(request)
        } catch {
            throw ConnectionError.unknownError
        }
        client.onClose { [weak self] in
            guard let self = self else { return }
            self.onClientClose()
        }
    }

    public static func connect(host: String, port: Int, username: String, password: String) throws -> MessageQueueClient {
        guard let address = IPAddress(host: host, port: port) else { fatalError("Invalid address: \(host):\(port)") }
        let loop = EventLoop()
        let client = TCPClientChannel(loop: loop)
        try client.connect(remoteAddress: address, nodelay: false, keepAlive: 60)
        let startTime = ContinuousClock.now
        while client.state != .connected && client.state != .closed && .now - startTime < .seconds(10) {
            loop.run(.nowait)
        }
        return try MessageQueueClient(client: client, username: username, password: password)
    }

    public func update(wait: Bool = false) throws {
        if disconnected { return }
        loop.run(wait ? .once : .nowait)
        while let data = client.receive() {
            reader.append(data)
        }
        while let message = try reader.read() {
            switch message {
                 case .connectionRequest(_): break
                 case .connectionResponse(let response):
                    if authorized { break }
                    if response.success { authorized = true }
                    else {  
                        throw response.failure!
                    }
                case .disconnect: 
                    disconnected = true
                case .push(_): break
                case .tryPush(_): break
                case .pushResponse(let response):
                    guard let pushCompletion = pushRequests.removeValue(forKey: response.id) else { break }
                    pushCompletion(response.failure)
                case .pop(_): break
                case .tryPop(_): break
                case .popResponse(let response):
                    guard let popCompletion = popRequests.removeValue(forKey: response.id) else { break }
                    popCompletion(response.payload, response.failure)
            }
        }
    }

    public func push(queue: String, _ data: [UInt8],  timeout: Duration? = nil, completion: @escaping (PushError?) -> Void) {
        assert(loop.inEventLoop)
        if client.state == .closed { completion(.connectionClosed) }
        let id = getID()
        let request = MessageQueuePacket.push(.init(queue: queue, id: id, payload: data, timeout: UInt64((timeout ?? .seconds(0)) / .milliseconds(1))))
        do {
            try writer.send(request)
            pushRequests[id] = completion
        } catch {
            completion(.connectionClosed)
        }
    }

    public func tryPush(queue: String, _ data: [UInt8], completion: @escaping (PushError?) -> Void) {
        assert(loop.inEventLoop)
        if client.state == .closed { completion(.connectionClosed) }
        let id = getID()
        let request  = MessageQueuePacket.tryPush(.init(queue: queue, id: id, payload: data))
        do {
            try writer.send(request)
            pushRequests[id] = completion
        } catch {
            completion(.connectionClosed)
        }
    }

    public func pop(queue: String, timeout: Duration? = nil, completion: @escaping ([UInt8], PopError?) -> Void) {
        assert(loop.inEventLoop)
        if client.state == .closed { completion([], .connectionClosed) }
        let id = getID()
        let request = MessageQueuePacket.pop(.init(queue: queue, id: id, timeout: UInt64((timeout ?? .seconds(0)) / .milliseconds(1))))
        do {
            try writer.send(request)
            popRequests[id] = completion
        } catch {
            completion([], .connectionClosed)
        }
    }

    public func tryPop(queue: String, completion: @escaping ([UInt8], PopError?) -> Void) {
        assert(loop.inEventLoop)
        if client.state == .closed { completion([], .connectionClosed) }
        let id = getID()
        let request = MessageQueuePacket.tryPop(.init(queue: queue, id: id))
        do { 
            try writer.send(request)
            popRequests[id] = completion
        } catch {
            completion([], .connectionClosed)
        }
    }

    @inlinable
    internal func getID() -> Int {
        currentID += 1
        return currentID
    }

    @inlinable
    internal func onClientClose() {
        for (_, completion) in pushRequests {
            completion(.connectionClosed)
        }
        pushRequests.removeAll()
        for (_, completion) in popRequests {
            completion([], .connectionClosed)
        }
        popRequests.removeAll()
    }

    deinit {
        onClientClose()
    }
}