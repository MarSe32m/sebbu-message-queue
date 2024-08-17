
@usableFromInline
internal enum MessageQueuePacketType: UInt8 {
    case connectionRequest
    case connectionResponse
    case disconnect
    case push
    case tryPush
    case pushResponse
    case pop
    case tryPop
    case popResponse
}

@usableFromInline
enum MessageQueuePacketDecodingError: Error {
    case genericError
}

@usableFromInline
protocol MessageQueuePacketCodable {
    func write(_ buffer: inout ByteBuffer) -> Int
    //TODO: Make this throwing and don't return an optional
    static func read(_ buffer: inout ByteBuffer) throws -> Self
}

@usableFromInline
internal enum MessageQueuePacket {
    // Connection / disconnection
    case connectionRequest(ConnectionRequest)
    case connectionResponse(ConnectionResponse)
    case disconnect

    // Push
    case push(PushRequest)
    case tryPush(TryPushRequest)
    case pushResponse(PushResponse)

    // Pop
    case pop(PopRequest)
    case tryPop(TryPopRequest)
    case popResponse(PopResponse)

    @usableFromInline
    var type: MessageQueuePacketType {
        switch self {
            case .connectionRequest: return .connectionRequest
            case .connectionResponse: return .connectionResponse
            case .disconnect: return .disconnect
            case .push: return .push
            case .tryPush: return .tryPush
            case .pushResponse: return .pushResponse
            case .pop: return .pop
            case .tryPop: return .tryPop
            case .popResponse: return .popResponse
        }
    }

    public func write(_ buffer: inout ByteBuffer) -> Int {
        return switch self {
            case .connectionRequest(let request): 
                request.write(&buffer)
            case .connectionResponse(let response): 
                response.write(&buffer)
            case .disconnect:
                0
            case .push(let request): 
                request.write(&buffer)
            case .tryPush(let request): 
                request.write(&buffer)
            case .pushResponse(let response): 
                response.write(&buffer)

            case .pop(let request): 
                request.write(&buffer)
            case .tryPop(let request): 
                request.write(&buffer)
            case .popResponse(let response):
                response.write(&buffer)
        }
    }

    public static func read(_ buffer: inout ByteBuffer, type: MessageQueuePacketType) throws -> MessageQueuePacket {
        // At this point we have read the header already
        // We have also ensured that we have received enough data in the reader buffer
        switch type {
            case .connectionRequest: 
                let request = try ConnectionRequest.read(&buffer)
                return .connectionRequest(request)
            case .connectionResponse: 
                let response = try ConnectionResponse.read(&buffer)
                return .connectionResponse(response)
            case .disconnect: 
                return .disconnect
            case .push: 
                let request = try PushRequest.read(&buffer)
                return .push(request)
            case .tryPush: 
                let request = try TryPushRequest.read(&buffer)
                return .tryPush(request)
            case .pushResponse: 
                let response = try PushResponse.read(&buffer)
                return .pushResponse(response)
            case .pop: 
                let request = try PopRequest.read(&buffer)
                return .pop(request)
            case .tryPop: 
                let request = try TryPopRequest.read(&buffer)
                return .tryPop(request)
            case .popResponse:
                let response = try PopResponse.read(&buffer)
                return .popResponse(response)
        }
    }
}

@usableFromInline
struct ConnectionRequest: MessageQueuePacketCodable {
    @usableFromInline
    let username: [UInt8]
    @usableFromInline
    let password: [UInt8]
    
    public init(username: [UInt8], password: [UInt8]) {
        self.username = username
        self.password = password
    }

    public func write(_ buffer: inout ByteBuffer) -> Int  {
        var writtenBytes = 0
        writtenBytes += buffer.appendBytes(username, lengthByteCount: .two)
        writtenBytes += buffer.appendBytes(password, lengthByteCount: .two)
        return writtenBytes
    }

    public static func read(_ buffer: inout ByteBuffer) throws -> ConnectionRequest {
        guard let username = buffer.readBytes(lengthByteCount: .two) else { throw MessageQueuePacketDecodingError.genericError }
        guard let password = buffer.readBytes(lengthByteCount: .two) else { throw MessageQueuePacketDecodingError.genericError }
        return ConnectionRequest(username: username, password: password)
    }
}

@usableFromInline
enum ConnectionError: UInt8, Error {
    case wrongCredentials
    case unknownError
}

@usableFromInline
struct ConnectionResponse: MessageQueuePacketCodable {
    @usableFromInline
    let success: Bool
    @usableFromInline
    let failure: ConnectionError?

    public init(failure: ConnectionError?) {
        self.success = failure == nil
        self.failure = failure
    }

    @usableFromInline
    func write(_ buffer: inout ByteBuffer) -> Int  {
        buffer.append(failure)
    }

    @usableFromInline
    static func read(_ buffer: inout ByteBuffer) throws -> ConnectionResponse {
        let failure: ConnectionError? = buffer.readOptional()
        return ConnectionResponse(failure: failure)
    }
}

// Push
@usableFromInline
struct PushRequest: MessageQueuePacketCodable {
    @usableFromInline
    let queue: String
    @usableFromInline
    let id: Int
    @usableFromInline
    let payload: [UInt8]
    @usableFromInline
    let timeout: UInt64
    
    public init(queue: String, id: Int, payload: [UInt8], timeout: UInt64) {
        self.queue = queue
        self.id = id
        self.payload = payload
        self.timeout = timeout
    }

    @usableFromInline
    func write(_ buffer: inout ByteBuffer) -> Int  {
        var writtenBytes = 0
        writtenBytes += buffer.append(queue)
        writtenBytes += buffer.append(id)
        writtenBytes += buffer.appendBytes(payload, lengthByteCount: .four)
        writtenBytes += buffer.append(timeout)
        return writtenBytes
    }

    @usableFromInline
    static func read(_ buffer: inout ByteBuffer) throws -> PushRequest {
        guard let queue: String = buffer.read() else { throw MessageQueuePacketDecodingError.genericError }
        guard let id: Int = buffer.read() else { throw MessageQueuePacketDecodingError.genericError }
        guard let payload: [UInt8] = buffer.readBytes(lengthByteCount: .four) else { throw MessageQueuePacketDecodingError.genericError }
        guard let timeout: UInt64 = buffer.read() else { throw MessageQueuePacketDecodingError.genericError }
        return PushRequest(queue: queue, id: Int(id), payload: payload, timeout: timeout)
    }
}

public enum PushError: UInt8, Error {
    case timeout
    case queueFull
    case connectionClosed
}

@usableFromInline
struct PushResponse: MessageQueuePacketCodable {
    @usableFromInline
    let id: Int
    @usableFromInline
    let failure: PushError?

    public init(id: Int, failure: PushError?) {
        self.id = id
        self.failure = failure
    }

    @usableFromInline
    func write(_ buffer: inout ByteBuffer) -> Int  {
        var writtenBytes = 0
        writtenBytes += buffer.append(id)
        writtenBytes += buffer.append(failure)
        return writtenBytes
    }

    @usableFromInline
    static func read(_ buffer: inout ByteBuffer) throws -> PushResponse {
        guard let id: Int = buffer.read() else { throw MessageQueuePacketDecodingError.genericError }
        let failure: PushError? = buffer.readOptional()
        return PushResponse(id: Int(id), failure: failure)
    }
}

@usableFromInline
struct TryPushRequest: MessageQueuePacketCodable {
    @usableFromInline
    let queue: String
    @usableFromInline
    let id: Int
    @usableFromInline
    let payload: [UInt8]

    public init(queue: String, id: Int, payload: [UInt8]) {
        self.queue = queue
        self.id = id
        self.payload = payload
    }

    @usableFromInline
    func write(_ buffer: inout ByteBuffer) -> Int  {
        var writtenBytes = 0
        writtenBytes += buffer.append(queue)
        writtenBytes += buffer.append(id)
        writtenBytes += buffer.appendBytes(payload, lengthByteCount: .four)
        return writtenBytes
    }

    @usableFromInline
    static func read(_ buffer: inout ByteBuffer) throws -> TryPushRequest {
        guard let queue: String = buffer.read() else { throw MessageQueuePacketDecodingError.genericError }
        guard let id: Int = buffer.read() else { throw MessageQueuePacketDecodingError.genericError }
        guard let payload = buffer.readBytes(lengthByteCount: .four) else { throw MessageQueuePacketDecodingError.genericError }
        return TryPushRequest(queue: queue, id: Int(id), payload: payload)
    }
}

// Pop
@usableFromInline
struct PopRequest: MessageQueuePacketCodable {
    @usableFromInline
    let queue: String
    @usableFromInline
    let id: Int
    @usableFromInline
    let timeout: UInt64
    
    public init(queue: String, id: Int, timeout: UInt64) {
        self.queue = queue
        self.id = id
        self.timeout = timeout
    }

    @usableFromInline
    func write(_ buffer: inout ByteBuffer) -> Int  {
        var writtenBytes = 0
        writtenBytes += buffer.append(queue)
        writtenBytes += buffer.append(id)
        writtenBytes += buffer.append(timeout)
        return writtenBytes
    }

    @usableFromInline
    static func read(_ buffer: inout ByteBuffer) throws -> PopRequest {
        guard let queue: String = buffer.read() else { throw MessageQueuePacketDecodingError.genericError }
        guard let id: Int = buffer.read() else { throw MessageQueuePacketDecodingError.genericError }
        guard let timeout: UInt64 = buffer.read() else { throw MessageQueuePacketDecodingError.genericError }
        return PopRequest(queue: queue, id: Int(id), timeout: timeout)
    }
}

public enum PopError: UInt8, Error {
    case timeout
    case queueEmpty
    case connectionClosed
}

@usableFromInline
struct PopResponse: MessageQueuePacketCodable {
    @usableFromInline
    let id: Int
    @usableFromInline
    let failure: PopError?
    @usableFromInline
    let payload: [UInt8]

    public init(id: Int, failure: PopError?, payload: [UInt8]) {
        self.id = id
        self.failure = failure
        self.payload = payload
    }

     @usableFromInline
    func write(_ buffer: inout ByteBuffer) -> Int  {
        var writtenBytes = 0
        writtenBytes += buffer.append(id)
        writtenBytes += buffer.append(failure)
        writtenBytes += buffer.appendBytes(payload, lengthByteCount: .four)
        return writtenBytes
    }

    @usableFromInline
    static func read(_ buffer: inout ByteBuffer) throws -> PopResponse {
        guard let id: Int = buffer.read() else { throw MessageQueuePacketDecodingError.genericError }
        let failure: PopError? = buffer.readOptional()
        guard let payload = buffer.readBytes(lengthByteCount: .four) else { throw MessageQueuePacketDecodingError.genericError }
        return PopResponse(id: Int(id), failure: failure, payload: payload)
    }
}

@usableFromInline
struct TryPopRequest: MessageQueuePacketCodable {
    @usableFromInline
    let queue: String
    @usableFromInline
    let id: Int
    
    public init(queue: String, id: Int) {
        self.queue = queue
        self.id = id
    }

    @usableFromInline
    func write(_ buffer: inout ByteBuffer) -> Int  {
        var writtenBytes = 0
        writtenBytes += buffer.append(queue)
        writtenBytes += buffer.append(id)
        return writtenBytes
    }

    @usableFromInline
    static func read(_ buffer: inout ByteBuffer) throws -> TryPopRequest {
        guard let queue: String = buffer.read() else { throw MessageQueuePacketDecodingError.genericError }
        guard let id: Int = buffer.read() else { throw MessageQueuePacketDecodingError.genericError }
        return TryPopRequest(queue: queue, id: Int(id))
    }
}