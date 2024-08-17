import DequeModule

@usableFromInline
internal final class MessageQueue {
    @usableFromInline
    struct Pusher {
        @usableFromInline
        let id: Int
        @usableFromInline
        let timeout: ContinuousClock.Instant?
        @usableFromInline
        let completion: (PushError?) -> Void

        @inlinable
        init(id: Int, timeout: Duration?, completion: @escaping (PushError?) -> Void) {
            self.id = id
            self.timeout = timeout == nil ? nil : .now + timeout!
            self.completion = completion
        }
    }

    @usableFromInline
    struct Popper {
        @usableFromInline
        let id: Int
        @usableFromInline
        let timeout: ContinuousClock.Instant?
        @usableFromInline
        let completion: ([UInt8], PopError?) -> Void

        @inlinable
        init(id: Int, timeout: Duration?, completion: @escaping ([UInt8], PopError?) -> Void) {
            self.id = id
            self.timeout = timeout == nil ? nil : .now + timeout!
            self.completion = completion
        }
    }

    @usableFromInline
    internal var buffer: Deque<(pusher: Pusher?, data: [UInt8])> = Deque()

    @usableFromInline
    internal var poppers: Deque<Popper> = Deque()

    public let name: String
    public let maxBytes: Int

    @usableFromInline
    internal var currentBytes: Int = 0

    public init(name: String, maxBytes: Int = 1 << 22) {
        self.name = name
        self.maxBytes = maxBytes
    }

    public func push(_ data: [UInt8], timeout: Duration?, id: Int, completion: @escaping (_ error: PushError?) -> Void) {
        guard currentBytes + data.count <= maxBytes else {
            completion(.queueFull)
            return
        }
        if let popper = poppers.popFirst() {
            popper.completion(data, nil)
            completion(nil)
            return
        }
        currentBytes += data.count
        let pusher = Pusher(id: id, timeout: timeout, completion: completion)
        buffer.append((pusher, data))
    }

    public func tryPush(_ data: [UInt8]) -> Bool {
        guard currentBytes + data.count <= maxBytes else { return false }
        if let popper = poppers.popFirst() {
            popper.completion(data, nil)
        } else {
            currentBytes += data.count
            buffer.append((nil, data))
        }
        return true
    }

    public func pop(timeout: Duration?, id: Int, completion: @escaping (_ data: [UInt8], _ error: PopError?) -> Void) {
        if let data = tryPop() { 
            completion(data, nil) 
            return
        }
        let popper = Popper(id: id, timeout: timeout, completion: completion)
        poppers.append(popper)
    }

    public func tryPop() -> [UInt8]? {
        guard let (pusher, data) = buffer.popFirst() else {
            return nil
        }
        currentBytes -= data.count
        if let pusher {
            pusher.completion(nil)
        }
        return data
    }

    public func pruneTimedout() {
        poppers.removeAll { popper in 
            if let timeout = popper.timeout, timeout <= .now {
                popper.completion([], .timeout)
                return true
            }
            return false
        }
        buffer.removeAll { (pusher, data) in 
            guard let pusher else { return false }
            if let timeout = pusher.timeout, timeout <= .now {
                pusher.completion(.timeout)
                currentBytes -= data.count
                return true
            }
            return false
        }
    }
}