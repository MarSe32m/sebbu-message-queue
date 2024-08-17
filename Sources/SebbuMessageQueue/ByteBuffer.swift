import DequeModule

@usableFromInline
enum ByteCount: Int {
    case one = 1
    case two = 2
    case four = 4
    case eight = 8
}

@usableFromInline
struct ByteBuffer {
    @usableFromInline
    var buffers: Deque<[UInt8]> = Deque()

    @usableFromInline
    var count: Int = 0

    @usableFromInline
    var returnBuffer: [UInt8] = []

    @usableFromInline
    let returnBufferCapacity: Int

    @usableFromInline
    var indexInCurrentBuffer: Int = 0

    @inlinable
    init(returnBufferCapacity: Int = 16 * 1024) {
        self.returnBufferCapacity = returnBufferCapacity
    }

    @inlinable
    @inline(__always)
    @discardableResult
    public mutating func append<T: FixedWidthInteger>(_ integer: T) -> Int {
        withUnsafeBytes(of: integer) { append(.init($0)) }
    }

    @inlinable
    @inline(__always)
    @discardableResult
    public mutating func append<T: RawRepresentable>(_ value: T) -> Int where T.RawValue == UInt8 {
        append(value.rawValue)
    }

    @inlinable
    @inline(__always)
    @discardableResult
    public mutating func append<T: RawRepresentable>(_ value: T?) -> Int where T.RawValue == UInt8 {
        var writtenBytes = 0
        if let value {
            writtenBytes += append(1 as UInt8)
            writtenBytes += append(value.rawValue)
        } else {
            writtenBytes += append(0 as UInt8)
            writtenBytes += append(0 as UInt8)
        }
        return writtenBytes
    }

    @inlinable
    @inline(__always)
    @discardableResult
    public mutating func appendBytes(_ bytes: [UInt8], lengthByteCount: ByteCount) -> Int {
        var writtenCount = 0
        switch lengthByteCount {
            case .one:
                assert(bytes.count <= Int(Int8.max))
                let count = Int8(bytes.count)
                writtenCount += append(count)
            case .two:
                assert(bytes.count <= Int(Int16.max))
                let count = Int16(bytes.count)
                writtenCount += append(count)
            case .four:
                assert(bytes.count <= Int(Int32.max))
                let count = Int32(bytes.count)
                writtenCount += append(count)
            case .eight:
                assert(bytes.count <= Int(Int64.max))
                let count = Int64(bytes.count)
                writtenCount += append(count)
        }
        writtenCount += append(bytes)
        return writtenCount
    }

    @inlinable
    @inline(__always)
    @discardableResult
    public mutating func append(_ string: String) -> Int {
        let stringBytes = [UInt8](string.utf8)
        return appendBytes(stringBytes, lengthByteCount: .four)
    }

    @inlinable
    @inline(__always)
    @discardableResult
    public mutating func append(_ duration: Duration) -> Int {
        withUnsafeBytes(of: duration) { ptr in 
            append(.init(ptr))
        }
    }

    @inlinable
    @inline(__always)
    @discardableResult
    public mutating func append(_ buffer: [UInt8]) -> Int {
        if buffer.isEmpty { return 0 }
        count += buffer.count
        if buffers.isEmpty { 
            buffers.append(buffer)
        } else if buffers[buffers.count - 1].count + buffer.count <= 32 { 
            buffers[buffers.count - 1].append(contentsOf: buffer) 
        } else {
            buffers.append(buffer)
        }
        return buffer.count
    }

    @inlinable
    @inline(__always)
    public mutating func read(_ bytes: Int) -> [UInt8]? {
        if bytes > count { return nil }
        defer { count -= bytes}
        returnBuffer.removeAll(keepingCapacity: returnBuffer.capacity <= returnBufferCapacity)
        while returnBuffer.count < bytes {
            let diff = bytes - returnBuffer.count
            let buf = buffers.removeFirst()
            // Very fast path
            if diff == bytes && buf.count == bytes && indexInCurrentBuffer == 0 {
                return buf
            }
            if buf.count - indexInCurrentBuffer > diff {
                returnBuffer.append(contentsOf: buf[indexInCurrentBuffer..<indexInCurrentBuffer + diff])
                indexInCurrentBuffer += diff
                buffers.prepend(buf)
            } else {
                returnBuffer.append(contentsOf: buf[indexInCurrentBuffer...])
                indexInCurrentBuffer = 0
            }
        }
        assert(returnBuffer.count == bytes)
        return returnBuffer
    }

    @inlinable
    @inline(__always)
    public mutating func read<T: RawRepresentable>() -> T? where T.RawValue == UInt8 {
        guard let byte = read() as UInt8? else { return nil }
        return T.init(rawValue: byte)
    }

    @inlinable
    @inline(__always)
    public mutating func readOptional<T: RawRepresentable>() -> T? where T.RawValue == UInt8 {
        guard let isPresent = read() as UInt8? else { return nil }
        guard let byte = read() as UInt8? else { return nil }
        return isPresent == 0 ? nil : .init(rawValue: byte)
    }

    @inlinable
    @inline(__always)
    public mutating func read<T: FixedWidthInteger>() -> T? {
        guard let bytes = read(T.bitWidth / 8) else { return nil }
        return bytes.withUnsafeBytes { ptr in 
            ptr.loadUnaligned(as: T.self)
        }
    }

    @inlinable
    @inline(__always)
    @discardableResult
    public mutating func readBytes(lengthByteCount: ByteCount) ->[UInt8]? {
        var count: Int?
        switch lengthByteCount {
            case .one:
                guard let _count = read() as Int8? else { return nil }
                count = Int(_count)
            case .two:
                guard let _count = read() as Int16? else { return nil }
                count = Int(_count)
            case .four:
                guard let _count = read() as Int32? else { return nil }
                count = Int(_count)
            case .eight:
                guard let _count = read() as Int64? else { return nil }
                count = Int(_count)
        }
        guard let count else { return nil }
        guard let bytes = read(count) else { return nil }
        return bytes
    }
    
    @inlinable
    @inline(__always)
    @discardableResult
    public mutating func read() -> String? {
        guard let stringBytes = readBytes(lengthByteCount: .four) else { return nil }
        return String(decoding: stringBytes, as: UTF8.self)
    }

    @inlinable
    @inline(__always)
    @discardableResult
    public mutating func read() -> Duration? {
        guard let bytes = read(MemoryLayout<Duration>.size) else { return nil }
        return bytes.withUnsafeBytes { 
            $0.loadUnaligned(as: Duration.self)
        }
    }
}