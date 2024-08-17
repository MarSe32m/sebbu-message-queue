import SebbuNetworking
import DequeModule

@usableFromInline
final class PacketReader {
    @usableFromInline
    enum DecodeState {
        case waitingForHeader
        case header(type: MessageQueuePacketType, length: Int)
    }

    @usableFromInline
    internal var state: DecodeState = .waitingForHeader

    @usableFromInline
    internal let client: TCPClientChannel

    @usableFromInline
    internal var buffer: ByteBuffer = ByteBuffer()

    @inlinable
    init(client: TCPClientChannel) {
        self.client = client
    }

    @inline(__always)
    func append(_ data: [UInt8]) {
        buffer.append(data)
    }

    @inline(__always)
    func read() throws -> MessageQueuePacket? {
        if case .waitingForHeader = state {
            if let (type, length) = readHeader() {
                self.state = .header(type: type, length: length)
            } else {
                return nil
            }
        }
        guard case let .header(type, length) = state else { fatalError("unreachable") }
        // Need more data
        guard buffer.count >= length else { return nil }
        defer { state = .waitingForHeader }
        return try MessageQueuePacket.read(&buffer, type: type)
    }

    @inline(__always)
    func readHeader() -> (type: MessageQueuePacketType, length: Int)? {
        guard let bytes = buffer.read(5) else { return nil }
        //TODO: This is a bad case, close the connection perhaps?
        guard let type = MessageQueuePacketType(rawValue: bytes[0]) else { fatalError("TODO: Close the connection") }
        guard let length = decodeUInt32(bytes[1...]) else { fatalError("TODO: Close the connection") }
        return (type, Int(length))
    }

    @inline(__always)
    func readUInt8() -> UInt8? {
        buffer.read(1)?[0]    
    }

    @inline(__always)
    func readUInt16() -> UInt16? {
        guard let bytes = buffer.read(2) else { return nil }
        return bytes.withUnsafeBytes { ptr in 
            ptr.loadUnaligned(as: UInt16.self)
        }        
    }

    @inline(__always)
    func readUInt32() -> UInt32? {
        guard let bytes = buffer.read(4) else { return nil }
        return bytes.withUnsafeBytes { ptr in 
            ptr.loadUnaligned(as: UInt32.self)
        }        
    }

    @inline(__always)
    func readUInt64() -> UInt64? {
        guard let bytes = buffer.read(8) else { return nil }
        return bytes.withUnsafeBytes { ptr in 
            ptr.loadUnaligned(as: UInt64.self)
        }        
    }

    @inline(__always)
    func readBytes(_ count: Int) -> [UInt8]? {
        buffer.read(count)
    }

    @inline(__always)
    func decodeUInt32(_ bytes: ArraySlice<UInt8>) -> UInt32? {
        if bytes.count < 4 { return nil }
        return bytes.withUnsafeBytes { ptr in 
            ptr.loadUnaligned(as: UInt32.self)
        }
    }
}

