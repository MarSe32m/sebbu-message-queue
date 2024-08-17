import SebbuNetworking
import DequeModule

@usableFromInline
internal final class PacketWriter {
    @usableFromInline
    internal let client: TCPClientChannel

    @usableFromInline
    internal var writeBuffer: ByteBuffer = ByteBuffer()

    @inlinable
    internal init(client: TCPClientChannel) {
        self.client = client
    }

    @inline(__always)
    @inlinable
    internal func flush(_ header: [UInt8]) throws {
        try client.send(header)
        for buffer in writeBuffer.buffers {
            try client.send(buffer)
        }
        writeBuffer.buffers.removeAll(keepingCapacity: true)
    }

    @usableFromInline
    var _header: [UInt8] = [0, 0, 0, 0, 0]

    @inline(__always)
    @inlinable
    internal func send(_ packet: MessageQueuePacket) throws {
        let writtenBytes = UInt32(packet.write(&writeBuffer))
        _header[0] = packet.type.rawValue
        withUnsafeBytes(of: writtenBytes) { ptr in 
            for i in 0..<4 {
                _header[i + 1] = ptr[i]
            }
        }
        try flush(_header)
    }
}