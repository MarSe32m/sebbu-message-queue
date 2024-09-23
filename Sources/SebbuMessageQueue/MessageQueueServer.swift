import SebbuNetworking
import Synchronization

public final class MessageQueueServer {
    @usableFromInline
    internal var messageQueues: [String: MessageQueue] = [:]

    private let username: [UInt8]
    private let password: [UInt8]

    @usableFromInline
    internal let server: TCPServerChannel

    @usableFromInline
    internal var clients: [Client] = []

    public init(host: String, port: Int, username: String, password: String) throws {
        self.username = [UInt8](username.utf8)
        self.password = [UInt8](password.utf8)
        guard let address = IPAddress(host: host, port: port) else {
            fatalError("Invalid ip address: \(host):\(port)")
        }
        server = TCPServerChannel(loop: .default)
        try server.bind(address: address)
        try server.listen(backlog: 16)
    }

    public func run() {
        server.eventLoop.schedule(timeout: .seconds(1), repeating: .seconds(1)) { shouldStop in 
            for val in self.messageQueues.values {
                val.pruneTimedout()
            }
        }
        var clientsToRemove: [Client] = []
        while true {
            server.eventLoop.run(.once)
            while let client = server.receive() {
                clients.append(Client(client))
            }
            for client in clients {
                messageLoop: while true {
                    let message: MessageQueuePacket?
                    do {
                        message = try client.receive()
                    } catch {
                        clientsToRemove.append(client)
                        break messageLoop
                    }
                    guard let message else { break messageLoop }
                    if !client.authorized {
                        guard case .connectionRequest(let request) = message,
                            request.username == username && request.password == password else { 
                            clientsToRemove.append(client)
                            break messageLoop
                        }
                        client.authorized = true
                        do {
                            try client.send(.connectionResponse(.init(failure: nil)))
                        } catch {
                            clientsToRemove.append(client)
                        }
                    }
                    switch message {
                        case .connectionRequest(_): break // Handled above
                        case .connectionResponse(_): break // Client bound
                        case .disconnect: 
                            clientsToRemove.append(client)
                            break messageLoop
                        case .push(let request): 
                            let queue = getQueue(name: request.queue)
                            queue.push(request.payload, timeout: request.timeout == 0 ? nil : .milliseconds(request.timeout), id: client.id) { error in 
                                do {
                                    try client.send(.pushResponse(.init(id: request.id, failure: error)))
                                } catch {
                                    clientsToRemove.append(client)
                                }
                            }
                        case .tryPush(let request): 
                            let queue = getQueue(name: request.queue)
                            let pushed = queue.tryPush(request.payload)
                            do {
                                try client.send(.pushResponse(.init(id: request.id, failure: pushed ? nil : .queueFull)))
                            } catch {
                                clientsToRemove.append(client)
                            }
                        case .pushResponse(_): break // Client bound
                        case .pop(let request): 
                            let queue = getQueue(name: request.queue)
                            queue.pop(timeout: request.timeout == 0 ? nil : .milliseconds(request.timeout), id: client.id) { data, error in 
                                do {
                                    try client.send(.popResponse(.init(id: request.id, failure: error, payload: data)))
                                } catch {
                                    clientsToRemove.append(client)
                                }
                            }

                        case .tryPop(let request): 
                            let queue = getQueue(name: request.queue)
                            do {
                                if let data = queue.tryPop() {
                                    try client.send(.popResponse(.init(id: request.id, failure: nil, payload: data)))
                                } else {
                                    try client.send(.popResponse(.init(id: request.id, failure: .queueEmpty, payload: [])))
                                }
                            } catch {
                                clientsToRemove.append(client)
                            }
                        case .popResponse(_): break // Client bound
                    }
                }
            }
            clients.removeAll { client in
                clientsToRemove.contains {
                    client.id == $0.id
                }
            }
            clientsToRemove.removeAll(keepingCapacity: true)
        }
    }
    
    private func getQueue(name: String) -> MessageQueue {
        if let queue = messageQueues[name] { return queue }
        //TODO: Take maxBytes as a parameter and pass it to the MessageQueue
        let newQueue = MessageQueue(name: name)
        messageQueues[name] = newQueue
        return newQueue
    }

}

internal extension MessageQueueServer {
    @usableFromInline
    final class Client {
        @usableFromInline
        static let currentID: Atomic<Int> = Atomic(0)

        @usableFromInline
        let reader: PacketReader

        @usableFromInline
        let writer: PacketWriter

        @usableFromInline
        let clientChannel: TCPClientChannel

        @usableFromInline
        var authorized: Bool = false

        @usableFromInline
        let id: Int

        @inlinable
        init(_ client: TCPClientChannel) {
            self.clientChannel = client
            self.reader = PacketReader(client: client)
            self.writer = PacketWriter(client: client)
            self.id = Client.currentID.wrappingAdd(1, ordering: .relaxed).newValue
        }

        func receive() throws -> MessageQueuePacket? {
            while let data = clientChannel.receive() { 
                reader.append(data)
            }
            return try reader.read()
        }

        func send(_ packet: MessageQueuePacket) throws {
            try writer.send(packet)
        }
    }
}