import SebbuMessageQueue
import Foundation

Thread.detachNewThread {
    let server = try! MessageQueueServer(host: "0.0.0.0", port: 25565, username: "sebbu", password: "passwrd")
    server.run()
}

Thread.sleep(forTimeInterval: 1)

func testSyncClient() throws {
    let client = try MessageQueueClient.connect(host: "172.19.30.73", port: 25565, username: "sebbu", password: "passwrd")
    let data = (0..<1 * 1024 * 1024).map { _ in UInt8.random(in: .min ... .max)}
    var iteration = 0
    while true {
        client.push(queue: "Moinn", data, timeout: .seconds(1)) { error in 
            if let error {
                print("ERROR PUSHING:", error)
            }
        }
        client.push(queue: "Moin", data, timeout: .seconds(1)) { error in 
            if let error {
                print("ERROR PUSHING:", error)
            }
        }
        client.pop(queue: "Moin") { data, error in 
            if let error {
                print("ERROR POPPING:", error)
            } else {
                print("GOT DATA:", data.count, iteration)
            }
        }
        iteration += 1
        try client.update()
        Thread.sleep(forTimeInterval: 2)
    }
}

func testAsyncClient() async throws {
    let ip = "172.19.16.91"
    //let ip = "127.0.0.1"
    let client = try await AsyncMessageQueueClient.connect(host: ip, port: 25565, username: "sebbu", password: "passwrd")
    let data = (0..<128).map { _ in UInt8.random(in: .min ... .max)}
    let pushingTask = Task {
        for _ in 0..<10 {
            try await client.push(queue: "Moin", data)
        }
    }
    let timingOutPushTask = Task {
        for _ in 0..<10 {
            do {
                try await client.push(queue: "Moinn", data, timeout: .seconds(1))
            } catch {
                print("Push error:", error)
            }
        }
    }
    let poppingTask = Task {
        for _ in 0..<10 {
            let data = try await client.pop(queue: "Moin")
            print("Received data:", data.count)
        }
    }
    try await pushingTask.value
    await timingOutPushTask.value
    try await poppingTask.value
}

try await testAsyncClient()

try await Task.sleep(for: .seconds(5))
print("Done")