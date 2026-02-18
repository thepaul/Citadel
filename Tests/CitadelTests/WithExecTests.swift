@testable import Citadel
import NIO
@preconcurrency import NIOSSH
import XCTest
import Foundation

@available(macOS 15.0, *)
final class WithExecTests: XCTestCase {

    // MARK: - Helpers

    private struct TestTimeout: Error {}

    private func runTest(
        timeout: Duration = .seconds(5),
        perform: @escaping (SSHServer, SSHClient) async throws -> Void
    ) async throws {
        let authDelegate = AuthDelegate(supportedAuthenticationMethods: .password) { request, promise in
            switch request.request {
            case .password(.init(password: "test")) where request.username == "citadel":
                promise.succeed(.success)
            default:
                promise.succeed(.failure)
            }
        }
        let server = try await SSHServer.host(
            host: "localhost",
            port: 0,
            hostKeys: [NIOSSHPrivateKey(p521Key: .init())],
            authenticationDelegate: authDelegate
        )

        let port = try XCTUnwrap(server.channel.localAddress?.port)

        let client = try await SSHClient.connect(
            host: "localhost",
            port: port,
            authenticationMethod: .passwordBased(username: "citadel", password: "test"),
            hostKeyValidator: .acceptAnything(),
            reconnect: .never
        )

        defer {
            Task { try? await server.close() }
        }

        do {
            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    try await perform(server, client)
                }
                group.addTask {
                    try await Task.sleep(for: timeout)
                    throw TestTimeout()
                }
                // First to finish wins; cancel the other
                try await group.next()
                group.cancelAll()
            }
        } catch is TestTimeout {
            XCTFail("Test timed out after \(timeout)")
        } catch let error as ChannelError where error == .alreadyClosed {
            // Server-initiated channel close can race with client close in withExec
        }

        do {
            try await client.close()
        } catch let error as ChannelError where error == .alreadyClosed {
            // Already cleaned up
        }
    }

    // MARK: - ExecHandler infrastructure tests
    // These use public APIs other than withExec to show the fixes are
    // necessary at the server handler level, not specific to one client API.

    /// Stderr-only output is delivered via executeCommand.
    /// Exercises the writeAndFlush fix: without it, stderr data sits in the
    /// channel's write buffer and never reaches the client because nothing
    /// triggers a flush.
    func testExecuteCommandReceivesStderrOnly() async throws {
        final class StderrOnly: ExecDelegate, @unchecked Sendable {
            struct Ctx: ExecCommandContext {
                func terminate() async throws {}
            }
            func setEnvironmentValue(_ value: String, forKey key: String) async throws {}
            func start(command: String, outputHandler: ExecOutputHandler) async throws -> ExecCommandContext {
                DispatchQueue.global().async {
                    let handle = outputHandler.stderrPipe.fileHandleForWriting
                    handle.write(Data("only on stderr".utf8))
                    try? handle.close()
                    // No stdout at all — close it immediately
                    try? outputHandler.stdoutPipe.fileHandleForWriting.close()
                    Thread.sleep(forTimeInterval: 0.3)
                    outputHandler.succeed(exitCode: 0)
                }
                return Ctx()
            }
        }

        try await runTest { server, client in
            let execDelegate = StderrOnly()
            server.enableExec(withDelegate: execDelegate)

            let result = try await client.executeCommand("test", mergeStreams: true)
            XCTAssertEqual(String(buffer: result), "only on stderr")
        }
    }

    /// Stderr arrives on the dedicated stderr stream of executeCommandPair.
    /// Same underlying fix: without writeAndFlush the data never flushes.
    func testExecuteCommandPairReceivesStderr() async throws {
        final class StderrExec: ExecDelegate, @unchecked Sendable {
            struct Ctx: ExecCommandContext {
                func terminate() async throws {}
            }
            func setEnvironmentValue(_ value: String, forKey key: String) async throws {}
            func start(command: String, outputHandler: ExecOutputHandler) async throws -> ExecCommandContext {
                DispatchQueue.global().async {
                    outputHandler.stderrPipe.fileHandleForWriting.write(Data("err".utf8))
                    try? outputHandler.stderrPipe.fileHandleForWriting.close()
                    outputHandler.stdoutPipe.fileHandleForWriting.write(Data("out".utf8))
                    try? outputHandler.stdoutPipe.fileHandleForWriting.close()
                    Thread.sleep(forTimeInterval: 0.3)
                    outputHandler.succeed(exitCode: 0)
                }
                return Ctx()
            }
        }

        try await runTest { server, client in
            let execDelegate = StderrExec()
            server.enableExec(withDelegate: execDelegate)

            let streams = try await client.executeCommandPair("test")

            var stdoutBuf = ByteBuffer()
            for try await chunk in streams.stdout {
                stdoutBuf.writeImmutableBuffer(chunk)
            }

            var stderrBuf = ByteBuffer()
            for try await chunk in streams.stderr {
                stderrBuf.writeImmutableBuffer(chunk)
            }

            XCTAssertEqual(String(buffer: stdoutBuf), "out")
            XCTAssertEqual(String(buffer: stderrBuf), "err")
        }
    }
}
