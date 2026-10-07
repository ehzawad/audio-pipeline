import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

public enum VoiceFailure: Error { case invalidPacket, sequenceGap, invalidOrigin, invalidSession, control(Int) }
public struct MediaPacket: Sendable {
    public let epoch: UInt32
    public let sequence: UInt32
    public let samples: [Int16]
    public static let sampleRate = 24_000
    public init(data: Data) throws {
        let b = [UInt8](data)
        guard b.count >= 10, b.count <= 968, (b.count - 8) % 2 == 0 else { throw VoiceFailure.invalidPacket }
        func u32(_ i: Int) -> UInt32 { UInt32(b[i]) | UInt32(b[i+1]) << 8 | UInt32(b[i+2]) << 16 | UInt32(b[i+3]) << 24 }
        epoch = u32(0); sequence = u32(4)
        guard epoch > 0, sequence > 0 else { throw VoiceFailure.invalidPacket }
        samples = stride(from: 8, to: b.count, by: 2).map { Int16(bitPattern: UInt16(b[$0]) | UInt16(b[$0+1]) << 8) }
    }
}
public struct PlaybackReceipt: Encodable, Equatable, Sendable {
    public let type = "played"
    public let epoch: UInt32
    public let seq: UInt32
}
/// Confine to one actor/audio-control queue. Receipt means renderer completion, not human hearing.
public struct PlaybackCursor: Sendable {
    public private(set) var epoch: UInt32 = 0
    public private(set) var received: UInt32 = 0
    public private(set) var played: UInt32 = 0
    private var finished: Set<UInt32> = []
    public init() {}
    @discardableResult public mutating func clear(_ next: UInt32) throws -> Bool {
        guard next > 0 else { throw VoiceFailure.invalidPacket }
        guard next > epoch else { return false }
        epoch = next; received = 0; played = 0; finished.removeAll(); return true
    }
    public mutating func receive(_ packet: MediaPacket) throws -> Bool {
        guard packet.epoch >= epoch else { return false }
        try clear(packet.epoch)
        guard received < UInt32.max, packet.sequence == received + 1, received - played < 100 else { throw VoiceFailure.sequenceGap }
        received = packet.sequence; return true
    }
    public mutating func didPlay(_ packet: MediaPacket) -> PlaybackReceipt? {
        guard packet.epoch == epoch, packet.sequence > played, packet.sequence <= received else { return nil }
        finished.insert(packet.sequence); let before = played
        while played < UInt32.max, finished.remove(played + 1) != nil { played += 1 }
        return played > before ? PlaybackReceipt(epoch: epoch, seq: played) : nil
    }
}
public struct SessionGrant: Decodable, Sendable {
    public let ticket: String
    public let sessionID: String
    public let state: String
    public let expiresAt: Double
    enum CodingKeys: String, CodingKey { case ticket, sessionID = "session_id", state, expiresAt = "grant_expires_at" }
}
private final class NoRedirect: NSObject, URLSessionTaskDelegate {
    func urlSession(_ session: URLSession, task: URLSessionTask, willPerformHTTPRedirection response: HTTPURLResponse,
                    newRequest request: URLRequest, completionHandler: @escaping (URLRequest?) -> Void) { completionHandler(nil) }
}
/// Your authenticated application backend provides the identity token; never ship issuer keys.
public actor VoiceControl {
    public let origin: URL
    private let token: @Sendable () async throws -> String
    private let session: URLSession
    private let mediaSession: URLSession
    public init(origin: URL, token: @escaping @Sendable () async throws -> String) throws {
        let loopback = ["localhost", "127.0.0.1", "[::1]", "::1"].contains(origin.host ?? "")
        guard origin.user == nil, origin.password == nil, origin.query == nil, origin.fragment == nil,
              ["", "/"].contains(origin.path), origin.host != nil,
              origin.scheme == "https" || (origin.scheme == "http" && loopback) else { throw VoiceFailure.invalidOrigin }
        self.origin = origin; self.token = token
        let configuration = URLSessionConfiguration.ephemeral
        configuration.timeoutIntervalForRequest = 10; configuration.timeoutIntervalForResource = 15
        self.session = URLSession(configuration: configuration, delegate: NoRedirect(), delegateQueue: nil)
        let media = URLSessionConfiguration.ephemeral
        media.timeoutIntervalForRequest = 20; media.timeoutIntervalForResource = 14400
        self.mediaSession = URLSession(configuration: media, delegate: NoRedirect(), delegateQueue: nil)
    }
    deinit { session.invalidateAndCancel(); mediaSession.invalidateAndCancel() }
    private func request(_ path: String, method: String, key: String? = nil, body: Data? = nil) async throws -> Data {
        var req = URLRequest(url: origin.appendingPathComponent(path)); req.httpMethod = method; req.httpBody = body
        let identity = try await token()
        guard !identity.isEmpty, identity.utf8.count <= 4096 else { throw VoiceFailure.invalidSession }
        req.setValue("Bearer " + identity, forHTTPHeaderField: "Authorization")
        if let key { req.setValue(key, forHTTPHeaderField: "Idempotency-Key") }
        if body != nil { req.setValue("application/json", forHTTPHeaderField: "Content-Type") }
        let (bytes, response) = try await session.data(for: req)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            throw VoiceFailure.control((response as? HTTPURLResponse)?.statusCode ?? 0)
        }
        return bytes
    }
    private func path(_ id: String) throws -> String {
        guard id.count == 32, id.allSatisfy({ "0123456789abcdef".contains($0) }) else { throw VoiceFailure.invalidSession }
        return "v1/sessions/" + id
    }
    public func create(idempotencyKey: String) async throws -> SessionGrant {
        guard !idempotencyKey.isEmpty, idempotencyKey.utf8.count <= 128 else { throw VoiceFailure.invalidSession }
        let data = try await request("v1/sessions", method: "POST", key: idempotencyKey, body: Data(#"{"transport":"websocket"}"#.utf8))
        return try JSONDecoder().decode(SessionGrant.self, from: data)
    }
    public func status(_ id: String) async throws -> Data { try await request(path(id), method: "GET") }
    public func stop(_ id: String) async throws { _ = try await request(path(id), method: "DELETE") }
    public func reconnect(_ id: String) async throws -> SessionGrant {
        let data = try await request(path(id) + "/reconnect", method: "POST")
        return try JSONDecoder().decode(SessionGrant.self, from: data)
    }
    /// Return a new task for a fresh grant. The app owns its receive loop and audio engine.
    public func mediaTask(_ grant: SessionGrant) throws -> URLSessionWebSocketTask {
        guard grant.state == "reserved" else { throw VoiceFailure.invalidSession }
        var url = URLComponents(url: origin, resolvingAgainstBaseURL: false)!
        url.scheme = origin.scheme == "https" ? "wss" : "ws"; url.path = "/ws"
        let task = mediaSession.webSocketTask(with: url.url!, protocols: ["duplex", grant.ticket])
        task.maximumMessageSize = 65536
        return task // call resume(), send hello, then serialize capture/receipt writes in an actor
    }
}
