import XCTest
@testable import VoiceProtocol
final class VoiceProtocolTests: XCTestCase {
    func data(_ hex: String) -> Data {
        let chars = Array(hex); return Data(stride(from: 0, to: chars.count, by: 2).map { UInt8(String(chars[$0...($0+1)]), radix: 16)! })
    }
    func fixture() throws -> [String: Any] {
        let url = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
            .deletingLastPathComponent().deletingLastPathComponent().appendingPathComponent("fixtures/protocol.json")
        return try JSONSerialization.jsonObject(with: Data(contentsOf: url)) as! [String: Any]
    }
    func testSharedWireFixture() throws {
        let f = try fixture()
        for item in f["packets"] as! [[String: Any]] {
            let p = try MediaPacket(data: data(item["hex"] as! String))
            XCTAssertEqual(p.epoch, (item["epoch"] as! NSNumber).uint32Value)
            XCTAssertEqual(p.sequence, (item["seq"] as! NSNumber).uint32Value)
            XCTAssertEqual(p.samples.map(Int.init), item["samples"] as! [Int])
        }
        for hex in f["invalid"] as! [String] { XCTAssertThrowsError(try MediaPacket(data: data(hex))) }
    }
    func testCompletedPrefixAndCancellation() throws {
        var c = PlaybackCursor()
        let a = try MediaPacket(data: data("01000000010000000000")), b = try MediaPacket(data: data("01000000020000000000"))
        XCTAssertTrue(try c.receive(a)); XCTAssertTrue(try c.receive(b)); XCTAssertNil(c.didPlay(b))
        XCTAssertEqual(c.didPlay(a)?.seq, 2); try c.clear(2)
        XCTAssertNil(c.didPlay(b)); XCTAssertFalse(try c.receive(a))
    }
    func testOriginBoundary() throws {
        XCTAssertThrowsError(try VoiceControl(origin: URL(string: "http://public.example")!, token: {"short-lived"}))
        _ = try VoiceControl(origin: URL(string: "https://voice.example")!, token: {"short-lived"})
    }
}
