// swift-tools-version: 5.9
import PackageDescription
let package = Package(name: "VoiceProtocol", platforms: [.iOS(.v15), .macOS(.v12)],
    products: [.library(name: "VoiceProtocol", targets: ["VoiceProtocol"])],
    targets: [.target(name: "VoiceProtocol"), .testTarget(name: "VoiceProtocolTests", dependencies: ["VoiceProtocol"])])
