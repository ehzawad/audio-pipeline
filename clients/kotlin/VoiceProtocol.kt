package voice

/** Android/JVM wire core. AudioRecord/AudioTrack and the WebSocket lifecycle belong to the app. */
data class MediaPacket(val epoch: Long, val sequence: Long, val samples: ShortArray) {
    companion object {
        const val SAMPLE_RATE = 24000
        fun decode(bytes: ByteArray): MediaPacket {
            require(bytes.size in 10..968 && (bytes.size - 8) % 2 == 0) { "Invalid PCM envelope" }
            fun u32(i: Int): Long = (0..3).fold(0L) { n, j -> n or ((bytes[i+j].toLong() and 255L) shl (8*j)) }
            val epoch = u32(0); val seq = u32(4)
            require(epoch > 0 && seq > 0) { "Epoch and sequence must be positive" }
            val samples = ShortArray((bytes.size-8)/2) { i ->
                ((bytes[8+2*i].toInt() and 255) or ((bytes[9+2*i].toInt() and 255) shl 8)).toShort()
            }
            return MediaPacket(epoch, seq, samples)
        }
    }
}
data class PlaybackReceipt(val epoch: Long, val seq: Long) {
    fun json() = """{"type":"played","epoch":$epoch,"seq":$seq}"""
}
/** Use one coroutine/actor or external lock. A receipt is rendering completion, NOT network arrival. */
class PlaybackCursor {
    var epoch = 0L; private set
    var received = 0L; private set
    var played = 0L; private set
    private val completed = mutableSetOf<Long>()
    fun clear(next: Long): Boolean {
        require(next in 1..0xffffffffL)
        if (next <= epoch) return false
        epoch=next; received=0; played=0; completed.clear(); return true
    }
    fun receive(packet: MediaPacket): Boolean {
        if (packet.epoch < epoch) return false
        clear(packet.epoch)
        require(packet.sequence == received+1 && received-played < 100) { "Sequence gap or playout backlog" }
        received=packet.sequence; return true
    }
    fun didPlay(packet: MediaPacket): PlaybackReceipt? {
        if (packet.epoch != epoch || packet.sequence <= played || packet.sequence > received) return null
        completed.add(packet.sequence); val before=played
        while (completed.remove(played+1)) played++
        return if (played > before) PlaybackReceipt(epoch, played) else null
    }
}
