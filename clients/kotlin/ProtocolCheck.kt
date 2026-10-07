package voice
import java.io.File
fun main(args: Array<String>) {
    // TSV is generated from the canonical JSON fixture by scripts/check_clients.py.
    for (line in File(args.single()).readLines()) {
        val fields=line.split('\t'); val hex=fields[1]
        val bytes=hex.chunked(2).map { it.toInt(16).toByte() }.toByteArray()
        if (fields[0] == "invalid") { check(runCatching { MediaPacket.decode(bytes) }.isFailure); continue }
        val p=MediaPacket.decode(bytes)
        check(p.epoch==fields[2].toLong() && p.sequence==fields[3].toLong())
        check(p.samples.map { it.toInt() }==fields[4].split(',').map { it.toInt() })
    }
    val c=PlaybackCursor(); val a=MediaPacket(1,1,shortArrayOf(0));val b=MediaPacket(1,2,shortArrayOf(0))
    check(c.receive(a));check(c.receive(b));check(c.didPlay(b)==null);check(c.didPlay(a)?.seq==2L)
    c.clear(2);check(c.didPlay(b)==null);check(!c.receive(a))
    println("Kotlin wire fixtures, contiguous receipts, stale epoch cancellation: PASS")
}
