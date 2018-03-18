package no.nav.integrasjon.manager

import kotlinx.coroutines.experimental.channels.Channel

sealed class Status
object Problem: Status()
object Ready: Status()

class Channels<V>(noOfCoroutines: Int): AutoCloseable {

    val fromDownstream = Channel<Status>()
    val toManager = Channel<Status>(noOfCoroutines)
    val toDownstream = Channel<V>()

    override fun close() {
        toDownstream.close()
        fromDownstream.close()
        toManager.close()
    }
}
