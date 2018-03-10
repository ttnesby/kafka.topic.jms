package no.nav.integrasjon

import kotlinx.coroutines.experimental.CancellationException
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import mu.KotlinLogging

fun <V,T>eventTransformerAsync(
        eventIn: ReceiveChannel<V>,
        pipeline: SendChannel<T>,
        transform: (V) -> T,
        status: SendChannel<Status>) = async {

    val log = KotlinLogging.logger { }

    var allGood = true
    status.send(Ready)

    try {
        while (isActive && allGood) {
            eventIn.receive().also { pipeline.send(transform(it)) }
            log.debug { "TRANSFORMED in pipeline!" }
        }
    }
    catch (e: Exception) {
        when (e) {
            is CancellationException -> {/* it's ok*/ }
            else -> log.error("Exception", e)
        }
    }

    // notify manager if this job is still active
    if (isActive && !status.isClosedForSend) status.send(Problem)
}