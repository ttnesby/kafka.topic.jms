package no.nav.integrasjon

import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel

data class EventTransformed(val value: String)

fun <V>eventTransformerAsync(
        eventIn: ReceiveChannel<V>,
        pipeline: SendChannel<EventTransformed>,
        manager: SendChannel<ManagementStatus>) = async {

    var allGood = true

    try {
        while (isActive && allGood) {

            eventIn.receive().also {
                when (it) {
                    is String -> {
                        pipeline.send(EventTransformed(it.toUpperCase()))
                        println("TRANSFORMED!")
                    }
                    else -> {
                    }
                }
            }
        }
    }
    catch (e: Exception) {}

    // notify manager if this job is still active
    if (isActive) manager.send(Problem)
}