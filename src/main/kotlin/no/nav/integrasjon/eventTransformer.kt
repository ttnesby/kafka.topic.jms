package no.nav.integrasjon

import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel

data class EventTransformed(val value: String)

fun <V>eventTransformerAsync(
        eventIn: ReceiveChannel<V>,
        pipeline: SendChannel<EventTransformed>) = async {

    var noException = true

    while (isActive && noException) {

        eventIn.receive().let {
            when(it) {
                is String -> {
                    pipeline.send(EventTransformed(it.toUpperCase()))
                }
                else -> {}
            }
        }
    }
}