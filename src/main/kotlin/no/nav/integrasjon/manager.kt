package no.nav.integrasjon

import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import mu.KotlinLogging
import java.util.*
import javax.jms.ConnectionFactory

sealed class Status
object Problem: Status()
object Ready: Status()

class Channels<V,T>(noOfCoroutines: Int): AutoCloseable {

    val transformedEvents = Channel<T>()
    val commitAction = Channel<CommitAction>()
    val status = Channel<Status>(noOfCoroutines)
    val kafkaEvents = Channel<V>()

    override fun close() {
        transformedEvents.close()
        commitAction.close()
        status.close()
        kafkaEvents.close()
    }
}

fun <K,V,T>managePipelineAsync(
        consumerProps: Properties,
        topic: String,
        transform: (V) -> T,
        connectionFactory: ConnectionFactory,
        queueName: String,
        toText: (T) -> String) = async {

    val log = KotlinLogging.logger { }
    val c = Channels<V, T>(3)
    val r = mutableListOf<Job>()

    // kick off coroutines in reverse order

    r.add(jmsWriterAsync(
            connectionFactory,
            queueName,
            c.transformedEvents,
            toText,
            c.commitAction,
            c.status))

    if (c.status.receive() == Problem) {
        c.close()
        return@async
    }

    log.debug { "jmsWriter up and running" }

    r.add(eventTransformerAsync(
            c.kafkaEvents,
            c.transformedEvents,
            transform,
            c.status))

    if (c.status.receive() == Problem) {
        r.filter { it.isActive }.forEach { it.cancelAndJoin() }
        c.close()
        return@async
    }
    log.debug { "eventTransformer up and running" }

    r.add(kafkaTopicListenerAsync<K, V>(
            consumerProps,
            topic,
            kafkaEvents = c.kafkaEvents,
            commitAction = c.commitAction,
            status = c.status))

    if (c.status.receive() == Problem) {
        r.filter { it.isActive }.forEach { it.cancelAndJoin() }
        c.close()
        return@async
    }
    log.debug { "kafkaTopicListener up and running" }

    try {
        while (isActive && (c.status.receive().let { it } != Problem)) {}
    }
    finally {
        withContext(NonCancellable) {
            r.reversed().forEach { it.cancelAndJoin() }
            c.close()
            log.info("@end of async - leaving managePipelineAsync")
        }
    }
}