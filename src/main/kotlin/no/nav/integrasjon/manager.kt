package no.nav.integrasjon

import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import mu.KotlinLogging
import java.util.*
import javax.jms.ConnectionFactory

sealed class Status
object Problem: Status()
object Ready: Status()

class Channels<V,T>: AutoCloseable {

    val transformedEvents = Channel<T>()
    val commitAction = Channel<CommitAction>()
    val status = Channel<Status>()
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

    val log = KotlinLogging.logger {  }

    Channels<V,T>().use { c ->

        // kick off coroutines in reverse order

        val jmsWriter = jmsWriterAsync(
                connectionFactory,
                queueName,
                c.transformedEvents,
                toText,
                c.commitAction,
                c.status)

        if (c.status.receive() == Problem) return@async

        log.debug { "jmsWriter up and running" }

        val transformer = eventTransformerAsync(
                c.kafkaEvents,
                c.transformedEvents,
                transform,
                c.status)

        if (c.status.receive() == Problem) {
            jmsWriter.cancelAndJoin()
            return@async
        }

        log.debug { "eventTransformer up and running" }

        val consumer = kafkaTopicListenerAsync<K, V>(
                consumerProps,
                topic,
                kafkaEvents = c.kafkaEvents,
                commitAction = c.commitAction,
                status = c.status)

        if (c.status.receive() == Problem) {
            jmsWriter.cancelAndJoin()
            transformer.cancelAndJoin()
            return@async
        }

        log.debug { "kafkaTopicListener up and running" }
        try {
            while (isActive && (c.status.poll()?.let { it } != Problem)) {
                delay(1_000)
                log.info("managePipelineAsync in loop")
            }
        }
        catch (e: CancellationException) {
            log.error("CancellationException",e)
        }
        finally {
            withContext(NonCancellable) {
                consumer.cancelAndJoin()
                transformer.cancelAndJoin()
                jmsWriter.cancelAndJoin()

                log.info("leaving managePipelineAsync")
            }

        }
    }
}