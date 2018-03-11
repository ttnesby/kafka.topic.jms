package no.nav.integrasjon

import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import mu.KotlinLogging

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

class ManagePipeline<K,V,T>(
        private val kafkaTopicConsumer: KafkaTopicConsumer<K,V>,
        private val transform: (V) -> T,
        private val jmsDetails: JMSDetails,
        private val toText: (T) -> String) {

    private val c = Channels<V,T>(3)

    fun manageAsync() = async {

        val r = mutableListOf<Job>()

        // kick off coroutines in reverse order

        r.add(jmsWriterAsync(
                jmsDetails,
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

        r.add(kafkaTopicConsumer.consumeAsync(c.kafkaEvents, c.commitAction,c.status))

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

    companion object {

        private val log = KotlinLogging.logger {  }

        inline fun <reified K, reified V, T> init(
                clientDetails: KafkaClientDetails,
                noinline transform: (V) -> T,
                jmsDetails: JMSDetails,
                noinline toText: (T) -> String): ManagePipeline<K,V,T> = ManagePipeline(
                    KafkaTopicConsumer.init(clientDetails),
                    transform,
                    jmsDetails,
                    toText)
    }
}
