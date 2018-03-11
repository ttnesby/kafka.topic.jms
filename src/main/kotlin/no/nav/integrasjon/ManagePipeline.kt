package no.nav.integrasjon

import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import mu.KotlinLogging

sealed class Status
object Problem: Status()
object Ready: Status()

class Channels<V>(noOfCoroutines: Int): AutoCloseable {

    val commitAction = Channel<CommitAction>()
    val status = Channel<Status>(noOfCoroutines)
    val kafkaEvents = Channel<V>()

    override fun close() {
        commitAction.close()
        status.close()
        kafkaEvents.close()
    }
}

class ManagePipeline<K,V>(
        private val kafkaTopicConsumer: KafkaTopicConsumer<K,V>,
        private val jmsTextMessageWriter: JMSTextMessageWriter<V>) {

    private val c = Channels<V>(2)

    fun manageAsync() = async {

        val r = mutableListOf<Job>()
        var latestStatus : Status = Ready

        // kick off coroutines starting with the end of pipeline

        r.add(jmsTextMessageWriter.writeAsync(c.kafkaEvents, c.commitAction, c.status))

        if (c.status.receive() == Problem) {
            c.close()
            return@async
        }

        log.debug { "JMSTextMessageWriter::writeAsync is up and running" }

        r.add(kafkaTopicConsumer.consumeAsync(c.kafkaEvents, c.commitAction,c.status))

        if (c.status.receive() == Problem) {
            r.filter { it.isActive }.forEach { it.cancelAndJoin() }
            c.close()
            return@async
        }
        log.debug { "KafkaTopicConsumer::consumeAsync is up and running" }

        try {
            while (isActive && (c.status.receive().let {
                        latestStatus = it
                    it} != Problem)) {}
        }
        finally {
            if (latestStatus == Problem) log.error("Coroutine reported problem - shutting down everything!")
            withContext(NonCancellable) {
                r.reversed().forEach { it.cancelAndJoin() }
                c.close()
                log.info("@end of manageAsync - goodbye!")
            }
        }
    }

    companion object {

        private val log = KotlinLogging.logger {  }

        inline fun <reified K, reified V> init(
                clientDetails: KafkaClientDetails,
                jmsTextMessageWriter: JMSTextMessageWriter<V>): ManagePipeline<K,V> = ManagePipeline(
                    KafkaTopicConsumer.init(clientDetails),
                    jmsTextMessageWriter)
    }
}
