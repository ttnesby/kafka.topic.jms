package no.nav.integrasjon.manager

import kotlinx.coroutines.experimental.*
import mu.KotlinLogging
import no.nav.integrasjon.jms.JMSTextMessageWriter
import no.nav.integrasjon.kafka.KafkaClientDetails
import no.nav.integrasjon.kafka.KafkaTopicConsumer

class ManagePipeline<K,V>(
        private val kafkaTopicConsumer: KafkaTopicConsumer<K, V>,
        private val jmsTextMessageWriter: JMSTextMessageWriter<V>) {

    private val c = Channels<V>(2)

    fun manageAsync() = async {

        val r = mutableListOf<Job>()
        var latestStatus : Status = Ready

        // kick off coroutines starting with the end of pipeline

        r.add(jmsTextMessageWriter.writeAsync(c.toDownstream, c.fromDownstream, c.toManager))

        if (c.toManager.receive() == Problem) {
            c.close()
            return@async
        }

        r.add(kafkaTopicConsumer.consumeAsync(c.toDownstream, c.fromDownstream,c.toManager))

        if (c.toManager.receive() == Problem) {
            r.filter { it.isActive }.forEach { it.cancelAndJoin() }
            c.close()
            return@async
        }

        try {
            while (isActive && (c.toManager.receive().let {
                        latestStatus = it
                    it} != Problem)) {}
        }
        finally {
            if (latestStatus == Problem) log.error("Coroutine reported problem - shutting down everything!")
            withContext(NonCancellable) {
                r.reversed().forEach { it.cancelAndJoin() }
                c.close()
            }
        }
        log.info("@end of manageAsync - goodbye!")
    }

    companion object {

        private val log = KotlinLogging.logger {  }

        inline fun <reified K, reified V> init(
                clientDetails: KafkaClientDetails,
                jmsTextMessageWriter: JMSTextMessageWriter<V>): ManagePipeline<K, V> = ManagePipeline(
                KafkaTopicConsumer.init(clientDetails),
                jmsTextMessageWriter)
    }
}
