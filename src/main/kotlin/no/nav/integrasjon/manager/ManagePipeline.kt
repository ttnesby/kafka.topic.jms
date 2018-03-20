package no.nav.integrasjon.manager

import kotlinx.coroutines.experimental.*
import mu.KotlinLogging
import no.nav.integrasjon.jms.JMSTextMessageWriter
import no.nav.integrasjon.kafka.KafkaClientProperties
import no.nav.integrasjon.kafka.KafkaTopicConsumer

class ManagePipeline<K,V>(
        private val kafkaTopicConsumer: KafkaTopicConsumer<K, V>,
        private val jmsTextMessageWriter: JMSTextMessageWriter<V>) {

    private var allGood = false
    val isOk get() = allGood
    private val c = Channels<V>(2)

    fun manageAsync() = async {

        val r = mutableListOf<Job>()
        var latestStatus : Status = Ready

        // kick off coroutines starting with the end of pipeline

        log.info("starting JMS writeAsync")
        r.add(jmsTextMessageWriter.writeAsync(c.toDownstream, c.fromDownstream, c.toManager))

        if (c.toManager.receive() == Problem) {
            c.close()
            return@async
        }

        log.info("starting Kafka consumeAsync")
        r.add(kafkaTopicConsumer.consumeAsync(c.toDownstream, c.fromDownstream,c.toManager))

        if (c.toManager.receive() == Problem) {
            r.filter { it.isActive }.forEach { it.cancelAndJoin() }
            c.close()
            return@async
        }

        log.info("@start of manageAsync - monitoring pipeline")

        allGood = true

        try {
            while (isActive && (c.toManager.receive().let { latestStatus = it;it} != Problem)) {}
        }
        finally {
            if (latestStatus == Problem) {
                allGood = false
                log.error("Pipeline reported problem - shutting down")
            }
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
                clientProperties: KafkaClientProperties,
                jmsTextMessageWriter: JMSTextMessageWriter<V>): ManagePipeline<K, V> = ManagePipeline(
                KafkaTopicConsumer.init(clientProperties),
                jmsTextMessageWriter)
    }
}
