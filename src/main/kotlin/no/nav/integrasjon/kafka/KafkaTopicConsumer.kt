package no.nav.integrasjon.kafka

import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import mu.KotlinLogging
import no.nav.integrasjon.manager.Problem
import no.nav.integrasjon.manager.Ready
import no.nav.integrasjon.manager.Status
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.util.*
import kotlin.reflect.full.starProjectedType

class KafkaTopicConsumer<K, out V>(
        private val clientProperties: KafkaClientProperties,
        toDownstream: SendChannel<V>,
        status: ReceiveChannel<Status>) : AutoCloseable {

    private val asyncProcess: Job

    init {
        log.info { "Starting" }
        asyncProcess = consumeAsync(toDownstream, status)
    }

    override fun close() = runBlocking {
        log.info { "Closing" }
        asyncProcess.cancelAndJoin()
        log.info { "Closed" }
    }

    val isActive
        get() = asyncProcess.isActive


    private fun consumeAsync(
            toDownstream: SendChannel<V>,
            status: ReceiveChannel<Status>) = async {
        try {
            KafkaConsumer<K, V>(clientProperties.baseProps)
                    .apply {
                        // be a loner - independent of group logic by reading from all partitions for topic
                        assign(partitionsFor(event2Topic(clientProperties.kafkaEvent))
                                .map { TopicPartition(it.topic(), it.partition()) })
                    }
                    .use { c ->

                        var allGood = true

                        log.info("@start of consumeAsync")

                        while (isActive && allGood) {

                            c.poll(clientProperties.pollTimeout).forEach { e ->

                                val tpo = "topic ${e.topic()}, partition ${e.partition()} and offset ${e.offset()}"

                                log.info {"Polled event from kafka $tpo"}

                                log.info { "Send event downstream and wait for response" }
                                toDownstream.send(e.value())

                                // wait for feedback from pipeline
                                when (status.receive()) {
                                    Ready -> try {
                                        log.info { "Got Ready from downstream, trying commit" }
                                        c.commitSync()
                                        log.info { "Event $tpo is committed" }
                                    }
                                    catch (ex: CommitFailedException) {
                                        log.error("CommitFailedException", ex)
                                        log.error("Event $tpo NOT COMMITTED! Expect duplicates")
                                        log.error("Prepare for shutdown")
                                        allGood = false
                                    }
                                    Problem -> {
                                        // problems downstream
                                        log.error("Got Problem from downstream, prepare for shutdown")
                                        allGood = false

                                    }
                                }
                            }
                        }
                    }
        }
        // IllegalArgumentException, IllegalStateException, InvalidOffsetException, WakeupException
        // InterruptException, AuthenticationException, AuthorizationException, KafkaException
        // IllegalArgumentException, IllegalStateException
        catch (e: Exception) {
            when (e) {
                is CancellationException -> {/* it's ok to be cancelled by manager*/ }
                else -> log.error("Exception", e)
            }
        }
        finally {
            log.info("@end of consumeAsync - goodbye!")
        }
    }

    companion object {

        private val log = KotlinLogging.logger {  }

        inline fun <reified K, reified V> init(
                clientProperties: KafkaClientProperties,
                toDownstream: SendChannel<V>,
                status: ReceiveChannel<Status>) = KafkaTopicConsumer<K, V>(
                KafkaClientProperties(
                        propertiesInjection<K, V>(clientProperties.baseProps),
                        clientProperties.kafkaEvent,
                        clientProperties.pollTimeout
                ),
                toDownstream,
                status)

        inline fun <reified K, reified V> propertiesInjection(baseProps: Properties) = baseProps.apply {
            set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, getKafkaDeserializer(K::class.starProjectedType))
            set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, getKafkaDeserializer(V::class.starProjectedType))

            // want to be a loner for topic / not using group id - see assignment to partitions for the topic
            //set(ConsumerConfig.GROUP_ID_CONFIG, "")
            set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            // only commit after successful put to JMS
            set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
            // poll only one record of
            set(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1)
        }

        fun event2Topic(kafkaEvent: KafkaEvents): String = when (kafkaEvent) {
            KafkaEvents.OPPFOLGINGSPLAN -> "oppfolgingplan"
            KafkaEvents.BANKKONTONR -> "bankkontonr"
            KafkaEvents.MAALEKORT -> "maalekort"
            KafkaEvents.BARNEHAGELISTE -> "barnehageliste"
            KafkaEvents.STRING -> "string"
            KafkaEvents.INT -> "int"
            KafkaEvents.AVRO -> "avro"
            KafkaEvents.MUSIC -> "music"
        }
    }
}
