package no.nav.integrasjon.test.utils

import kotlinx.coroutines.experimental.CancellationException
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.delay
import mu.KotlinLogging
import no.nav.integrasjon.Bootstrap
import no.nav.integrasjon.kafka.KafkaClientProperties
import no.nav.integrasjon.kafka.KafkaTopicConsumer
import no.nav.integrasjon.kafka.getKafkaSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*
import kotlin.reflect.full.starProjectedType


class KafkaTopicProducer<K, in V>(
        private val clientProperties: KafkaClientProperties,
        private val key: K,
        private val untilShutdown: Boolean = false,
        private val delayTime: Long = 250) {

    private val topic = KafkaTopicConsumer.event2Topic(clientProperties.kafkaEvent)

    fun produceAsync(data: List<V>) = async {

        try {
            if (!untilShutdown) {
                // best effort to send data synchronously
                log.info("@start of produceAsync")

                KafkaProducer<K, V>(clientProperties.baseProps).use { p ->
                    data.forEach { d ->
                        p.send(ProducerRecord<K, V>(topic, null, d)).get()
                        delay(delayTime)
                        log.debug { "Sent record to kafka topic $topic" }
                    }
                }
            }
            else {
                while (!Bootstrap.shutdownhookActive) {
                    // best effort to send data synchronously
                    log.info("@start of produceAsync")

                    KafkaProducer<K, V>(clientProperties.baseProps).use { p ->
                        data.forEach { d ->
                            p.send(ProducerRecord<K, V>(topic, null, d)).get()
                            delay(delayTime)
                            log.debug { "Sent record to kafka topic $topic" }
                        }
                    }
                }
            }
        }
        catch (e: Exception) {
            when (e) {
                is CancellationException -> {/* it's ok*/ }
                else -> log.error("Exception", e)
            }
        }

        log.info("@end of produceAsync - goodbye!")
    }

    companion object {

        private val log = KotlinLogging.logger {  }

        inline fun <reified K, reified V> init(
                clientProperties: KafkaClientProperties,
                key: K, untilShutdown: Boolean = false, delayTime: Long = 250) = KafkaTopicProducer<K,V>(
                KafkaClientProperties(
                        producerInjection<K,V>(clientProperties.baseProps),
                        clientProperties.kafkaEvent),
                key,
                untilShutdown,
                delayTime)

        inline fun <reified K, reified V> producerInjection(baseProps: Properties) = baseProps.apply {
            set(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, getKafkaSerializer(K::class.starProjectedType))
            set(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, getKafkaSerializer(V::class.starProjectedType))
            set(ProducerConfig.ACKS_CONFIG, "all")
            set(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1)
            set(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 120_000)
        }
    }
}
