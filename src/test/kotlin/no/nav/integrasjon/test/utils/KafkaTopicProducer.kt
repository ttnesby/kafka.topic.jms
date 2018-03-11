package no.nav.integrasjon.test.utils

import kotlinx.coroutines.experimental.CancellationException
import kotlinx.coroutines.experimental.async
import mu.KotlinLogging
import no.nav.integrasjon.KafkaClientDetails
import no.nav.integrasjon.getKafkaSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*
import kotlin.reflect.full.starProjectedType

inline fun <reified K, reified V> producerInjection(baseProps: Properties) = baseProps.apply {
    set(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, getKafkaSerializer(K::class.starProjectedType))
    set(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, getKafkaSerializer(V::class.starProjectedType))
    set(ProducerConfig.ACKS_CONFIG, "all")
    set(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1)
}

class KafkaTopicProducer<K, in V>(private val clientDetails: KafkaClientDetails, private val key: K) {

    fun produceAsync(data: List<V>) = async {

        try {
            // best effort to send data synchronously
            KafkaProducer<K, V>(clientDetails.baseProps).use { p ->
                data.forEach { d ->
                    p.send(ProducerRecord<K, V>(clientDetails.topic, key, d)).get()
                }
            }
        }
        catch (e: Exception) {
            when (e) {
                is CancellationException -> {/* it's ok*/ }
                else -> log.error("Exception", e)
            }
        }
    }

    companion object {

        private val log = KotlinLogging.logger {  }

        inline fun <reified K, reified V> init(
                clientDetails: KafkaClientDetails,
                key: K) = KafkaTopicProducer<K,V>(
                KafkaClientDetails(
                        producerInjection<K,V>(clientDetails.baseProps),
                        clientDetails.topic),
                key)
    }
}
