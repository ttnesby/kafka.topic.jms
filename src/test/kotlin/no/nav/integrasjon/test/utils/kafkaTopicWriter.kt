package no.nav.integrasjon.test.utils

import kotlinx.coroutines.experimental.async
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

fun <K,V>kafkaTopicWriterAsync(
        producerProps: Properties,
        topic: String,
        key: K,
        data: List<V>
        ) = async {

    // best effort to send data synchronously
    KafkaProducer<K,V>(producerProps).use { p ->
        data.forEach { d ->
            p.send(ProducerRecord<K, V>(topic, key, d)).get()
        }
    }
}