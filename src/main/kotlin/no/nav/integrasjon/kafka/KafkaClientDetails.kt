package no.nav.integrasjon.kafka

import java.util.*

data class KafkaClientDetails(
        val baseProps: Properties,
        val topic: String,
        val pollTimeout: Long = 10_000L //for consumer
)