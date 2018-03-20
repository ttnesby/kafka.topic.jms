package no.nav.integrasjon.kafka

import java.util.*

/**
 * KafkaClientProperties is a data class for setting producer and consumer configuration
 *
 * Observe that KafkaTopicConsumer class injects its own relevant properties
 *
 * Minimum configuration for KafkaTopicConsumer is bootstrapping brokers, eventually schema reg if required
 * Setting client id is good practice and a topic to listen to is required
 *
 KafkaClientProperties(
    Properties().apply {
        set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kEnv.brokersURL)
        set(ConsumerConfig.CLIENT_ID_CONFIG, "kafkaTopic2jms")
        set("schema.registry.url",kEnv.serverPark.schemaregistry.url)
    },
    "aTopic"
 )
 */

data class KafkaClientProperties(
        val baseProps: Properties = Properties(),
        val kafkaEvent: KafkaEvents = KafkaEvents.MUSIC,
        val pollTimeout: Long = 10_000L,

        val isEmpty: Boolean = baseProps.size == 0
)