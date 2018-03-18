package no.nav.integrasjon.kafka

import kotlinx.coroutines.experimental.CancellationException
import kotlinx.coroutines.experimental.async
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

class KafkaTopicConsumer<K, out V>(private val clientDetails: KafkaClientDetails) {

    fun consumeAsync(
            toDownstream: SendChannel<V>,
            fromDownStream: ReceiveChannel<Status>,
            toManager: SendChannel<Status>) = async {
        try {
            KafkaConsumer<K, V>(clientDetails.baseProps)
                    .apply {
                        // be a loner - independent of group logic by reading from all partitions for topic
                        assign(partitionsFor(clientDetails.topic).map { TopicPartition(it.topic(),it.partition()) })
                    }
                    .use { c ->

                        var allGood = true
                        toManager.send(Ready)

                        log.info("@start of consumeAsync")

                        while (isActive && allGood) {

                            c.poll(clientDetails.pollTimeout).forEach { e ->

                                log.debug {"Polled event from kafka topic ${clientDetails.topic}" }

                                // send event further down the pipeline

                                toDownstream.send(e.value())
                                log.debug { "Sent event to pipeline - $e" }

                                // wait for feedback from pipeline
                                when (fromDownStream.receive()) {
                                    Ready -> try {
                                        c.commitSync()
                                        log.debug { "Got Ready from downstream - event committed" }
                                    }
                                    catch (e: CommitFailedException) {
                                        log.error("CommitFailedException", e)
                                        allGood = false
                                    }
                                    Problem -> {
                                        // problems further down the pipeline
                                        allGood = false
                                        log.debug { "Got Problem from downstream - time to leave" }
                                    }
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
        // IllegalArgumentException, IllegalStateException, InvalidOffsetException, WakeupException
        // InterruptException, AuthenticationException, AuthorizationException, KafkaException
        // IllegalArgumentException, IllegalStateException

        // notify manager if this job is still active
        if (isActive && !toManager.isClosedForSend) {
            toManager.send(Problem)
            log.error("Reported problem to manager")
        }
        log.info("@end of consumeAsync - goodbye!")
    }

    companion object {

        private val log = KotlinLogging.logger {  }

        inline fun <reified K, reified V> init(clientDetails: KafkaClientDetails) = KafkaTopicConsumer<K, V>(
                KafkaClientDetails(
                        consumerInjection<K, V>(clientDetails.baseProps),
                        clientDetails.topic,
                        clientDetails.pollTimeout
                ))

        inline fun <reified K, reified V> consumerInjection(baseProps: Properties) = baseProps.apply {
            set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, getKafkaDeserializer(K::class.starProjectedType))
            set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, getKafkaDeserializer(V::class.starProjectedType))

            // want to be a loner for topic / not using group id - see assignment to partitions for the topic
            //set(ConsumerConfig.GROUP_ID_CONFIG, "")
            set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            // only commit after successful put to JMS
            set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
            // poll only one record of
            set(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1)
            //TODO - must set max.bytes to at least 5 MB
        }
    }
}
