package no.nav.integrasjon

import kotlinx.coroutines.experimental.CancellationException
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.ClosedSendChannelException
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory

import java.util.*

sealed class CommitAction
object DoCommit : CommitAction()
object NoCommit : CommitAction()

fun <K,V>kafkaTopicListenerAsync(
        consumerProps: Properties,
        topic: String,
        timeout: Long = 10_000,
        kafkaEvents: SendChannel<V>,
        commitAction: ReceiveChannel<CommitAction>,
        status: SendChannel<Status>) = async {

    val log = KotlinLogging.logger {  }

    try {
        KafkaConsumer<K, V>(consumerProps)
                .apply { subscribe(kotlin.collections.listOf(topic)) }
                .use { c ->

                    var allGood = true
                    status.send(Ready)

                    while (isActive && allGood) {

                        c.poll(timeout).forEach { e ->

                            log.debug {"FETCHED from kafka!" }

                            // send event further down the pipeline

                            kafkaEvents.send(e.value())

                            // wait for feedback from pipeline
                            when (commitAction.receive()) {
                                DoCommit -> try {
                                    c.commitSync()
                                }
                                catch (e: CommitFailedException) {
                                    log.error("CommitFailedException", e)
                                    allGood = false
                                }
                                NoCommit -> {
                                    // problems further down the pipeline
                                    allGood = false
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
    if (isActive && !status.isClosedForSend) status.send(Problem)
}
