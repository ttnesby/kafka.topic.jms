package no.nav.integrasjon

import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.ClosedSendChannelException
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.KafkaConsumer

import java.util.*

sealed class CommitAction
object DoCommit : CommitAction()
object NoCommit : CommitAction()

sealed class ManagementStatus
object Problem: ManagementStatus()

fun <K,V>kafkaTopicListenerAsync(
        consumerProps: Properties,
        topic: String,
        timeout: Long = 10_000,
        pipeline: SendChannel<V>,
        commitAction: ReceiveChannel<CommitAction>,
        manager: SendChannel<ManagementStatus>) = async {

    try {
        KafkaConsumer<K, V>(consumerProps)
                .apply { subscribe(kotlin.collections.listOf(topic)) }
                .use { c ->

                    var allGood = true

                    while (isActive && allGood) {

                        c.poll(timeout).forEach { e ->

                            println("FETCHED from kafka!")

                            // send event further down the pipeline
                            try {
                                pipeline.send(e.value())

                                // wait for feedback from pipeline
                                when (commitAction.receive()) {
                                    DoCommit -> try {
                                        c.commitSync()
                                    } catch (e: CommitFailedException) {
                                        allGood = false
                                    }
                                    NoCommit -> {
                                        // problems further down the pipeline
                                        allGood = false
                                    }
                                }
                            }
                            catch (e: ClosedSendChannelException) {
                                allGood = false
                            }
                        }
                    }
                }
    }
    catch (e: Exception) {}
    // IllegalArgumentException, IllegalStateException, InvalidOffsetException, WakeupException
    // InterruptException, AuthenticationException, AuthorizationException, KafkaException
    // IllegalArgumentException, IllegalStateException

    // notify manager if this job is still active
    if (isActive) manager.send(Problem)
}
