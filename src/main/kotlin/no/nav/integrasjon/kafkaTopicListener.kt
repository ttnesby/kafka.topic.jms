package no.nav.integrasjon

import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.*

sealed class CommitAction

object DoCommit : CommitAction()
object NoCommit : CommitAction()

fun <K,V>kafkaTopicListenerAsync(
        consumerProps: Properties,
        topic: String,
        timeout: Long = 10_000,
        pipeline: SendChannel<V>,
        commitAction: ReceiveChannel<CommitAction>) = async {

    KafkaConsumer<K,V>(consumerProps)
            .apply { subscribe(kotlin.collections.listOf(topic)) }
            .use { c ->

                var noException = true

                while (isActive && noException) {

                    c.poll(timeout).forEach { e ->

                        // send event further down the pipeline
                        pipeline.send(e.value())

                        // wait for feedback from pipeline
                        when(commitAction.receive()) {
                            DoCommit -> try {
                                c.commitSync()
                            }
                            catch (e: CommitFailedException) {
                                //TODO - should log error and exit
                                noException = false
                            }
                            NoCommit -> {
                                // No success later in pipeline
                                noException = false
                            }
                        }
                    }
                }
            }
}
