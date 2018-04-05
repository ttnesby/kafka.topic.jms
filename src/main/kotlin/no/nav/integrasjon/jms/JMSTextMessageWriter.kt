package no.nav.integrasjon.jms

import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import mu.KotlinLogging
import no.nav.integrasjon.*
import javax.jms.*
import kotlin.IllegalStateException

/**
 * JMSTextMessageWriter is an generic abstract class for sending TextMessage to a JMS-enabled backend system
 * By implementing the AutoCloseable interface, this class hides utilization of kotlin coroutines
 *
 * The overall concept
 *
 * A long living asynchronous process [writeAsync] performs the following simple tasks
 * - wait for reception of a data unit of type [V] from upstream
 * - [transform] the data unit to [Result]
 * - send the text message part of result to JMS backend
 * - send status (Ready, Problem) back to upstream
 *
 * @param V type of data to receive and send as text message to JMS
 * @param jmsProperties required details for establishing a JMS connection
 * @param status a channel for sending status
 *
 * @constructor will automatically initiate the [writeAsync] process
 *
 * @property data as a channel for receiving data of type [V]
 * @property isActive whether the [writeAsync] is active or not
 *
 * The user of the class must check if status channel has problem after start
 */
abstract class JMSTextMessageWriter<V>(
        private val jmsProperties: JMSProperties,
        status: SendChannel<Status>,
        jmsMetric: SendChannel<JMSMetric>) : AutoCloseable {

    // Result from the transform function, as a data class
    protected data class Result(val status: Boolean = false, val txtMsg: TextMessage)

    // data to receive
    val data = Channel<V>()
    private val asyncProcess: Job

    init {
        log.info { "Starting" }
        asyncProcess = writeAsync(data, status, jmsMetric)
    }

    val isActive
        get() = asyncProcess.isActive

    override fun close() = runBlocking {
        log.info { "Closing" }
        asyncProcess.cancelAndJoin()
        data.close()
        log.info { "Closed" }
    }

    private fun writeAsync(
            data: ReceiveChannel<V>,
            status: SendChannel<Status>,
            jmsMetric: SendChannel<JMSMetric>) = async {

        try {
            val connection = jmsProperties.connFactory.createConnection(jmsProperties.username, jmsProperties.password)
                    .apply { this.start() }

            val session = connection?.createSession(false, Session.AUTO_ACKNOWLEDGE)
                    ?: throw IllegalStateException("Cannot create session!")

            val producer = session.createProducer(session.createQueue(jmsProperties.queueName))

            connection.use { _ ->

                var allGood = true
                status.send(Ready)

                log.info("@start of writeAsync")

                // receive data, send to jms, and tell pipeline to commit
                while (isActive && allGood) {

                    data.receive().also { e ->
                        try {
                            log.info { "Received event from upstream" }

                            log.info { "Invoke transformation" }
                            val result = transform(session, e)

                            when(result.status) {
                                true -> {
                                    log.info { "Transformation to JMS TextMessage ok" }

                                    log.info { "Send TextMessage to JMS backend ${jmsProperties.queueName}" }
                                    producer.send(result.txtMsg)

                                    log.info { "Send to JMS completed" }
                                    jmsMetric.send(SentToJMS)

                                    log.info {"Send Ready to upstream"}
                                    status.send(Ready)

                                }
                                else -> {
                                    log.error("Transformation failure, indicate problem to upstream and " +
                                            "prepare for shutdown")
                                    allGood = false
                                    status.send(Problem)
                                }
                            }
                        }
                        catch (e: Exception) {
                            when (e) {
                                is JobCancellationException -> {/* it's ok to be cancelled by manager*/
                                }
                                else -> {

                                    // MessageFormatException, UnsupportedOperationException
                                    // InvalidDestinationException, JMSException
                                    log.error("Exception", e)
                                    log.error("Send Problem to upstream and prepare for shutdown")
                                    allGood = false
                                    status.send(Problem)
                                }
                            }
                        }
                    }
                }
            }
        }
        // JMSSecurityException, JMSException, ClosedReceiveChannelException
        catch (e: Exception) {
            when(e) {
                is JobCancellationException -> {/* it's ok to be cancelled by manager*/}
                else -> log.error("Exception", e)
            }
        }
        finally {
            // notify manager if this job is still active
            if (isActive && !status.isClosedForSend) {
                log.error("Report problem to upstream")
                status.send(Problem)
            }
        }
        log.info("@end of writeAsync - goodbye!")
    }

    // the transform is dependent on the session for creating correct JMS TextMessage
    protected abstract fun transform(session: Session, event: V): Result

    companion object {
        internal val log = KotlinLogging.logger {  }
    }
}
