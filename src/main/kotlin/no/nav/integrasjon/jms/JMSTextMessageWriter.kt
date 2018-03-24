package no.nav.integrasjon.jms

import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import mu.KotlinLogging
import no.nav.integrasjon.manager.Problem
import no.nav.integrasjon.manager.Ready
import no.nav.integrasjon.manager.Status
import javax.jms.*
import kotlin.IllegalStateException

abstract class JMSTextMessageWriter<V>(
        private val jmsProperties: JMSProperties,
        status: SendChannel<Status>
        ) : AutoCloseable {

    protected data class Result(val status: Boolean = false, val txtMsg: TextMessage)

    val data = Channel<V>()
    private val asyncProcess: Job

    init {
        log.info { "Starting" }
        asyncProcess = writeAsync(data, status)
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
            status: SendChannel<Status>) = async {

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
        // JMSSecurityException, JMSException, ClosedReceiveChannelException
        catch (e: Exception) {
            when(e) {
                is CancellationException -> {/* it's ok to be cancelled by manager*/}
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

    protected abstract fun transform(session: Session, event: V): Result

    companion object {
        internal val log = KotlinLogging.logger {  }
    }
}
