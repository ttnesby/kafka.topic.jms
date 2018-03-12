package no.nav.integrasjon

import kotlinx.coroutines.experimental.CancellationException
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import mu.KotlinLogging
import javax.jms.*
import kotlin.IllegalStateException

data class JMSDetails(
        val connFactory: ConnectionFactory,
        val queueName: String
)

abstract class JMSTextMessageWriter<in V>(jmsDetails: JMSDetails) {

    private val connection = jmsDetails.connFactory.createConnection().apply { this.start() }
    protected val session = connection?.createSession(false, Session.AUTO_ACKNOWLEDGE) ?:
            throw IllegalStateException("Cannot create session in JMSTextMessageWriter!")
    private val producer = session.createProducer(session.createQueue(jmsDetails.queueName))

    data class Result(val status: Boolean = false, val txtMsg: TextMessage)

    fun writeAsync(
            data: ReceiveChannel<V>,
            commitAction: SendChannel<CommitAction>,
            status: SendChannel<Status>) = async {

        try {
            connection.use { _ ->

                var allGood = true
                status.send(Ready)

                log.info("@start of writeAsync")

                // receive data, send to jms, and tell pipeline to commit
                while (isActive && allGood) {

                    data.receive().also { e ->
                        try {
                            log.debug {"Received event: ${e.toString()}" }

                            val result = transform(e)

                            when(result.status) {
                                true -> {
                                    log.debug {"Transformation ok: ${result.txtMsg}" }
                                    producer.send(result.txtMsg)
                                    log.debug {"Sent and received on JMS" }
                                    commitAction.send(DoCommit)
                                    log.debug {"Sent DoCommit to pipeline"}
                                }
                                else -> {
                                    log.debug {"Transformation failure!" }
                                    allGood = false
                                    commitAction.send(NoCommit)
                                    log.error("Sent NoCommit to pipeline")
                                }
                            }
                        }
                        catch (e: Exception) {
                            // MessageFormatException, UnsupportedOperationException
                            // InvalidDestinationException, JMSException
                            allGood = false
                            commitAction.send(NoCommit)
                            log.error("Exception", e)
                            log.error("Sent NoCommit to pipeline")
                        }
                    }
                }
            }
        }
        catch (e: Exception) {
            when(e) {
                is CancellationException -> {/* it's ok*/}
                else -> log.error("Exception", e)
            }
        } // JMSSecurityException, JMSException, ClosedReceiveChannelException

        // notify manager if this job is still active
        if (isActive && !status.isClosedForSend) {
            status.send(Problem)
            log.error("Reported problem to manager")
        }

        log.info("@end of writeAsync - goodbye!")
    }

    abstract fun transform(event: V): Result

    companion object {
        val log = KotlinLogging.logger {  }
    }
}
