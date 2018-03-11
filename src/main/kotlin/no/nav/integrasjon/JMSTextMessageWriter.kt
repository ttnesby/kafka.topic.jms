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
                            producer.send(transform(e))
                            log.debug {"SENT to jms!" }
                            commitAction.send(DoCommit)
                            log.debug {"SENT DoCommit!"}
                        }
                        catch (e: Exception) {
                            // MessageFormatException, UnsupportedOperationException
                            // InvalidDestinationException, JMSException
                            allGood = false
                            commitAction.send(NoCommit)
                            log.error("Exception", e)
                            log.error("SENT NoCommit!")
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
        if (isActive && !status.isClosedForSend) status.send(Problem)

        log.info("@end of writeAsync - goodbye!")
    }

    abstract fun transform(event: V): TextMessage

    companion object {
        private val log = KotlinLogging.logger {  }
    }
}
