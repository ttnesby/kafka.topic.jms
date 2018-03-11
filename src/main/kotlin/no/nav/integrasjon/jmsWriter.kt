package no.nav.integrasjon

import kotlinx.coroutines.experimental.CancellationException
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import mu.KotlinLogging
import javax.jms.*

data class JMSDetails(
        val connFactory: ConnectionFactory,
        val queueName: String
)

fun <T>jmsWriterAsync(
        jmsDetails: JMSDetails,
        data: ReceiveChannel<T>,
        toText: (T) -> String,
        commitAction: SendChannel<CommitAction>,
        status: SendChannel<Status>) = async {

    val log = KotlinLogging.logger {  }

    try {
        jmsDetails.connFactory.createConnection().use { c ->
            c.start()
            val s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)
            val p = s.createProducer(s.createQueue(jmsDetails.queueName))

            var allGood = true
            status.send(Ready)

            // receive data, send to jms, and tell pipeline to commit
            while (isActive && allGood) {

                data.receive().also { e ->
                    try {
                        p.send(s.createTextMessage(toText(e)))
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
}