package no.nav.integrasjon

import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import mu.KotlinLogging
import javax.jms.*

fun <T>jmsWriterAsync(
        connFactory: ConnectionFactory,
        queueName: String,
        data: ReceiveChannel<T>,
        toText: (T) -> String,
        commitAction: SendChannel<CommitAction>,
        status: SendChannel<Status>) = async {

    val log = KotlinLogging.logger {  }

    try {
        connFactory.createConnection().use { c ->
            c.start()
            val s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)
            val p = s.createProducer(s.createQueue(queueName))

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
                        log.error(e.stackTrace.toString())
                        log.error("SENT NoCommit!")
                    }
                }
            }
        }
    }
    catch (e: Exception) {
        log.error(e.stackTrace.toString())
    } // JMSSecurityException, JMSException, ClosedReceiveChannelException

    // notify manager if this job is still active
    if (isActive) status.send(Problem)
}