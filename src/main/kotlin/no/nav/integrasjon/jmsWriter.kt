package no.nav.integrasjon

import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import javax.jms.*

fun jmsWriterAsync(
        connFactory: ConnectionFactory,
        queueName: String,
        data: ReceiveChannel<EventTransformed>,
        pipeline: SendChannel<CommitAction>,
        manager: SendChannel<ManagementStatus>) = async {

    try {
        connFactory.createConnection().use { c ->
            c.start()
            val s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)
            val p = s.createProducer(s.createQueue(queueName))

            var allGood = true

            // receive data, send to jms, and tell pipeline to commit
            while (isActive && allGood) {

                data.receive().also { e ->
                    try {
                        p.send(s.createTextMessage(e.value))
                        println("SENT to jms!")
                        pipeline.send(DoCommit)
                        println("SENT DoCommit!")
                    }
                    catch (e: Exception) {
                        // MessageFormatException, UnsupportedOperationException
                        // InvalidDestinationException, JMSException
                        allGood = false
                        pipeline.send(NoCommit)
                        println(e.stackTrace.toString())
                        println("SENT NoCommit!")
                    }
                }
            }
        }
    }
    catch (e: Exception) {} // JMSSecurityException, JMSException, ClosedReceiveChannelException

    // notify manager if this job is still active
    if (isActive) manager.send(Problem)
}