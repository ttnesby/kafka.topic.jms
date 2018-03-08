package no.nav.integrasjon

import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import javax.jms.ConnectionFactory
import javax.jms.Session

fun jmsWriterAsync(
        connFactory: ConnectionFactory,
        queueName: String,
        eventTrfd: ReceiveChannel<EventTransformed>,
        kCommit: SendChannel<CommitAction>) = async {

    connFactory.createConnection().use { c->
        c.start()
        c.createSession(false, Session.AUTO_ACKNOWLEDGE).use { s ->
            s.createProducer(s.createQueue(queueName)).use { p ->

                while (isActive) {
                    // receive transformed event, send to jms, and tell kafka listener to commit
                    eventTrfd.receive().also { e ->
                        p.send(s.createTextMessage(e.value))
                        kCommit.send(DoCommit)
                    }
                }
            }
        }
    }
}