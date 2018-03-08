package no.nav.integrasjon.test.utils

import javax.jms.ConnectionFactory
import javax.jms.Session

class EmbeddedActiveMQ(connFactory: ConnectionFactory, private val queueName: String) {

    private val conn = connFactory.createConnection().also {
        it.start()
    }
    private val session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val qSize
        get() = session.createBrowser(session.createQueue(queueName)).enumeration.toList().size
}
