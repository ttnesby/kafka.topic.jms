package no.nav.integrasjon.test.utils

import javax.jms.ConnectionFactory
import javax.jms.Session

/**
 * Quick class for embedded active mq
 * Using interface AutoCloseable in order to use .use in kotlin
 */
class EmbeddedActiveMQ(connFactory: ConnectionFactory, private val queueName: String) : AutoCloseable {

    private val conn = connFactory.createConnection().also {
        it.start()
    }
    private val session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val qSize
        get() = session.createBrowser(session.createQueue(queueName)).enumeration.toList().size

    override fun close() {
        conn.close()
    }
}
