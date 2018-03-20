package no.nav.integrasjon.test.utils

import no.nav.integrasjon.jms.JMSProperties
import javax.jms.Session

/**
 * Class for embedded active mq
 * Using interface AutoCloseable enabling .use in kotlin
 */
class EmbeddedActiveMQ(val jmsProperties: JMSProperties) : AutoCloseable {

    private val conn = jmsProperties.connFactory.createConnection().also {
        it.start()
    }
    private val session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val queue
        get() = session.createBrowser(session.createQueue(jmsProperties.queueName)).enumeration.toList()

    override fun close() {
        conn.close()
    }
}
