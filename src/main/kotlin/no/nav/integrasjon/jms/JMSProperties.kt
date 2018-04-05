package no.nav.integrasjon.jms

import javax.jms.ConnectionFactory

/**
 * JMSProperties is a data class hosting the minimum set of properties for instanciating a JMS connection
 * @property connFactory the specific connection factory to use (active MQ, ibm MQ, ...)
 * @property queueName which queue for sending TextMessages to
 * @property username eventual username for log on
 * @property password eventual password for log on
 */
data class JMSProperties(
        val connFactory: ConnectionFactory,
        val queueName: String,
        val username: String,
        val password: String
)