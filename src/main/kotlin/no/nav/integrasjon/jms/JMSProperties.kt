package no.nav.integrasjon.jms

import javax.jms.ConnectionFactory

data class JMSProperties(
        val connFactory: ConnectionFactory,
        val queueName: String,
        val username: String,
        val password: String
)