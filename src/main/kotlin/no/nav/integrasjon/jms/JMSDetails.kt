package no.nav.integrasjon.jms

import javax.jms.ConnectionFactory

data class JMSDetails(
        val connFactory: ConnectionFactory,
        val queueName: String,
        val username: String = "app",
        val password: String = ""
)