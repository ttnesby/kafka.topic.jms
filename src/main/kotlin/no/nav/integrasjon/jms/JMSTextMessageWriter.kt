package no.nav.integrasjon.jms

import kotlinx.coroutines.experimental.CancellationException
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import mu.KotlinLogging
import no.nav.integrasjon.manager.Problem
import no.nav.integrasjon.manager.Ready
import no.nav.integrasjon.manager.Status
import javax.jms.*
import kotlin.IllegalStateException

abstract class JMSTextMessageWriter<in V>(private val jmsProperties: JMSProperties) {

    data class Result(val status: Boolean = false, val txtMsg: TextMessage)

    fun writeAsync(
            fromUpstream: ReceiveChannel<V>,
            toUpstream: SendChannel<Status>,
            toManager: SendChannel<Status>) = async {

        try {
            // doing this now, in case of issues, catch by error handling
            var allGood = true
            toManager.send(Ready)


            val connection = jmsProperties.connFactory.createConnection(jmsProperties.username, jmsProperties.password)
                    .apply { this.start() }

            val session = connection?.createSession(false, Session.AUTO_ACKNOWLEDGE)
                    ?: throw IllegalStateException("Cannot create session!")

            val producer = session.createProducer(session.createQueue(jmsProperties.queueName))

            connection.use { _ ->

                log.info("@start of writeAsync")

                // receive fromUpstream, send to jms, and tell pipeline to commit
                while (isActive && allGood) {

                    fromUpstream.receive().also { e ->
                        try {
                            log.info { "Received event from upstream" }

                            log.info { "Invoke transformation" }
                            val result = transform(session, e)

                            when(result.status) {
                                true -> {
                                    log.info { "Transformation to JMS TextMessage ok" }

                                    log.info { "Send TextMessage to JMS backend ${jmsProperties.queueName}" }
                                    producer.send(result.txtMsg)

                                    log.info { "Send to JMS completed" }

                                    log.info {"Send Ready to upstream"}
                                    toUpstream.send(Ready)

                                }
                                else -> {
                                    log.error {"Transformation failure, indicate problem to upstream and " +
                                            "prepare for shutdown" }
                                    allGood = false
                                    toUpstream.send(Problem)
                                }
                            }
                        }
                        catch (e: Exception) {
                            // MessageFormatException, UnsupportedOperationException
                            // InvalidDestinationException, JMSException
                            log.error("Exception", e)
                            log.error("Send Problem to upstream and prepare for shutdown")
                            allGood = false
                            toUpstream.send(Problem)
                        }
                    }
                }
            }
        }
        catch (e: Exception) {
            when(e) {
                is CancellationException -> {/* it's ok to be cancelled by manager*/}
                else -> log.error("Exception", e)
            }
        } // JMSSecurityException, JMSException, ClosedReceiveChannelException

        // notify manager if this job is still active
        if (isActive && !toManager.isClosedForSend) {
            toManager.send(Problem)
            log.error("Reported problem to manager")
        }

        log.info("@end of writeAsync - goodbye!")
    }

    abstract fun transform(session: Session, event: V): Result

    companion object {

        val log = KotlinLogging.logger {  }
    }
}
