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

abstract class JMSTextMessageWriter<in V>(jmsDetails: JMSDetails) {

    private val connection = jmsDetails.connFactory.createConnection("app","").apply { this.start() }
    protected val session = connection?.createSession(false, Session.AUTO_ACKNOWLEDGE) ?:
            throw IllegalStateException("Cannot create session in JMSTextMessageWriter!")
    private val producer = session.createProducer(session.createQueue(jmsDetails.queueName))

    data class Result(val status: Boolean = false, val txtMsg: TextMessage)

    fun writeAsync(
            fromUpstream: ReceiveChannel<V>,
            toUpstream: SendChannel<Status>,
            toManager: SendChannel<Status>) = async {

        try {
            connection.use { _ ->

                var allGood = true
                toManager.send(Ready)

                log.info("@start of writeAsync")

                // receive fromUpstream, send to jms, and tell pipeline to commit
                while (isActive && allGood) {

                    fromUpstream.receive().also { e ->
                        try {
                            log.debug {"Received event: ${e.toString()}" }

                            val result = transform(e)

                            when(result.status) {
                                true -> {
                                    log.debug {"Transformation ok: ${result.txtMsg.text}" }
                                    producer.send(result.txtMsg)
                                    log.debug {"Sent and received on JMS" }
                                    toUpstream.send(Ready)
                                    log.debug {"Sent Ready to upstream"}
                                }
                                else -> {
                                    log.debug {"Transformation failure!" }
                                    allGood = false
                                    toUpstream.send(Problem)
                                    log.error("Sent Problem to upstream")
                                }
                            }
                        }
                        catch (e: Exception) {
                            // MessageFormatException, UnsupportedOperationException
                            // InvalidDestinationException, JMSException
                            allGood = false
                            toUpstream.send(Problem)
                            log.error("Exception", e)
                            log.error("Sent Problem to upstream")
                        }
                    }
                }
            }
        }
        catch (e: Exception) {
            when(e) {
                is CancellationException -> {/* it's ok*/}
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

    abstract fun transform(event: V): Result

    companion object {

        val log = KotlinLogging.logger {  }
    }
}
