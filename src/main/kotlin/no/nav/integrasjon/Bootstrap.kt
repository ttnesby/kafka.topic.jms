package no.nav.integrasjon

import io.ktor.application.call
import io.ktor.application.install
import io.ktor.http.ContentType
import io.ktor.response.respondText
import io.ktor.routing.Routing
import io.ktor.routing.get
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import mu.KotlinLogging
import no.nav.integrasjon.jms.ExternalAttachmentToJMS
import no.nav.integrasjon.jms.JMSProperties
import no.nav.integrasjon.kafka.KafkaClientProperties
import no.nav.integrasjon.kafka.KafkaTopicConsumer
import no.nav.integrasjon.manager.Problem
import no.nav.integrasjon.manager.Status
import org.apache.avro.generic.GenericRecord
import java.util.concurrent.TimeUnit

object Bootstrap {

    private val log = KotlinLogging.logger {  }

    @Volatile var shutdownhookActive = false
    @Volatile var mainThread: Thread = Thread.currentThread()

    init {
        log.info { "Installing shutdown hook" }
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                shutdownhookActive = true
                mainThread.join()
            }
        })
    }

    fun invoke(kafkaProps: KafkaClientProperties, jmsProps: JMSProperties) {

        log.info { "@start of bootstrap" }
        log.info { "Starting pipeline" }

        // establish a pipeline of asynchronous coroutines communicating via channels
        // upstream - listen to kafka topic and send event to downstream
        // downstream - receive kafka events and write TextMessage to JMS backend

        val status = Channel<Status>()

        log.info { "Starting embedded REST server" }
        val eREST = embeddedServer(Netty, 8080){}.start()

        try {
            runBlocking {

                ExternalAttachmentToJMS(jmsProps, status, kafkaProps.kafkaEvent).use { jms ->

                    if (status.receive() == Problem) return@use

                    KafkaTopicConsumer.init<String, GenericRecord>(kafkaProps, jms.data, status).use { consumer ->

                        log.info { "Installing /isAlive and /isReady routes" }
                        eREST.application.install(Routing) {
                            get("/isAlive") {
                                call.respondText("kafkatopic2jms is alive", ContentType.Text.Plain)
                            }
                            get("/isReady") {
                                call.respondText("kafkatopic2jms is ready", ContentType.Text.Plain)
                            }
                        }
                        log.info { "/isAlive and /isReady routes are available" }

                        while (jms.isActive && consumer.isActive && !shutdownhookActive) delay(67)

                        if (shutdownhookActive) log.info { "Shutdown hook actived - preparing shutdown" }
                    }
                }
            }

        }
        catch (e: Exception) {
            log.error("Exception", e)
        }
        finally {
            eREST.stop(100,100, TimeUnit.MILLISECONDS)
            status.close()
            log.info { "@end of bootstrap" }
        }
    }
}