package no.nav.integrasjon

import com.ibm.mq.jms.MQConnectionFactory
import com.ibm.msg.client.wmq.WMQConstants
import com.ibm.msg.client.wmq.compat.base.internal.MQC
import io.ktor.application.*
import io.ktor.http.ContentType
import io.ktor.response.respondText
import io.ktor.routing.get
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import mu.KotlinLogging
import no.nav.integrasjon.jms.ExternalAttachmentToJMS
import no.nav.integrasjon.jms.JMSProperties
import no.nav.integrasjon.kafka.KafkaClientProperties
import no.nav.integrasjon.kafka.KafkaEvents
import no.nav.integrasjon.kafka.KafkaTopicConsumer
import no.nav.integrasjon.manager.Problem
import no.nav.integrasjon.manager.Status
import org.apache.avro.generic.GenericRecord
import java.util.*


fun main(args: Array<String>) {

    val fp = FasitProperties()

    if (fp.isEmpty) throw IllegalStateException("Missing fasit properties - $fp")

    val prodEvents = KafkaEvents.values().filter { it.value.production }

    if (fp.kafkaEvent !in prodEvents.map { it.value.name } )
        throw IllegalStateException("Incorrect kafka event as fasit property, ${fp.kafkaEvent} NOT IN " +
                "${prodEvents.map { it.value.name }}")

    // TODO - how to get the kafka brokers and schema reg?
    val kafkaProps = KafkaClientProperties(
            Properties(), // brokers, schema reg, client id
            KafkaEvents.valueOf(fp.kafkaEvent)
    )

    // set relevant jms properties
    val jmsProps = JMSProperties(
            MQConnectionFactory().apply {
                hostName = fp.mqHostname
                port = fp.mqPort
                queueManager = fp.mqQueueManagerName
                channel = fp.mqChannel
                transportType = WMQConstants.WMQ_CM_CLIENT
                clientReconnectOptions = WMQConstants.WMQ_CLIENT_RECONNECT // will try to reconnect
                clientReconnectTimeout = 600 // reconnection attempts for 10 minutes
                ccsid = 1208
                setIntProperty(WMQConstants.JMS_IBM_ENCODING, MQC.MQENC_NATIVE)
                setIntProperty(WMQConstants.JMS_IBM_CHARACTER_SET, 1208)
            },
            fp.outputQueueName,
            fp.mqUsername,
            fp.mqPassword
    )

    bootstrap(kafkaProps, jmsProps)
}

fun bootstrap(kafkaProps: KafkaClientProperties, jmsProps: JMSProperties) {

    val log = KotlinLogging.logger {  }

    log.info { "@start of bootstrap" }
    log.info { "Starting pipeline" }

    // establish a pipeline of asynchronous coroutines communicating via channels
    // (upstream) listen to kafka topic and send event to downstream
    // (downstream) receive kafka events and write TextMessage to JMS backend

    val status = Channel<Status>()

    try {
        runBlocking {

            ExternalAttachmentToJMS(jmsProps, status, kafkaProps.kafkaEvent).use { jms ->

                if (status.receive() == Problem) return@use

                KafkaTopicConsumer.init<String, GenericRecord>(kafkaProps, jms.data, status).use { consumer ->

                    log.info { "Starting embedded REST server" }

                    // establish asynchronous REST process
                    val eREST = embeddedServer(Netty, 8080) {
                        routing {
                            get("/isAlive") {
                                call.respondText("kafkatopic2jms is alive", ContentType.Text.Plain)
                            }
                            get("/isReady") {
                                call.respondText("kafkatopic2jms is ready", ContentType.Text.Plain)
                            }
                        }
                    }.start(wait = false)

                    while (jms.isActive && consumer.isActive) delay(67)
                }
            }
        }

    }
    catch (e: Exception) {
        log.error("Exception", e)
    }
    finally {
        status.close()
        log.info { "@end of bootstrap" }
    }
}