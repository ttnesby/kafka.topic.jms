package no.nav.integrasjon

import com.ibm.mq.jms.MQConnectionFactory
import com.ibm.msg.client.wmq.WMQConstants
import com.ibm.msg.client.wmq.compat.base.internal.MQC
import io.ktor.application.*
import io.ktor.http.ContentType
import io.ktor.response.respondText
import io.ktor.routing.get
import io.ktor.routing.routing
import io.ktor.server.engine.ShutDownUrl
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.experimental.cancelAndJoin
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import no.nav.integrasjon.jms.ExternalAttachmentToJMS
import no.nav.integrasjon.jms.JMSProperties
import no.nav.integrasjon.kafka.KafkaClientProperties
import no.nav.integrasjon.kafka.KafkaEvents
import no.nav.integrasjon.manager.ManagePipeline
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
                clientReconnectTimeout = 300 // reconnection attempts for 5 minutes
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

    // establish a pipeline of asynchronous coroutines communicating via channels
    // (upstream) listen to kafka topic and send event to downstream
    // (downstream) receive kafka events and write TextMessage to JMS backend

    val manager = ManagePipeline.init<String, GenericRecord>(
            kafkaProps,
            ExternalAttachmentToJMS(jmsProps, kafkaProps.kafkaEvent))

    // start the management process
    val mngmtProcess = manager.manageAsync()

    // give the creation of pipeline some slack
    runBlocking { delay(1_000) }

    // leave if there are problems
    if (!manager.isOk) return

    // establish asynchronous REST process
    val eREST = embeddedServer(Netty, 8080) {
        install(ShutDownUrl.ApplicationCallFeature) {
            shutDownUrl = "/redbutton/press"
            exitCodeSupplier = { 0 }
        }
        routing {
            get("/isAlive") {
                call.respondText("kafkatopic2jms is alive", ContentType.Text.Plain)
            }
            get("/isReady") {
                call.respondText("kafkatopic2jms is ready", ContentType.Text.Plain)
            }
        }
    }.start(wait = false)

    // define a flag and subscribe to the REST shutdown option
    var eRIsActive = true
    eREST.environment.monitor.subscribe(ApplicationStopping) { eRIsActive = false }

    runBlocking {

        while (manager.isOk && eRIsActive) delay(1_000)

        mngmtProcess.cancelAndJoin()
        eREST.application.dispose()
    }
}