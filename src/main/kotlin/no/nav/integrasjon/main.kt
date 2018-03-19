package no.nav.integrasjon

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
import no.nav.integrasjon.jms.JMSDetails
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import javax.jms.ConnectionFactory


fun main(args: Array<String>) {


}

fun bootstrap() {

    // get kafka properties and set kafka client details
/*
    val kEnv = KafkaEnvironment(topics = listOf("aTopic"), withSchemaRegistry = true).apply {
        start()
    }
*/

/*    val kCDetailsAvro = KafkaClientDetails(
            Properties().apply {
                set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kEnv.brokersURL)
                set(ConsumerConfig.CLIENT_ID_CONFIG, "kafkaTopicConsumer")
                set("schema.registry.url",kEnv.serverPark.schemaregistry.url)
            },
            "aTopic"
    )*/

    // get fasit properties and make JMS connection factory with settings

    val jmsDetails = JMSDetails(
            ActiveMQConnectionFactory("vm://localhost?broker.persistent=false") as ConnectionFactory,
            "toDownstream"
    )

    // establish pipeline

/*
    val manager = ManagePipeline.init<String, GenericRecord>(
            kCDetailsAvro,
            ExternalAttachmentToJMS(
                    jmsDetails,
                    "src/test/resources/musicCatalog.xsl"))
            .manageAsync()
*/
    // iff pipeline is up and running, establish REST server

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

    var eRIsActive = true

    eREST.environment.monitor.subscribe(ApplicationStopping) { eRIsActive = false }

    runBlocking {

        println("Waiting for problem with kafka-topic-jms pipeline or NAIS REST server")
/*        while (manager.isActive && eRIsActive) delay(100)

        manager.cancelAndJoin()
        kEnv.tearDown()*/
        eREST.application.dispose()
    }
}