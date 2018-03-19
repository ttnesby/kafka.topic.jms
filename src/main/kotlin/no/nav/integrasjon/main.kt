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


    val xmlFile = String(
            Files.readAllBytes(Paths.get("src/test/resources/musicCatalog.html")),
            StandardCharsets.UTF_8
    )

    fun xmlOneliner(xml: String): String {

        var betweeTags = false
        val str = StringBuilder()


        tailrec fun iter(i: CharIterator): String = if (!i.hasNext()) str.toString() else {
            val c = i.nextChar()

            when(c) {
                '<' -> {
                    betweeTags = false
                    str.append(c)
                }
                '>' -> {
                    betweeTags = true
                    str.append(c)
                }
                ' ' -> if (!betweeTags) str.append(c)
                '\n' -> if (!betweeTags) str.append(c)
                else -> str.append(c)
            }
            iter(i)
        }

        return iter(xml.iterator())
    }

    println(xmlOneliner(xmlFile))

    val xmlFile2 = String(
            Files.readAllBytes(Paths.get("src/test/resources/oppfolging_2913_04.xml")),
            StandardCharsets.UTF_8
    )

    println(xmlOneliner(xmlFile2))


}

fun bootstrap() {

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

    val jmsDetails = JMSDetails(
            ActiveMQConnectionFactory("vm://localhost?broker.persistent=false") as ConnectionFactory,
            "toDownstream"
    )


/*
    val manager = ManagePipeline.init<String, GenericRecord>(
            kCDetailsAvro,
            ExternalAttachmentToJMS(
                    jmsDetails,
                    "src/test/resources/musicCatalog.xsl"))
            .manageAsync()
*/


    val eREST = embeddedServer(Netty, 8080) {
        install(ShutDownUrl.ApplicationCallFeature) {
            shutDownUrl = "/shutdown"
            exitCodeSupplier = { 0 }
        }
        routing {
            get("/isAlive") {
                call.respondText("Application is alive", ContentType.Text.Plain)
            }
            get("/isReady") {
                call.respondText("Application is ready", ContentType.Text.Plain)
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
    }
}