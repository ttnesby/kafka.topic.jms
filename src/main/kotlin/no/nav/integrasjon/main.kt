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
import no.nav.common.KafkaEnvironment
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.io.StringReader
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import javax.jms.ConnectionFactory
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamReader
import javax.xml.stream.events.XMLEvent

fun getElems(xmlFile: String, elems: Map<String, Int>): List<String> {

    val xmlReader = XMLInputFactory.newFactory().createXMLStreamReader(StringReader(xmlFile))

    var strBuilder = StringBuilder()

    tailrec fun iterCDATA(xmlReader: XMLStreamReader): String =
            if (!xmlReader.hasNext())
                ""
            else {
                xmlReader.next()
                if (xmlReader.eventType == XMLEvent.END_ELEMENT)
                    strBuilder.toString().trim().dropLast(3)
                else {
                    strBuilder.append(xmlReader.text)
                    iterCDATA(xmlReader)
                }
            }

    tailrec fun iterElem(xmlReader: XMLStreamReader, elem: String, type: Int): String =

            if (!xmlReader.hasNext())
                ""
            else {
                xmlReader.next()

                if (xmlReader.eventType == XMLEvent.START_ELEMENT && xmlReader.localName == elem)
                    if (type == XMLEvent.START_ELEMENT) {
                        xmlReader.next()
                        xmlReader.text
                    }
                    else iterCDATA(xmlReader)

                else
                    iterElem(xmlReader, elem, type)
            }

    return elems.map { iterElem(xmlReader,it.key,it.value) }
}

fun main(args: Array<String>) {

    val xmlFile = String(
            Files.readAllBytes(Paths.get("src/test/resources/oppf_2913_04.xml")),
            StandardCharsets.UTF_8
    )

    val values = getElems(
            xmlFile,
            mapOf(
                    "ServiceCode" to XMLEvent.START_ELEMENT,
                    "Reference" to XMLEvent.START_ELEMENT,
                    "FormData" to XMLEvent.CDATA)
    )

    println(getElems(values[2], mapOf("orgnr" to XMLEvent.START_ELEMENT)).first())


}

fun bootstrap() {

    val kEnv = KafkaEnvironment(topics = listOf("aTopic"), withSchemaRegistry = true).apply {
        start()
    }

    val kCDetailsAvro = KafkaClientDetails(
            Properties().apply {
                set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kEnv.brokersURL)
                set(ConsumerConfig.CLIENT_ID_CONFIG, "kafkaTopicConsumer")
                set("schema.registry.url",kEnv.serverPark.schemaregistry.url)
            },
            "aTopic"
    )

    val jmsDetails = JMSDetails(
            ActiveMQConnectionFactory("vm://localhost?broker.persistent=false") as ConnectionFactory,
            "kafkaEvents"
    )


    val manager = ManagePipeline.init<String, GenericRecord>(
            kCDetailsAvro,
            ExternalAttchmentToJMS(
                    jmsDetails,
                    "src/test/resources/musicCatalog.xsl"))
            .manageAsync()


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
        while (manager.isActive && eRIsActive) delay(100)

        manager.cancelAndJoin()
        kEnv.tearDown()
    }
}