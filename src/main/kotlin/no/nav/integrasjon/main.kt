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
import org.redundent.kotlin.xml.xml
import java.io.File
import java.io.StringReader
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.UUID.randomUUID
import javax.jms.ConnectionFactory
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.events.XMLEvent

fun main(args: Array<String>) {

    val bytes = Files.readAllBytes(Paths.get("src/test/resources/oppf_2913_04.xml"))
    val toStr = String(bytes,StandardCharsets.UTF_8)

    val reader = StringReader(toStr)
    val xmlReader = XMLInputFactory.newFactory().createXMLStreamReader(reader)

    var serviceCode: String = ""
    var reference: String = ""
    var formData: String = ""
    var strBuilder = StringBuilder()

    while (xmlReader.hasNext() && (serviceCode.isEmpty() || reference.isEmpty() || formData.isEmpty())) {

        xmlReader.next()

        when (xmlReader.eventType) {
            XMLEvent.START_ELEMENT -> when (xmlReader.localName) {
                "ServiceCode" -> {
                    xmlReader.next()
                    serviceCode = xmlReader.text
                    xmlReader.next()
                }
                "Reference" -> {
                    xmlReader.next()
                    reference = xmlReader.text
                    xmlReader.next()
                }
                "FormData" -> {
                    while (xmlReader.hasNext() && formData.isEmpty()) {
                        xmlReader.next()
                        when (xmlReader.eventType) {
                            XMLEvent.CHARACTERS -> strBuilder.append(xmlReader.text)
                            XMLEvent.CDATA -> strBuilder.append(xmlReader.text)
                            XMLEvent.END_ELEMENT -> {
                                formData = strBuilder.toString().trim().dropLast(3)
                            }
                        }
                    }
                    xmlReader.next()
                }
            }
        }
    }

    xmlReader.close()

    println(serviceCode)
    println(reference)
    println(formData)

    val cdt = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"))

    val eiFellesFormat = xml("EI_fellesformat",prettyFormat = true) {
        xmlns="http://www.nav.no/xml/eiff/2/"
        namespace("xsi","http://www.w3.org/2001/XMLSchema-instance")

        "NavInnDokument" {
            xmlns="http://www.nav.no/xml/NavDokument/1/"

            "DokumentInfo" {
                "dokumentType" {
                    -serviceCode
                }
                "dokumentreferanse" {
                    -reference
                }
                "dokumentDato" {
                    -cdt
                }
            }
            "Avsender" {
                "id" {
                    "idNr" {
                        -"tbd"
                    }
                    "idType" {
                        -"ENH"
                    }
                }
            }
            "Godkjenner" {}
            "Innhold" {}
        }
        "MottakenhetBlokk" {
            attribute("name","ediLoggId")
            -randomUUID().toString()
        }
    }

    println(eiFellesFormat.toString())


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