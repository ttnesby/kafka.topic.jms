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

fun main(args: Array<String>) {

    fun getElems(xmlFile: String, elems: Map<String, Int>): List<String> {

        val xmlReader = XMLInputFactory.newFactory().createXMLStreamReader(
                StringReader(String(Files.readAllBytes(Paths.get(xmlFile)),StandardCharsets.UTF_8)))

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

    println(
            getElems(
                    "src/test/resources/oppf_2913_04.xml",
                    mapOf(
                            "ServiceCode" to XMLEvent.START_ELEMENT,
                            "Reference" to XMLEvent.START_ELEMENT,
                            "FormData" to XMLEvent.CDATA)
                    )
    )
    

/*
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

    val bytes2 = Files.readAllBytes(Paths.get("src/test/resources/oppf_2913_04_inner.xml"))
    //val toStr2 = String(bytes2,StandardCharsets.UTF_8)

    val reader2 = StringReader(String(bytes2,StandardCharsets.UTF_8))
    val xmlReader2 = XMLInputFactory.newFactory().createXMLStreamReader(reader2)

    var orgCode: String = ""

    xmlReader2.apply {

        while (hasNext() && orgCode.isEmpty()) {

            next()

            when (eventType) {
                XMLEvent.START_ELEMENT -> when (localName) {
                    "orgnr" -> {
                        next()
                        orgCode = text
                        next()
                    }
                }
            }
        }

    }.also { it.close() }
*/
/*    while (xmlReader2.hasNext() && orgCode.isEmpty()) {

        xmlReader2.next()

        when (xmlReader2.eventType) {
            XMLEvent.START_ELEMENT -> when (xmlReader2.localName) {
                "orgnr" -> {
                    xmlReader2.next()
                    orgCode = xmlReader2.text
                    xmlReader2.next()
                }
            }
        }
    }

    xmlReader2.close()*/
    //println(orgCode)


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