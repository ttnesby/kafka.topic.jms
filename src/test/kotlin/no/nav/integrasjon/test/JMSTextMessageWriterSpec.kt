package no.nav.integrasjon.test

import kotlinx.coroutines.experimental.cancelAndJoin
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.withTimeoutOrNull
import mu.KotlinLogging
import no.nav.integrasjon.jms.ExternalAttachmentToJMS
import no.nav.integrasjon.jms.JMSDetails
import no.nav.integrasjon.jms.JMSTextMessageWriter
import no.nav.integrasjon.manager.Channels
import no.nav.integrasjon.test.utils.EmbeddedActiveMQ
import no.nav.integrasjon.test.utils.getFileAsString
import no.nav.integrasjon.test.utils.xmlOneliner
import org.amshove.kluent.shouldContainAll
import org.amshove.kluent.shouldEqualTo
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.context
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import java.io.File
import javax.jms.TextMessage

object JMSTextMessageWriterSpec : Spek({

    val log = KotlinLogging.logger {  }

    val jmsDetails = JMSDetails(
            ActiveMQConnectionFactory("vm://localhost?broker.persistent=false"),
            "toDownstream"
    )

    class TrfString : JMSTextMessageWriter<String>(jmsDetails) {
        override fun transform(event: String): Result =
                Result(
                        status = true,
                        txtMsg = session.createTextMessage().apply { this.text = event.toUpperCase() }
                )
    }

    class TrfInt : JMSTextMessageWriter<Int>(jmsDetails) {
        override fun transform(event: Int): Result =
                Result(
                        status = true,
                        txtMsg = session.createTextMessage().apply { this.text = (event * event).toString() }
                )
    }

    class TrfAvro : JMSTextMessageWriter<GenericRecord>(jmsDetails) {
        override fun transform(event: GenericRecord): Result =
                Result(
                        status = true,
                        txtMsg = session.createTextMessage().apply { this.text = event.toString() }
                )
    }

    val dataStr = (1..100).map {"data-$it"}
    val dataInt = (1..100).map { it }

    val schema = Schema.Parser().parse(File("src/main/resources/external_attachment.avsc"))

    val dataAvro = (1..100).map {
        GenericData.Record(schema).apply {
            put("batch","batch-$it")
            put("sc","sc-$it")
            put("sec","sec-$it")
            put("archRef","archRef-$it")
        }
    }
    val dataMusic = (1..100).map {
        GenericData.Record(schema).apply {
            put("batch", getFileAsString("src/test/resources/musicCatalog.xml"))
            put("sc","TESTONLY") // must be hard coded in order to be accepted by ExternalattachmentToJMS::transform
            put("sec","sec-$it")
            put("archRef","archRef-$it")
        }
    }

    val dataEia = mutableListOf<GenericRecord>(
            GenericData.Record(schema).apply {
                put("batch", getFileAsString("src/test/resources/oppfolging_2913_02.xml"))
                put("sc","2913")
                put("sec","2")
                put("archRef","test")
            },
            GenericData.Record(schema).apply {
                put("batch", getFileAsString("src/test/resources/oppfolging_2913_03.xml"))
                put("sc","2913")
                put("sec","3")
                put("archRef","test")
            },
            GenericData.Record(schema).apply {
                put("batch", getFileAsString("src/test/resources/oppfolging_2913_04.xml"))
                put("sc","2913")
                put("sec","4")
                put("archRef","test")
            },
            GenericData.Record(schema).apply {
                put("batch", getFileAsString("src/test/resources/oppfolging_navoppfplan_rapportering_sykemeldte.xml"))
                put("sc","NavOppfPlan")
                put("sec","rapportering_sykemeldte")
                put("archRef","test")
            },
            GenericData.Record(schema).apply {
                put("batch", getFileAsString("src/test/resources/bankkontonummer_2896_87.xml"))
                put("sc","2896")
                put("sec","87")
                put("archRef","test")
            }
    )

    val dataOther = mutableListOf<GenericRecord>(
            GenericData.Record(schema).apply {
                put("batch", getFileAsString("src/test/resources/maalekort_4711_01.xml"))
                put("sc","4711")
                put("sec","1")
                put("archRef","test")
            },
            GenericData.Record(schema).apply {
                put("batch", getFileAsString("src/test/resources/barnehageliste_4795_01.xml"))
                put("sc","4795")
                put("sec","1")
                put("archRef","test")
            }
    )

    val waitPatience = 100L
    val patienceLimit = 7_000L


    describe("JMSTextMessageWriter tests") {

        context("string elements, transform and send to jms") {

            it("should receive ${dataStr.size} string elements, transformed to uppercase") {

                val channels = Channels<String>(1)
                val jms = TrfString().writeAsync(channels.toDownstream,channels.fromDownstream,channels.toManager)


                runBlocking {

                    EmbeddedActiveMQ(jmsDetails).use { eMQ ->

                        withTimeoutOrNull(patienceLimit) {
                            dataStr.forEach {
                                channels.toDownstream.send(it)
                                channels.fromDownstream.receive() //receive ack so the jms will continue to receive
                            }
                        }

                        jms.cancelAndJoin()
                        channels.close()

                        eMQ.queue.map { (it as TextMessage).text }
                    }
                } shouldContainAll dataStr.map { it.toUpperCase() }
            }

            it("should receive ${dataInt.size} int elements, transformed to square") {

                val channels = Channels<Int>(1)
                val jms = TrfInt().writeAsync(channels.toDownstream,channels.fromDownstream,channels.toManager)


                runBlocking {

                    EmbeddedActiveMQ(jmsDetails).use { eMQ ->

                        withTimeoutOrNull(patienceLimit) {
                            dataInt.forEach {
                                channels.toDownstream.send(it)
                                channels.fromDownstream.receive() //receive ack so the jms will continue to receive
                            }
                        }

                        jms.cancelAndJoin()
                        channels.close()

                        eMQ.queue.map { (it as TextMessage).text }
                    }
                } shouldContainAll dataInt.map { (it*it).toString() }
            }

            it("should receive ${dataAvro.size} avro elements, transformed to square") {

                val channels = Channels<GenericRecord>(1)
                val jms = TrfAvro().writeAsync(channels.toDownstream,channels.fromDownstream,channels.toManager)


                runBlocking {

                    EmbeddedActiveMQ(jmsDetails).use { eMQ ->

                        withTimeoutOrNull(patienceLimit) {
                            dataAvro.forEach {
                                channels.toDownstream.send(it)
                                channels.fromDownstream.receive() //receive ack so the jms will continue to receive
                            }
                        }

                        jms.cancelAndJoin()
                        channels.close()

                        eMQ.queue.map { (it as TextMessage).text }
                    }
                } shouldContainAll dataAvro.map { it.toString() }
            }

            it("should receive ${dataMusic.size} avro elements, transformed to html") {

                val channels = Channels<GenericRecord>(1)
                val jms = ExternalAttachmentToJMS(jmsDetails, "src/test/resources/musicCatalog.xsl" )
                        .writeAsync(channels.toDownstream,channels.fromDownstream,channels.toManager)

                runBlocking {

                    EmbeddedActiveMQ(jmsDetails).use { eMQ ->

                        withTimeoutOrNull(patienceLimit) {
                            dataMusic.forEach {
                                channels.toDownstream.send(it)
                                channels.fromDownstream.receive() //receive ack so the jms will continue to receive
                            }
                        }

                        jms.cancelAndJoin()
                        channels.close()

                        eMQ.queue.map { xmlOneliner((it as TextMessage).text).hashCode() }
                    }
                } shouldContainAll dataMusic.map {
                    xmlOneliner(getFileAsString("src/test/resources/musicCatalog.html")).hashCode()
                }
            }

            it("should receive ${dataEia.size} avro elements, transformed to xml") {

                val channels = Channels<GenericRecord>(1)
                val jms = ExternalAttachmentToJMS(
                        jmsDetails,
                        "src/main/resources/altinn2eifellesformat2018_03_16.xsl" )
                        .writeAsync(channels.toDownstream,channels.fromDownstream,channels.toManager)

                runBlocking {

                    EmbeddedActiveMQ(jmsDetails).use { eMQ ->

                        withTimeoutOrNull(patienceLimit) {
                            dataEia.forEach {
                                channels.toDownstream.send(it)
                                channels.fromDownstream.receive() //receive ack so the jms will continue to receive
                            }
                        }

                        jms.cancelAndJoin()
                        channels.close()

                        eMQ.queue.size
                    }
                } shouldEqualTo dataEia.size
            }

            it("should receive ${dataOther.size} avro elements, transformed to xml") {

                val channels = Channels<GenericRecord>(1)
                val jms = ExternalAttachmentToJMS(
                        jmsDetails,
                        "src/main/resources/altinn2eifellesformat2018_03_16.xsl" )
                        .writeAsync(channels.toDownstream,channels.fromDownstream,channels.toManager)

                runBlocking {

                    EmbeddedActiveMQ(jmsDetails).use { eMQ ->

                        withTimeoutOrNull(patienceLimit) {
                            dataOther.forEach {
                                channels.toDownstream.send(it)
                                channels.fromDownstream.receive() //receive ack so the jms will continue to receive
                            }
                        }

                        jms.cancelAndJoin()
                        channels.close()

                        eMQ.queue.size
                    }
                } shouldEqualTo dataOther.size
            }
        }
    }
})