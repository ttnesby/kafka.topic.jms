@file:Suppress("UNCHECKED_CAST")

package no.nav.integrasjon.test

import kotlinx.coroutines.experimental.cancelAndJoin
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.withTimeoutOrNull
import no.nav.integrasjon.jms.ExternalAttachmentToJMS
import no.nav.integrasjon.jms.JMSProperties
import no.nav.integrasjon.jms.JMSTextMessageWriter
import no.nav.integrasjon.kafka.KafkaEvents
import no.nav.integrasjon.manager.Channels
import no.nav.integrasjon.test.utils.EmbeddedActiveMQ
import no.nav.integrasjon.test.utils.getFileAsString
import no.nav.integrasjon.test.utils.xmlOneliner
import org.amshove.kluent.shouldContainAll
import org.amshove.kluent.shouldEqualTo
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.avro.generic.GenericRecord
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.context
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import javax.jms.TextMessage
import no.nav.integrasjon.test.utils.D.kPData

object JMSTextMessageWriterSpec : Spek({

    //val log = KotlinLogging.logger {  }

    val jmsDetails = JMSProperties(
            ActiveMQConnectionFactory("vm://localhost?broker.persistent=false"),
            "toDownstream",
            "",
            ""
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

    //val waitPatience = 100L
    val patienceLimit = 7_000L


    describe("JMSTextMessageWriter tests") {

        context("string elements, transform and send to jms") {

            it("should receive ${kPData[KafkaEvents.STRING]!!.size} string elements, transformed to uppercase") {

                val channels = Channels<String>(1)
                val jms = TrfString().writeAsync(channels.toDownstream,channels.fromDownstream,channels.toManager)
                val data = kPData[KafkaEvents.STRING]!! as List<String>

                runBlocking {

                    EmbeddedActiveMQ(jmsDetails).use { eMQ ->

                        withTimeoutOrNull(patienceLimit) {
                            data.forEach {
                                channels.toDownstream.send(it)
                                channels.fromDownstream.receive() //receive ack so the jms will continue to receive
                            }
                        }

                        jms.cancelAndJoin()
                        channels.close()

                        eMQ.queue.map { (it as TextMessage).text }
                    }
                } shouldContainAll data.map { it.toUpperCase() }
            }

            it("should receive ${kPData[KafkaEvents.INT]!!.size} int elements, transformed to square") {

                val channels = Channels<Int>(1)
                val jms = TrfInt().writeAsync(channels.toDownstream,channels.fromDownstream,channels.toManager)
                val data = kPData[KafkaEvents.INT]!! as List<Int>


                runBlocking {

                    EmbeddedActiveMQ(jmsDetails).use { eMQ ->

                        withTimeoutOrNull(patienceLimit) {
                            data.forEach {
                                channels.toDownstream.send(it)
                                channels.fromDownstream.receive() //receive ack so the jms will continue to receive
                            }
                        }

                        jms.cancelAndJoin()
                        channels.close()

                        eMQ.queue.map { (it as TextMessage).text }
                    }
                } shouldContainAll data.map { (it*it).toString() }
            }

            it("should receive ${kPData[KafkaEvents.AVRO]!!.size} avro elements, transformed") {

                val channels = Channels<GenericRecord>(1)
                val jms = TrfAvro().writeAsync(channels.toDownstream,channels.fromDownstream,channels.toManager)
                val data = kPData[KafkaEvents.AVRO]!! as List<GenericRecord>


                runBlocking {

                    EmbeddedActiveMQ(jmsDetails).use { eMQ ->

                        withTimeoutOrNull(patienceLimit) {
                            data.forEach {
                                channels.toDownstream.send(it)
                                channels.fromDownstream.receive() //receive ack so the jms will continue to receive
                            }
                        }

                        jms.cancelAndJoin()
                        channels.close()

                        eMQ.queue.map { (it as TextMessage).text }
                    }
                } shouldContainAll data.map { it.toString() }
            }

            it("should receive ${kPData[KafkaEvents.MUSIC]!!.size} avro elements, transformed to html") {

                val channels = Channels<GenericRecord>(1)
                val jms = ExternalAttachmentToJMS(jmsDetails, KafkaEvents.MUSIC )
                        .writeAsync(channels.toDownstream,channels.fromDownstream,channels.toManager)

                val data = kPData[KafkaEvents.MUSIC]!! as List<GenericRecord>

                runBlocking {

                    EmbeddedActiveMQ(jmsDetails).use { eMQ ->

                        withTimeoutOrNull(patienceLimit) {
                            data.forEach {
                                channels.toDownstream.send(it)
                                channels.fromDownstream.receive() //receive ack so the jms will continue to receive
                            }
                        }

                        jms.cancelAndJoin()
                        channels.close()

                        eMQ.queue.map { xmlOneliner((it as TextMessage).text).hashCode() }
                    }
                } shouldContainAll data.map {
                    xmlOneliner(getFileAsString("src/test/resources/musicCatalog.html")).hashCode()
                }
            }

            it("should receive ${kPData[KafkaEvents.OPPFOLGINGSPLAN]!!.size} avro elements, " +
                    "transformed to xml") {

                val channels = Channels<GenericRecord>(1)
                val jms = ExternalAttachmentToJMS(
                        jmsDetails,
                        KafkaEvents.OPPFOLGINGSPLAN)
                        .writeAsync(channels.toDownstream,channels.fromDownstream,channels.toManager)

                val data = kPData[KafkaEvents.OPPFOLGINGSPLAN]!! as List<GenericRecord>

                runBlocking {

                    EmbeddedActiveMQ(jmsDetails).use { eMQ ->

                        withTimeoutOrNull(patienceLimit) {
                            data.forEach {
                                channels.toDownstream.send(it)
                                channels.fromDownstream.receive() //receive ack so the jms will continue to receive
                            }
                        }

                        jms.cancelAndJoin()
                        channels.close()

                        eMQ.queue.size
                    }
                } shouldEqualTo data.size
            }
        }
    }
})