@file:Suppress("UNCHECKED_CAST")

package no.nav.integrasjon.test

import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.SendChannel
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.withTimeoutOrNull
import no.nav.integrasjon.jms.ExternalAttachmentToJMS
import no.nav.integrasjon.jms.JMSProperties
import no.nav.integrasjon.jms.JMSTextMessageWriter
import no.nav.integrasjon.kafka.KafkaEvents
import no.nav.integrasjon.Problem
import no.nav.integrasjon.Status
import no.nav.integrasjon.test.utils.EmbeddedActiveMQ
import org.amshove.kluent.shouldContainAll
import org.apache.activemq.ActiveMQConnectionFactory
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.context
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import javax.jms.TextMessage
import no.nav.integrasjon.test.utils.D.kPData
import no.nav.integrasjon.test.utils.getFileAsString
import no.nav.integrasjon.test.utils.xmlOneliner
import org.amshove.kluent.shouldEqualTo
import org.apache.avro.generic.GenericRecord
import javax.jms.Session

object JMSTextMessageWriterSpec : Spek({

    //val log = KotlinLogging.logger {  }

    // global use of one jms settings - embedded active MQ
    val jmsDetails = JMSProperties(
            ActiveMQConnectionFactory("vm://localhost?broker.persistent=false"),
            "toDownstream",
            "",
            ""
    )

    // implement a String version of JMSTextMessageWriter - transforming to upper case
    class TrfString(status: SendChannel<Status>) : JMSTextMessageWriter<String>(jmsDetails, status) {
        override fun transform(session: Session, event: String): Result =
                Result(
                        status = true,
                        txtMsg = session.createTextMessage().apply { this.text = event.toUpperCase() }
                )
    }

    // implement a Integer version of JMSTextMessageWriter - transforming each no to square
    class TrfInt(status: SendChannel<Status>) : JMSTextMessageWriter<Int>(jmsDetails, status) {
        override fun transform(session: Session, event: Int): Result =
                Result(
                        status = true,
                        txtMsg = session.createTextMessage().apply { this.text = (event * event).toString() }
                )
    }

    // implement a Apache Avro version of JMSTextMessageWriter - no special transformation
    class TrfAvro(status: SendChannel<Status>) : JMSTextMessageWriter<GenericRecord>(jmsDetails, status) {
        override fun transform(session: Session, event: GenericRecord): Result =
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

                val data = kPData[KafkaEvents.STRING]!! as List<String>
                val result = mutableListOf<String>()

                val status = Channel<Status>()
                TrfString(status).use { jms ->

                    runBlocking {
                        if (status.receive() == Problem) return@runBlocking

                        EmbeddedActiveMQ(jmsDetails).use { eMQ ->
                            withTimeoutOrNull(patienceLimit) {
                                data.forEach {
                                    jms.data.send(it)
                                    status.receive() //receive Ready so the jms will continue to receive
                                }
                            }
                            eMQ.queue.forEach { result.add((it as TextMessage).text) }
                        }
                    }
                }
                status.close()

                result shouldContainAll data.map { it.toUpperCase() }
            }

            it("should receive ${kPData[KafkaEvents.INT]!!.size} int elements, transformed to square") {

                val data = kPData[KafkaEvents.INT]!! as List<Int>
                val result = mutableListOf<String>()

                val status = Channel<Status>()
                TrfInt(status).use { jms ->

                    runBlocking {
                        if (status.receive() == Problem) return@runBlocking

                        EmbeddedActiveMQ(jmsDetails).use { eMQ ->
                            withTimeoutOrNull(patienceLimit) {
                                data.forEach {
                                    jms.data.send(it)
                                    status.receive() //receive Ready so the jms will continue to receive
                                }
                            }
                            eMQ.queue.forEach { result.add((it as TextMessage).text) }
                        }
                    }
                }
                status.close()

                result shouldContainAll data.map { (it*it).toString() }
            }

            it("should receive ${kPData[KafkaEvents.AVRO]!!.size} avro elements, transformed") {

                val data = kPData[KafkaEvents.AVRO]!! as List<GenericRecord>
                val result = mutableListOf<String>()

                val status = Channel<Status>()
                TrfAvro(status).use { jms ->

                    runBlocking {
                        if (status.receive() == Problem) return@runBlocking

                        EmbeddedActiveMQ(jmsDetails).use { eMQ ->
                            withTimeoutOrNull(patienceLimit) {
                                data.forEach {
                                    jms.data.send(it)
                                    status.receive() //receive Ready so the jms will continue to receive
                                }
                            }
                            eMQ.queue.forEach { result.add((it as TextMessage).text) }
                        }
                    }
                }
                status.close()

                result shouldContainAll data.map { it.toString() }
            }

            it("should receive ${kPData[KafkaEvents.MUSIC]!!.size} music elements, transformed to html") {

                val data = kPData[KafkaEvents.MUSIC]!! as List<GenericRecord>
                val result = mutableListOf<Int>()

                val status = Channel<Status>()
                ExternalAttachmentToJMS(jmsDetails, status, KafkaEvents.MUSIC ).use { jms ->

                    runBlocking {
                        if (status.receive() == Problem) return@runBlocking

                        EmbeddedActiveMQ(jmsDetails).use { eMQ ->
                            withTimeoutOrNull(patienceLimit) {
                                data.forEach {
                                    jms.data.send(it)
                                    status.receive() //receive Ready so the jms will continue to receive
                                }
                            }
                            eMQ.queue.forEach { result.add(xmlOneliner((it as TextMessage).text).hashCode()) }
                        }
                    }
                }
                status.close()

                result shouldContainAll data.map {
                    xmlOneliner(getFileAsString("src/test/resources/musicCatalog.html")).hashCode() }
            }

           it("should receive ${kPData[KafkaEvents.OPPFOLGINGSPLAN]!!.size} oppf elements, " +
                    "transformed to xml") {

                val data = kPData[KafkaEvents.OPPFOLGINGSPLAN]!! as List<GenericRecord>
                var result = 0

                val status = Channel<Status>()
                ExternalAttachmentToJMS(jmsDetails, status, KafkaEvents.OPPFOLGINGSPLAN ).use { jms ->

                    runBlocking {
                        if (status.receive() == Problem) return@runBlocking

                        EmbeddedActiveMQ(jmsDetails).use { eMQ ->
                            withTimeoutOrNull(patienceLimit) {
                                data.forEach {
                                    jms.data.send(it)
                                    status.receive() //receive Ready so the jms will continue to receive
                                }
                            }
                            result = eMQ.queue.size
                        }
                    }
                }
                status.close()

                result shouldEqualTo data.size

            }
        }
    }
})