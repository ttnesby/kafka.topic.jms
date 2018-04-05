@file:Suppress("UNCHECKED_CAST")

package no.nav.integrasjon.test

import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.SendChannel
import no.nav.common.KafkaEnvironment
import no.nav.integrasjon.JMSMetric
import no.nav.integrasjon.jms.JMSProperties
import no.nav.integrasjon.jms.JMSTextMessageWriter
import no.nav.integrasjon.kafka.KafkaClientProperties
import no.nav.integrasjon.kafka.KafkaEvents
import no.nav.integrasjon.kafka.KafkaTopicConsumer
import no.nav.integrasjon.Problem
import no.nav.integrasjon.Status
import no.nav.integrasjon.test.utils.D.kPData
import no.nav.integrasjon.test.utils.EmbeddedActiveMQ
import no.nav.integrasjon.test.utils.KafkaTopicProducer
import no.nav.integrasjon.test.utils.produceToJMSMP
import org.amshove.kluent.shouldContainAll
import org.amshove.kluent.shouldEqual
import org.amshove.kluent.shouldEqualTo
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.*
import java.util.*
import javax.jms.Session
import javax.jms.TextMessage

/**
 * For completeness, this object do a few more tests in the beginning, before
 * focusing on test of the relevant class. See function produceToJMSMP for details
 */
object ExternalAttachmentToJMSSpec : Spek({


    // create the topics to be created in kafka env
    val topics = KafkaEvents.values().map { KafkaTopicConsumer.event2Topic(it) }

    val kEnv = KafkaEnvironment(topics = topics, withSchemaRegistry = true)

    val kCPPType = mutableMapOf<KafkaEvents, KafkaClientProperties>()

    // create a map of all kafka client properties
    KafkaEvents.values().forEach {

        kCPPType[it] = KafkaClientProperties(
                Properties().apply {
                    set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kEnv.brokersURL)
                    set("schema.registry.url",kEnv.serverPark.schemaregistry.url)
                    set(ConsumerConfig.CLIENT_ID_CONFIG, "kafkaTopicConsumer")
                },
                it,
                100
        )
    }

    val jmsDetails = JMSProperties(
            ActiveMQConnectionFactory("vm://localhost?broker.persistent=false"),
            "toDownstream",
            "",
            ""
    )

    class TrfString(
            status: SendChannel<Status>,
            jmsMetric: SendChannel<JMSMetric>) : JMSTextMessageWriter<String>(jmsDetails, status, jmsMetric) {
        override fun transform(session: Session, event: String): Result =
                Result(
                        status = true,
                        txtMsg = session.createTextMessage().apply { this.text = event.toUpperCase() }
                )
    }

    class TrfInt(
            status: SendChannel<Status>,
            jmsMetric: SendChannel<JMSMetric>) : JMSTextMessageWriter<Int>(jmsDetails, status, jmsMetric) {
        override fun transform(session: Session, event: Int): Result =
                Result(
                        status = true,
                        txtMsg = session.createTextMessage().apply { this.text = (event * event).toString() }
                )
    }

    class TrfAvro(
            status: SendChannel<Status>,
            jmsMetric: SendChannel<JMSMetric>) : JMSTextMessageWriter<GenericRecord>(jmsDetails, status, jmsMetric) {
        override fun transform(session: Session, event: GenericRecord): Result =
                Result(
                        status = true,
                        txtMsg = session.createTextMessage().apply { this.text = event.toString() }
                )
    }


    val waitPatience = 100L
    val patienceLimit = 7_000L

    describe("Kafka topic listener transforming events to jms backend tests") {

        beforeGroup {
            kEnv.start()
        }

        context("send string elements, receive, transform, and send to jms") {

            it("should receive ${kPData[KafkaEvents.STRING]!!.size} string elements, transformed to uppercase") {

                val ke = KafkaEvents.STRING
                val data = kPData[ke]!! as List<String>
                val cliProps = kCPPType[ke]!!

                val producer = KafkaTopicProducer.init<String,String>(cliProps, "key").produceAsync(data)
                val status = Channel<Status>()
                val jmsMetric = Channel<JMSMetric>(50)

                val result = mutableListOf<String>()

                runBlocking {
                    TrfString(status, jmsMetric).use { jms ->

                        if (status.receive() == Problem) return@use

                        KafkaTopicConsumer.init<String, String>(cliProps, jms.data, status).use {

                            EmbeddedActiveMQ(jmsDetails).use { eMQ ->

                                withTimeoutOrNull(patienceLimit) {
                                    while (eMQ.queue.size < data.size) delay(waitPatience)
                                }
                                producer.cancelAndJoin()

                                eMQ.queue.forEach { result.add((it as TextMessage).text) }
                            }
                        }
                    }
                }

                result shouldContainAll data.map { it.toUpperCase() }
            }
        }

        context("send integer elements, receive, transform, and send to jms") {

            it("should receive ${kPData[KafkaEvents.INT]!!.size} integer elements, transformed to square") {

                val ke = KafkaEvents.INT
                val data = kPData[ke]!! as List<Int>
                val cliProps = kCPPType[ke]!!

                val producer = KafkaTopicProducer.init<String,Int>(cliProps, "key").produceAsync(data)
                val status = Channel<Status>()
                val jmsMetric = Channel<JMSMetric>(50)

                val result = mutableListOf<Int>()

                runBlocking {
                    TrfInt(status, jmsMetric).use { jms ->

                        if (status.receive() == Problem) return@use

                        KafkaTopicConsumer.init<String, Int>(cliProps, jms.data, status).use {

                            EmbeddedActiveMQ(jmsDetails).use { eMQ ->

                                withTimeoutOrNull(patienceLimit) {
                                    while (eMQ.queue.size < data.size) delay(waitPatience)
                                }
                                producer.cancelAndJoin()

                                eMQ.queue.forEach { result.add((it as TextMessage).text.toInt()) }
                            }
                        }
                    }
                }

                result shouldContainAll data.map { it * it }
            }
        }

        context("send avro elements, receive, transform, and send to jms") {

            it("should receive ${kPData[KafkaEvents.AVRO]!!.size} avro elements, transformed to toString") {

                val ke = KafkaEvents.AVRO
                val data = kPData[ke]!! as List<GenericRecord>
                val cliProps = kCPPType[ke]!!

                val producer = KafkaTopicProducer.init<String,GenericRecord>(cliProps, "key").produceAsync(data)
                val status = Channel<Status>()
                val jmsMetric = Channel<JMSMetric>(50)

                val result = mutableListOf<String>()

                runBlocking {
                    TrfAvro(status, jmsMetric).use { jms ->

                        if (status.receive() == Problem) return@use

                        KafkaTopicConsumer.init<String, GenericRecord>(cliProps, jms.data, status).use {

                            EmbeddedActiveMQ(jmsDetails).use { eMQ ->

                                withTimeoutOrNull(patienceLimit) {
                                    while (eMQ.queue.size < data.size) delay(waitPatience)
                                }
                                producer.cancelAndJoin()

                                eMQ.queue.forEach { result.add((it as TextMessage).text) }
                            }
                        }
                    }
                }

                result shouldContainAll data.map { it.toString() }
            }
        }

        context("send music elements, receive, transform, and send to jms") {

            it("should receive ${kPData[KafkaEvents.MUSIC]!!.size} music, transformed to html") {

                val data = kPData[KafkaEvents.MUSIC]!! as List<GenericRecord>

                produceToJMSMP(
                        kCPPType[KafkaEvents.MUSIC]!!,
                        jmsDetails,
                        KafkaEvents.MUSIC,
                        data
                ) shouldEqualTo  data.size
            }
        }

        context("send altinn elements, receive, transform and send to jms") {

            it("should receive ${kPData[KafkaEvents.OPPFOLGINGSPLAN]!!.size} oppfolg., " +
                    "transformed to xml") {

                val data = kPData[KafkaEvents.OPPFOLGINGSPLAN]!! as List<GenericRecord>

                produceToJMSMP(
                        kCPPType[KafkaEvents.OPPFOLGINGSPLAN]!!,
                        jmsDetails,
                        KafkaEvents.OPPFOLGINGSPLAN,
                        data
                ) shouldEqualTo  data.size
            }

            it("should receive ${kPData[KafkaEvents.BANKKONTONR]!!.size} bankkontonr, " +
                    "transformed to xml") {

                val data = kPData[KafkaEvents.BANKKONTONR]!! as List<GenericRecord>

                produceToJMSMP(
                        kCPPType[KafkaEvents.BANKKONTONR]!!,
                        jmsDetails,
                        KafkaEvents.BANKKONTONR,
                        data
                ) shouldEqual data.size
            }

            it("should receive ${kPData[KafkaEvents.MAALEKORT]!!.size} maalekort, " +
                    "transformed to xml") {

                val data = kPData[KafkaEvents.MAALEKORT]!! as List<GenericRecord>

                produceToJMSMP(
                        kCPPType[KafkaEvents.MAALEKORT]!!,
                        jmsDetails,
                        KafkaEvents.MAALEKORT,
                        data
                ) shouldEqual data.size
            }

            it("should receive ${kPData[KafkaEvents.BARNEHAGELISTE]!!.size} barnehageliste, " +
                    "transformed to xml") {

                val data = kPData[KafkaEvents.BARNEHAGELISTE]!! as List<GenericRecord>

                produceToJMSMP(
                        kCPPType[KafkaEvents.BARNEHAGELISTE]!!,
                        jmsDetails,
                        KafkaEvents.BARNEHAGELISTE,
                        data
                ) shouldEqual data.size
            }
        }

        afterGroup {
            kEnv.tearDown()
        }
    }
})