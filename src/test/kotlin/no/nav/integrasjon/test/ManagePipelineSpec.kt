@file:Suppress("UNCHECKED_CAST")

package no.nav.integrasjon.test

import kotlinx.coroutines.experimental.*
import no.nav.common.KafkaEnvironment
import no.nav.integrasjon.jms.JMSProperties
import no.nav.integrasjon.jms.JMSTextMessageWriter
import no.nav.integrasjon.kafka.KafkaClientProperties
import no.nav.integrasjon.kafka.KafkaEvents
import no.nav.integrasjon.kafka.KafkaTopicConsumer
import no.nav.integrasjon.manager.ManagePipeline
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


object ManagePipelineSpec : Spek({

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

    class TrfString : JMSTextMessageWriter<String>(jmsDetails) {
        override fun transform(session: Session, event: String): Result =
                Result(
                        status = true,
                        txtMsg = session.createTextMessage().apply { this.text = event.toUpperCase() }
                )
    }

    class TrfInt : JMSTextMessageWriter<Int>(jmsDetails) {
        override fun transform(session: Session, event: Int): Result =
                Result(
                        status = true,
                        txtMsg = session.createTextMessage().apply { this.text = (event * event).toString() }
                )
    }

    class TrfAvro : JMSTextMessageWriter<GenericRecord>(jmsDetails) {
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

                val data = kPData[KafkaEvents.STRING]!! as List<String>

                val manager = ManagePipeline.init<String,String>(
                        kCPPType[KafkaEvents.STRING]!!, TrfString()).manageAsync()
                val producer = KafkaTopicProducer.init<String,String>(
                        kCPPType[KafkaEvents.STRING]!!, "key").produceAsync(data)

                // helper object to get jms queue size


                runBlocking {

                    EmbeddedActiveMQ(jmsDetails).use { eMQ ->

                        withTimeoutOrNull(patienceLimit) {
                            while (eMQ.queue.size < data.size && manager.isActive) delay(waitPatience)
                        }

                        producer.cancelAndJoin()
                        manager.cancelAndJoin()

                        eMQ.queue.map { (it as TextMessage).text }
                    }
                } shouldContainAll data.map { it.toUpperCase() }
            }
        }

        context("send integer elements, receive, transform, and send to jms") {

            it("should receive ${kPData[KafkaEvents.INT]!!.size} integer elements, transformed to square") {

                val data = kPData[KafkaEvents.INT]!! as List<Int>

                val manager = ManagePipeline.init<String,Int>(kCPPType[KafkaEvents.INT]!!, TrfInt()).manageAsync()
                val producer = KafkaTopicProducer.init<String,Int>(
                        kCPPType[KafkaEvents.INT]!!, "key").produceAsync(data)

                runBlocking {

                    EmbeddedActiveMQ(jmsDetails).use { eMQ ->

                        withTimeoutOrNull(patienceLimit) {
                            while (eMQ.queue.size < data.size && manager.isActive) delay(waitPatience)
                        }

                        producer.cancelAndJoin()
                        manager.cancelAndJoin()

                        eMQ.queue.map { (it as TextMessage).text.toInt() }
                    }
                } shouldContainAll data.map { it * it }
            }
        }

        context("send avro elements, receive, transform, and send to jms") {

            it("should receive ${kPData[KafkaEvents.AVRO]!!.size} avro elements, transformed to toString") {

                val data = kPData[KafkaEvents.AVRO]!! as List<GenericRecord>

                val manager = ManagePipeline.init<String,GenericRecord>(
                        kCPPType[KafkaEvents.AVRO]!!, TrfAvro()).manageAsync()
                val producer = KafkaTopicProducer.init<String,GenericRecord>(
                        kCPPType[KafkaEvents.AVRO]!!,
                        "key").produceAsync(data)

                runBlocking {

                    EmbeddedActiveMQ(jmsDetails).use { eMQ ->

                        withTimeoutOrNull(patienceLimit) {
                            while (eMQ.queue.size < data.size && manager.isActive) delay(waitPatience)
                        }

                        producer.cancelAndJoin()
                        manager.cancelAndJoin()

                        eMQ.queue.map { (it as TextMessage).text }
                    }
                } shouldContainAll data.map { it.toString()  }
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