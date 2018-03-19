package no.nav.integrasjon.test

import kotlinx.coroutines.experimental.*
import no.nav.common.KafkaEnvironment
import no.nav.integrasjon.jms.ExternalAttachmentToJMS
import no.nav.integrasjon.jms.JMSDetails
import no.nav.integrasjon.jms.JMSTextMessageWriter
import no.nav.integrasjon.kafka.KafkaClientDetails
import no.nav.integrasjon.manager.ManagePipeline
import no.nav.integrasjon.test.utils.EmbeddedActiveMQ
import no.nav.integrasjon.test.utils.KafkaTopicProducer
import no.nav.integrasjon.test.utils.getFileAsString
import org.amshove.kluent.shouldContainAll
import org.amshove.kluent.shouldEqualTo
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.*
import java.io.File
import java.util.*
import javax.jms.TextMessage


object ManagePipelineSpec : Spek({

    val topicStr = "testString"
    val topicInt = "testInt"
    val topicAvro = "testAvro"

    val kEnv = KafkaEnvironment(topics = listOf(topicStr,topicInt,topicAvro), withSchemaRegistry = true)

    val kCDetailsStr = KafkaClientDetails(
            Properties().apply {
                set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kEnv.brokersURL)
                set(ConsumerConfig.CLIENT_ID_CONFIG, "kafkaTopicConsumer")
            },
            topicStr,
            100
    )

    val kCDetailsInt = KafkaClientDetails(
            Properties().apply {
                set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kEnv.brokersURL)
                set(ConsumerConfig.CLIENT_ID_CONFIG, "kafkaTopicConsumer")
            },
            topicInt,
            100
    )

    val kCDetailsAvro = KafkaClientDetails(
            Properties().apply {
                set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kEnv.brokersURL)
                set(ConsumerConfig.CLIENT_ID_CONFIG, "kafkaTopicConsumer")
                set("schema.registry.url",kEnv.serverPark.schemaregistry.url)
            },
            topicAvro,
            100
    )

    val kPDetailsStr = KafkaClientDetails(
            Properties().apply {
                set(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kEnv.brokersURL)
                set(ProducerConfig.CLIENT_ID_CONFIG, "kafkaTopicProducer")
            },
            topicStr
    )

    val kPDetailsInt = KafkaClientDetails(
            Properties().apply {
                set(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kEnv.brokersURL)
                set(ProducerConfig.CLIENT_ID_CONFIG, "kafkaTopicProducer")
            },
            topicInt
    )

    val kPDetailsAvro = KafkaClientDetails(
            Properties().apply {
                set(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kEnv.brokersURL)
                set(ProducerConfig.CLIENT_ID_CONFIG, "kafkaTopicProducer")
                set("schema.registry.url",kEnv.serverPark.schemaregistry.url)
            },
            topicAvro
    )

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
            put("sc","TESTONLY")
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


    describe("Kafka topic listener transforming events to jms backend tests") {

        beforeGroup {
            kEnv.start()
        }

        context("send string elements, receive, transform, and send to jms") {

            it("should receive ${dataStr.size} string elements, transformed to uppercase") {

                val manager = ManagePipeline.init<String,String>(kCDetailsStr, TrfString()).manageAsync()
                val producer = KafkaTopicProducer.init<String,String>(kPDetailsStr, "key").produceAsync(dataStr)

                // helper object to get jms queue size


                runBlocking {

                    EmbeddedActiveMQ(jmsDetails).use { eMQ ->

                        withTimeoutOrNull(patienceLimit) {
                            while (eMQ.queue.size < dataStr.size && manager.isActive) delay(waitPatience)
                        }

                        producer.cancelAndJoin()
                        manager.cancelAndJoin()

                        eMQ.queue.map { (it as TextMessage).text }
                    }
                } shouldContainAll dataStr.map { it.toUpperCase() }
            }
        }

        context("send integer elements, receive, transform, and send to jms") {

            it("should receive ${dataInt.size} integer elements, transformed to square") {

                val manager = ManagePipeline.init<String,Int>(kCDetailsInt, TrfInt()).manageAsync()
                val producer = KafkaTopicProducer.init<String,Int>(kPDetailsInt, "key").produceAsync(dataInt)

                runBlocking {

                    EmbeddedActiveMQ(jmsDetails).use { eMQ ->

                        withTimeoutOrNull(patienceLimit) {
                            while (eMQ.queue.size < dataInt.size && manager.isActive) delay(waitPatience)
                        }

                        producer.cancelAndJoin()
                        manager.cancelAndJoin()

                        eMQ.queue.map { (it as TextMessage).text.toInt() }
                    }
                } shouldContainAll dataInt.map { it * it }
            }
        }

        context("send avro elements, receive, transform, and send to jms") {

            it("should receive ${dataAvro.size} avro elements, transformed to toString") {

                val manager = ManagePipeline.init<String,GenericRecord>(kCDetailsAvro, TrfAvro()).manageAsync()
                val producer = KafkaTopicProducer.init<String,GenericRecord>(
                        kPDetailsAvro,
                        "key").produceAsync(dataAvro)

                runBlocking {

                    EmbeddedActiveMQ(jmsDetails).use { eMQ ->

                        withTimeoutOrNull(patienceLimit) {
                            while (eMQ.queue.size < dataAvro.size && manager.isActive) delay(waitPatience)
                        }

                        producer.cancelAndJoin()
                        manager.cancelAndJoin()

                        eMQ.queue.map { (it as TextMessage).text }
                    }
                } shouldContainAll dataAvro.map { it.toString()  }
            }
        }

        context("send avro ext. attachment elements, receive, transform, and send to jms") {

            it("should receive ${dataMusic.size} elements, transformed to html") {

                val manager = ManagePipeline.init<String,GenericRecord>(
                        kCDetailsAvro,
                        ExternalAttachmentToJMS(
                                jmsDetails,
                                "src/test/resources/musicCatalog.xsl"))
                        .manageAsync()

                val producer = KafkaTopicProducer.init<String,GenericRecord>(
                        kPDetailsAvro,
                        "key").produceAsync(dataMusic)

                runBlocking {

                    EmbeddedActiveMQ(jmsDetails).use { eMQ ->

                        withTimeoutOrNull(patienceLimit) {
                            while (eMQ.queue.size < dataMusic.size && manager.isActive) delay(waitPatience)
                        }

                        producer.cancelAndJoin()
                        manager.cancelAndJoin()

                        eMQ.queue.size
                    }
                } shouldEqualTo  dataMusic.size
            }
        }

        context("send altinn avro ext. attachment elements, receive, transform " +
                "and send to jms") {

            it("should receive ${dataEia.size} EIA elements, transformed to xml") {

                val manager = ManagePipeline.init<String,GenericRecord>(
                        kCDetailsAvro,
                        ExternalAttachmentToJMS(
                                jmsDetails,
                                "src/main/resources/altinn2eifellesformat2018_03_16.xsl"))
                        .manageAsync()

                val producer = KafkaTopicProducer.init<String,GenericRecord>(
                        kPDetailsAvro,
                        "key").produceAsync(dataEia)

                runBlocking {

                    EmbeddedActiveMQ(jmsDetails).use { eMQ ->

                        withTimeoutOrNull(patienceLimit) {
                            while (eMQ.queue.size < dataEia.size && manager.isActive) delay(waitPatience)
                        }

                        producer.cancelAndJoin()
                        manager.cancelAndJoin()

                        eMQ.queue.size
                    }
                } shouldEqualTo  dataEia.size
            }

            it("should receive ${dataOther.size} other elements, transformed to xml") {

                val manager = ManagePipeline.init<String,GenericRecord>(
                        kCDetailsAvro,
                        ExternalAttachmentToJMS(
                                jmsDetails,
                                "src/main/resources/altinn2eifellesformat2018_03_16.xsl"))
                        .manageAsync()

                val producer = KafkaTopicProducer.init<String,GenericRecord>(
                        kPDetailsAvro,
                        "key").produceAsync(dataOther)

                runBlocking {

                    EmbeddedActiveMQ(jmsDetails).use { eMQ ->

                        withTimeoutOrNull(patienceLimit) {
                            while (eMQ.queue.size < dataOther.size && manager.isActive) delay(waitPatience)
                        }

                        producer.cancelAndJoin()
                        manager.cancelAndJoin()

                        eMQ.queue.size
                    }
                } shouldEqualTo  dataOther.size
            }
        }

        afterGroup {
            kEnv.tearDown()
        }
    }
})