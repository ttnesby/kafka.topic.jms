package no.nav.integrasjon.test

import kotlinx.coroutines.experimental.*
import mu.KotlinLogging
import no.nav.common.KafkaEnvironment
import no.nav.integrasjon.*
import no.nav.integrasjon.test.utils.EmbeddedActiveMQ
import no.nav.integrasjon.test.utils.KafkaTopicProducer
import org.amshove.kluent.shouldContainAll
import org.amshove.kluent.shouldEqualTo
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.context
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.xcontext
import java.io.File
import java.nio.file.Files
import java.util.*
import java.util.stream.Collectors
import javax.jms.TextMessage


object KafkaTopic2JMSTextMessage : Spek({

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
            "kafkaEvents"
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

    val schema = Schema.Parser().let {
        it.parse(File("src/main/resources/external_attachment.avsc"))
    }

    fun getFileAsString(filePath: String) = Files.lines(File(filePath).toPath())
            .collect(Collectors.joining("\n"))

    describe("Kafka topic listener transforming events to jms backend tests") {

        val data = (1..100).map {"data-$it"}
        val dataInt = (1..100).map { it }
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
                put("sc","sc-$it")
                put("sec","sec-$it")
                put("archRef","archRef-$it")
            }
        }

        val waitPatience = 100L
        val patienceLimit = 7_000L

        beforeGroup {
            kEnv.start()
        }

        context("send string elements to kafka and receive them") {

            it("should receive ${data.size} string elements") {

                val events = mutableListOf<String>()

                Channels<String>(1).use { c ->

                    runBlocking {

                        // kick of asynchronous task for receiving data from kafka
                        val consumer = KafkaTopicConsumer.init<String, String>(kCDetailsStr)
                                .consumeAsync(c.kafkaEvents,c.commitAction,c.status)

                        if (c.status.receive() == Problem) return@runBlocking

                        //kick of asynchronous task for sending data to kafka
                        val producer = KafkaTopicProducer.init<String,String>(kPDetailsStr, "key").produceAsync(data)

                        withTimeoutOrNull(patienceLimit) {
                            while (events.size < data.size && (c.status.poll()?.let { it } != Problem))
                                c.kafkaEvents.receive().also {
                                    events.add(it)
                                    c.commitAction.send(DoCommit)
                                }
                        }

                        producer.cancelAndJoin()
                        consumer.cancelAndJoin()
                    }
                }

                events shouldContainAll data
            }

            it("should not receive any data when all data is already committed") {

                val events = mutableListOf<String>()

                Channels<String>(1).use { c ->

                    runBlocking {

                        // kick of asynchronous task for receiving data from kafka
                        val consumer = KafkaTopicConsumer.init<String, String>(kCDetailsStr)
                                .consumeAsync(c.kafkaEvents,c.commitAction,c.status)

                        if (c.status.receive() == Problem) return@runBlocking

                        withTimeoutOrNull(2_000L) {
                            while (events.isEmpty() && (c.status.poll()?.let { it } != Problem)) {
                                c.kafkaEvents.poll()?.let {
                                    events.add(it)
                                    c.commitAction.send(DoCommit)
                                }
                                delay(waitPatience)
                            }
                        }

                        consumer.cancelAndJoin()
                    }
                }
                events.isEmpty() shouldEqualTo true
            }
        }

        context("send integer elements to kafka and receive them") {

            it("should receive ${dataInt.size} integer elements") {

                val events = mutableListOf<Int>()

                Channels<Int>(1).use { c ->

                    runBlocking {

                        // kick of asynchronous task for receiving data from kafka
                        val consumer = KafkaTopicConsumer.init<String, Int>(kCDetailsInt)
                                .consumeAsync(c.kafkaEvents,c.commitAction,c.status)

                        if (c.status.receive() == Problem) return@runBlocking

                        //kick of asynchronous task for sending data to kafka
                        val producer = KafkaTopicProducer.init<String,Int>(kPDetailsInt, "key").produceAsync(dataInt)

                        withTimeoutOrNull(patienceLimit) {
                            while (events.size < dataInt.size && (c.status.poll()?.let { it } != Problem))
                                c.kafkaEvents.receive().also {
                                    events.add(it)
                                    c.commitAction.send(DoCommit)
                                }
                        }

                        producer.cancelAndJoin()
                        consumer.cancelAndJoin()
                    }
                }

                events shouldContainAll dataInt
            }

            it("should not receive any data when all data is already committed") {

                val events = mutableListOf<Int>()

                Channels<Int>(1).use { c ->

                    runBlocking {

                        // kick of asynchronous task for receiving data from kafka
                        val consumer = KafkaTopicConsumer.init<String, Int>(kCDetailsInt)
                                .consumeAsync(c.kafkaEvents,c.commitAction,c.status)

                        if (c.status.receive() == Problem) return@runBlocking

                        withTimeoutOrNull(2_000L) {
                            while (events.isEmpty()) {
                                c.kafkaEvents.poll()?.let {
                                    events.add(it)
                                    c.commitAction.send(DoCommit)
                                }
                                delay(waitPatience)
                            }
                        }
                        consumer.cancelAndJoin()
                    }
                }
                events.isEmpty() shouldEqualTo true
            }
        }

        context("send string elements, receive, transform, and send to jms") {

            it("should receive ${data.size} string elements, transformed to uppercase") {

                val manager = ManagePipeline.init<String,String>(kCDetailsStr, TrfString()).manageAsync()
                val producer = KafkaTopicProducer.init<String,String>(kPDetailsStr, "key").produceAsync(data)

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
                        ExternalAttchmentToJMS(
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

        afterGroup {
            kEnv.tearDown()
        }
    }
})