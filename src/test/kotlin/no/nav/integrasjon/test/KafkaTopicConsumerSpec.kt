package no.nav.integrasjon.test

import kotlinx.coroutines.experimental.cancelAndJoin
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.withTimeoutOrNull
import mu.KotlinLogging
import no.nav.common.KafkaEnvironment
import no.nav.integrasjon.kafka.KafkaClientDetails
import no.nav.integrasjon.kafka.KafkaTopicConsumer
import no.nav.integrasjon.manager.Channels
import no.nav.integrasjon.manager.Problem
import no.nav.integrasjon.manager.Ready
import no.nav.integrasjon.test.utils.KafkaTopicProducer
import org.amshove.kluent.shouldContainAll
import org.amshove.kluent.shouldEqualTo
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.context
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import java.io.File
import java.util.*

object KafkaTopicConsumerSpec : Spek({

    val log = KotlinLogging.logger {  }

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


    val waitPatience = 100L
    val patienceLimit = 7_000L


    describe("KafkaTopicConsumer tests") {

        beforeGroup {
            kEnv.start()
        }

        context("send string elements to kafka and receive them") {

            it("should receive ${dataStr.size} string elements") {

                val events = mutableListOf<String>()

                Channels<String>(1).use { c ->

                    runBlocking {

                        // kick of asynchronous task for receiving data from kafka
                        val consumer = KafkaTopicConsumer.init<String, String>(kCDetailsStr)
                                .consumeAsync(c.toDownstream,c.fromDownstream,c.toManager)

                        if (c.toManager.receive() == Problem) return@runBlocking

                        //kick of asynchronous task for sending data to kafka
                        val producer = KafkaTopicProducer.init<String,String>(kPDetailsStr, "key")
                                .produceAsync(dataStr)

                        withTimeoutOrNull(patienceLimit) {
                            while (events.size < dataStr.size && (c.toManager.poll()?.let { it } != Problem))
                                c.toDownstream.receive().also {
                                    events.add(it)
                                    c.fromDownstream.send(Ready)
                                }
                        }

                        producer.cancelAndJoin()
                        consumer.cancelAndJoin()
                    }
                }

                events shouldContainAll dataStr
            }

            it("should not receive any data when all data is already committed") {

                val events = mutableListOf<String>()

                Channels<String>(1).use { c ->

                    runBlocking {

                        // kick of asynchronous task for receiving data from kafka
                        val consumer = KafkaTopicConsumer.init<String, String>(kCDetailsStr)
                                .consumeAsync(c.toDownstream,c.fromDownstream,c.toManager)

                        if (c.toManager.receive() == Problem) return@runBlocking

                        withTimeoutOrNull(2_000L) {
                            while (events.isEmpty() && (c.toManager.poll()?.let { it } != Problem)) {
                                c.toDownstream.poll()?.let {
                                    events.add(it)
                                    c.fromDownstream.send(Ready)
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
                                .consumeAsync(c.toDownstream,c.fromDownstream,c.toManager)

                        if (c.toManager.receive() == Problem) return@runBlocking

                        //kick of asynchronous task for sending data to kafka
                        val producer = KafkaTopicProducer.init<String,Int>(kPDetailsInt, "key")
                                .produceAsync(dataInt)

                        withTimeoutOrNull(patienceLimit) {
                            while (events.size < dataInt.size && (c.toManager.poll()?.let { it } != Problem))
                                c.toDownstream.receive().also {
                                    events.add(it)
                                    c.fromDownstream.send(Ready)
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
                                .consumeAsync(c.toDownstream,c.fromDownstream,c.toManager)

                        if (c.toManager.receive() == Problem) return@runBlocking

                        withTimeoutOrNull(2_000L) {
                            while (events.isEmpty()) {
                                c.toDownstream.poll()?.let {
                                    events.add(it)
                                    c.fromDownstream.send(Ready)
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

        context("send avro elements to kafka and receive them") {

            it("should receive ${dataAvro.size} integer elements") {

                val events = mutableListOf<GenericRecord>()

                Channels<GenericRecord>(1).use { c ->

                    runBlocking {

                        // kick of asynchronous task for receiving data from kafka
                        val consumer = KafkaTopicConsumer.init<String, GenericRecord>(kCDetailsAvro)
                                .consumeAsync(c.toDownstream,c.fromDownstream,c.toManager)

                        if (c.toManager.receive() == Problem) return@runBlocking

                        //kick of asynchronous task for sending data to kafka
                        val producer = KafkaTopicProducer.init<String,GenericRecord>(kPDetailsAvro, "key")
                                .produceAsync(dataAvro)

                        withTimeoutOrNull(patienceLimit) {
                            while (events.size < dataAvro.size && (c.toManager.poll()?.let { it } != Problem))
                                c.toDownstream.receive().also {
                                    events.add(it)
                                    c.fromDownstream.send(Ready)
                                }
                        }

                        producer.cancelAndJoin()
                        consumer.cancelAndJoin()
                    }
                }

                events shouldContainAll dataAvro
            }
        }

        afterGroup {
            kEnv.tearDown()
        }
    }

})