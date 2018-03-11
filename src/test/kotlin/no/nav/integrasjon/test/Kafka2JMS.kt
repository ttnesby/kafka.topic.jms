package no.nav.integrasjon.test

import kotlinx.coroutines.experimental.cancelAndJoin
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import mu.KotlinLogging
import no.nav.common.KafkaEnvironment
import no.nav.integrasjon.*
import no.nav.integrasjon.test.utils.EmbeddedActiveMQ
import no.nav.integrasjon.test.utils.KafkaTopicProducer
import org.amshove.kluent.shouldContainAll
import org.amshove.kluent.shouldEqualTo
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.context
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import java.util.*
import javax.jms.TextMessage


object Kafka2JMS : Spek({

    val log = KotlinLogging.logger {  }

    val topic = "test"

    val kEnv = KafkaEnvironment(2,topics = listOf(topic))

    val kCDetails = KafkaClientDetails(
            Properties().apply {
                set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kEnv.brokersURL)
                set(ConsumerConfig.CLIENT_ID_CONFIG, "kafkaTopicConsumer")
            },
            topic,
            500
    )

    val kPDetails = KafkaClientDetails(
            Properties().apply {
                set(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kEnv.brokersURL)
                set(ProducerConfig.CLIENT_ID_CONFIG, "kafkaTopicProducer")
            },
            topic
    )

    val jmsDetails = JMSDetails(
            ActiveMQConnectionFactory("vm://localhost?broker.persistent=false"),
            "kafkaEvents"
    )


    describe("Kafka topic listener transforming events to jms backend tests") {

        val data = (1..10).map {"data-$it"}
        val dataInt = (1..10).map { it }

        val waitPatience = 100L

        context("send ${data.size} string elements to kafka and receive them") {

            beforeGroup {
                kEnv.start()
            }

            it("should receive ${data.size} string elements") {

                val events = mutableListOf<String>()

                Channels<String,String>(1).use { c ->

                    runBlocking {

                        // kick of asynchronous task for receiving data from kafka
                        val consumer = KafkaTopicConsumer.init<String, String>(kCDetails)
                                .consumeAsync(c.kafkaEvents,c.commitAction,c.status)

                        if (c.status.receive() == Problem) return@runBlocking

                        //kick of asynchronous task for sending data to kafka
                        val producer = KafkaTopicProducer.init<String,String>(kPDetails, "key").produceAsync(data)

                        while (events.size < data.size && (c.status.poll()?.let { it } != Problem))
                            c.kafkaEvents.receive().also {
                                events.add(it)
                                c.commitAction.send(DoCommit)
                            }

                        producer.cancelAndJoin()
                        consumer.cancelAndJoin()
                    }
                }

                events shouldContainAll  data
            }

            it("should not receive any data when all data is already committed") {

                val events = mutableListOf<String>()

                Channels<String,String>(1).use { c ->

                    runBlocking {

                        // kick of asynchronous task for receiving data from kafka
                        val consumer = KafkaTopicConsumer.init<String, String>(kCDetails)
                                .consumeAsync(c.kafkaEvents,c.commitAction,c.status)

                        if (c.status.receive() == Problem) return@runBlocking

                        var imPatience = 0

                        while (events.isEmpty() && imPatience < 10) {
                            c.kafkaEvents.poll()?.let {
                                events.add(it)
                                c.commitAction.send(DoCommit)
                            }
                            delay(waitPatience)
                            imPatience++
                        }
                        consumer.cancelAndJoin()
                    }
                }

                events.isEmpty() shouldEqualTo true
            }

            afterGroup {
                kEnv.tearDown()
            }
        }

        context("send ${dataInt.size} integer elements to kafka and receive them") {

            beforeGroup {
                kEnv.start()
            }

            it("should receive ${data.size} string elements") {

                val events = mutableListOf<Int>()

                Channels<Int,Int>(1).use { c ->

                    runBlocking {

                        // kick of asynchronous task for receiving data from kafka
                        val consumer = KafkaTopicConsumer.init<String, Int>(kCDetails)
                                .consumeAsync(c.kafkaEvents,c.commitAction,c.status)

                        if (c.status.receive() == Problem) return@runBlocking

                        //kick of asynchronous task for sending data to kafka
                        val producer = KafkaTopicProducer.init<String,Int>(kPDetails, "key").produceAsync(dataInt)

                        while (events.size < data.size && (c.status.poll()?.let { it } != Problem))
                            c.kafkaEvents.receive().also {
                                events.add(it)
                                c.commitAction.send(DoCommit)
                            }

                        producer.cancelAndJoin()
                        consumer.cancelAndJoin()
                    }
                }

                events shouldContainAll dataInt
            }

            it("should not receive any data when all data is already committed") {

                val events = mutableListOf<Int>()

                Channels<Int,Int>(1).use { c ->

                    runBlocking {

                        // kick of asynchronous task for receiving data from kafka
                        val consumer = KafkaTopicConsumer.init<String, Int>(kCDetails)
                                .consumeAsync(c.kafkaEvents,c.commitAction,c.status)

                        if (c.status.receive() == Problem) return@runBlocking

                        var imPatience = 0

                        while (events.isEmpty() && imPatience < 10) {
                            c.kafkaEvents.poll()?.let {
                                events.add(it)
                                c.commitAction.send(DoCommit)
                            }
                            delay(waitPatience)
                            imPatience++
                        }
                        consumer.cancelAndJoin()
                    }
                }

                events.isEmpty() shouldEqualTo true
            }

            afterGroup {
                kEnv.tearDown()
            }
        }

        context("send ${data.size} string elements, receive and transform them") {

            beforeGroup {
                kEnv.start()
            }

            it("should receive ${data.size} string elements, transformed to uppercase") {

                val transformed = mutableListOf<String>()

                Channels<String,String>(2).use { c ->

                    runBlocking {

                        // kick of transformer
                        val transformer = eventTransformerAsync(
                                c.kafkaEvents,
                                c.transformedEvents,
                                { s -> s.toUpperCase() },
                                c.status)

                        if (c.status.receive() == Problem) return@runBlocking

                        // kick of asynchronous task for receiving data from kafka
                        val consumer = KafkaTopicConsumer.init<String, String>(kCDetails)
                                .consumeAsync(c.kafkaEvents,c.commitAction,c.status)

                        if (c.status.receive() == Problem) {
                            transformer.cancelAndJoin()
                            return@runBlocking
                        }

                        // kick of asynchronous task for sending data to kafka
                        val producer = KafkaTopicProducer.init<String,String>(kPDetails, "key").produceAsync(data)

                        while (transformed.size < data.size && (c.status.poll()?.let { it } != Problem))
                            c.transformedEvents.receive().also {
                                transformed.add(it)
                                c.commitAction.send(DoCommit)
                            }

                        producer.cancelAndJoin()
                        consumer.cancelAndJoin()
                        transformer.cancelAndJoin()
                    }
                }

                transformed shouldContainAll  data.map { it.toUpperCase() }
            }

            afterGroup {
                kEnv.tearDown()
            }
        }

        context("send ${dataInt.size} integer elements, receive and transform them") {

            beforeGroup {
                kEnv.start()
            }

            it("should receive ${data.size} integer elements, transformed to square") {

                val transformed = mutableListOf<Int>()

                Channels<Int,Int>(2).use { c ->

                    runBlocking {

                        // kick of transformer
                        val transformer = eventTransformerAsync(
                                c.kafkaEvents,
                                c.transformedEvents,
                                { s -> s * s },
                                c.status)

                        if (c.status.receive() == Problem) return@runBlocking

                        // kick of asynchronous task for receiving data from kafka
                        val consumer = KafkaTopicConsumer.init<String, Int>(kCDetails)
                                .consumeAsync(c.kafkaEvents,c.commitAction,c.status)

                        if (c.status.receive() == Problem) {
                            transformer.cancelAndJoin()
                            return@runBlocking
                        }

                        // kick of asynchronous task for sending data to kafka
                        val producer = KafkaTopicProducer.init<String,Int>(kPDetails, "key").produceAsync(dataInt)

                        while (transformed.size < data.size && (c.status.poll()?.let { it } != Problem))
                            c.transformedEvents.receive().also {
                                transformed.add(it)
                                c.commitAction.send(DoCommit)
                            }

                        producer.cancelAndJoin()
                        consumer.cancelAndJoin()
                        transformer.cancelAndJoin()
                    }
                }

                transformed shouldContainAll dataInt.map { it * it }
            }

            afterGroup {
                kEnv.tearDown()
            }
        }

        context("basic send ${data.size} string elements, receive, transform, and send to jms") {

            beforeGroup {
                kEnv.start()
            }

            it("should receive ${data.size} string elements, transformed to uppercase") {

                val manager = ManagePipeline.init<String,String,String>(
                        kCDetails,
                        { s -> s.toUpperCase() },
                        jmsDetails,
                        { s -> s }).manageAsync()

                val producer = KafkaTopicProducer.init<String,String>(kPDetails, "key").produceAsync(data)

                runBlocking {

                    // helper object to get jms queue size
                    val queueAsList = EmbeddedActiveMQ(jmsDetails).use {
                        while (it.queue.size < data.size) delay(waitPatience)
                        it.queue
                    }

                    producer.cancelAndJoin()
                    manager.cancelAndJoin()

                    queueAsList.map { (it as TextMessage).text }
                } shouldContainAll data.map { it.toUpperCase() }
            }

            afterGroup {
                kEnv.tearDown()
            }
        }

        context("basic send ${dataInt.size} integer elements, receive, transform, and send to jms") {

            beforeGroup {
                kEnv.start()
            }

            it("should receive ${dataInt.size} integer elements, transformed to square") {

                val manager = ManagePipeline.init<String,Int,String>(
                        kCDetails,
                        { s -> (s * s).toString() },
                        jmsDetails,
                        { s -> s }).manageAsync()

                val producer = KafkaTopicProducer.init<String,Int>(kPDetails, "key").produceAsync(dataInt)

                runBlocking {

                    // helper object to get jms queue size
                    val queueAsList = EmbeddedActiveMQ(jmsDetails).use {
                        while (it.queue.size < data.size) delay(waitPatience)
                        it.queue
                    }

                    producer.cancelAndJoin()
                    manager.cancelAndJoin()

                    queueAsList.map { (it as TextMessage).text.toInt() }
                } shouldContainAll dataInt.map { it * it }
            }

            afterGroup {
                kEnv.tearDown()
            }
        }
    }
})