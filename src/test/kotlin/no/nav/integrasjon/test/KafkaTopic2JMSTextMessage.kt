package no.nav.integrasjon.test

import kotlinx.coroutines.experimental.cancelAndJoin
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
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


object KafkaTopic2JMSTextMessage : Spek({

    val topicStr = "testString"
    val topicInt = "testInt"

    val kEnv = KafkaEnvironment(3,topics = listOf(topicStr,topicInt))

    val kCDetailsStr = KafkaClientDetails(
            Properties().apply {
                set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kEnv.brokersURL)
                set(ConsumerConfig.CLIENT_ID_CONFIG, "kafkaTopicConsumer")
            },
            topicStr,
            250
    )

    val kCDetailsInt = KafkaClientDetails(
            Properties().apply {
                set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kEnv.brokersURL)
                set(ConsumerConfig.CLIENT_ID_CONFIG, "kafkaTopicConsumer")
            },
            topicInt,
            250
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

    val jmsDetails = JMSDetails(
            ActiveMQConnectionFactory("vm://localhost?broker.persistent=false"),
            "kafkaEvents"
    )

    class TrfString : JMSTextMessageWriter<String>(jmsDetails) {
        override fun transform(event: String): TextMessage = session.createTextMessage().apply {
            this.text = event.toUpperCase()
        }
    }

    class TrfInt : JMSTextMessageWriter<Int>(jmsDetails) {
        override fun transform(event: Int): TextMessage = session.createTextMessage().apply {
            this.text = (event * event).toString()
        }
    }

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

                Channels<String>(1).use { c ->

                    runBlocking {

                        // kick of asynchronous task for receiving data from kafka
                        val consumer = KafkaTopicConsumer.init<String, String>(kCDetailsStr)
                                .consumeAsync(c.kafkaEvents,c.commitAction,c.status)

                        if (c.status.receive() == Problem) return@runBlocking

                        //kick of asynchronous task for sending data to kafka
                        val producer = KafkaTopicProducer.init<String,String>(kPDetailsStr, "key").produceAsync(data)

                        while (events.size < data.size && (c.status.poll()?.let { it } != Problem))
                            c.kafkaEvents.receive().also {
                                events.add(it)
                                c.commitAction.send(DoCommit)
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
                //kEnv.tearDown()
            }
        }

        context("send ${dataInt.size} integer elements to kafka and receive them") {

            beforeGroup {
                //kEnv.start()
            }

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

                Channels<Int>(1).use { c ->

                    runBlocking {

                        // kick of asynchronous task for receiving data from kafka
                        val consumer = KafkaTopicConsumer.init<String, Int>(kCDetailsInt)
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
                //kEnv.tearDown()
            }
        }

        context("basic send ${data.size} string elements, receive, transform, and send to jms") {

            beforeGroup {
                //kEnv.start()
            }

            it("should receive ${data.size} string elements, transformed to uppercase") {

                val manager = ManagePipeline.init<String,String>(kCDetailsStr, TrfString()).manageAsync()
                val producer = KafkaTopicProducer.init<String,String>(kPDetailsInt, "key").produceAsync(data)

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
                //kEnv.tearDown()
            }
        }

        context("basic send ${dataInt.size} integer elements, receive, transform, and send to jms") {

            beforeGroup {
                //kEnv.start()
            }

            it("should receive ${dataInt.size} integer elements, transformed to square") {

                val manager = ManagePipeline.init<String,Int>(kCDetailsInt, TrfInt()).manageAsync()
                val producer = KafkaTopicProducer.init<String,Int>(kPDetailsInt, "key").produceAsync(dataInt)

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