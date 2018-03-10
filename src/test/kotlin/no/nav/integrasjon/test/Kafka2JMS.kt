package no.nav.integrasjon.test

import kotlinx.coroutines.experimental.cancelAndJoin
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import mu.KotlinLogging
import no.nav.common.KafkaEnvironment
import no.nav.integrasjon.*
import no.nav.integrasjon.test.utils.EmbeddedActiveMQ
import no.nav.integrasjon.test.utils.kafkaTopicWriterAsync
import org.amshove.kluent.shouldEqual
import org.amshove.kluent.shouldEqualTo
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.context
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.xcontext
import java.util.*
import javax.jms.ConnectionFactory


object Kafka2JMS : Spek({

    val log = KotlinLogging.logger {  }

    val topic = "test"
    val kEnv = KafkaEnvironment(topics = listOf(topic))

    // The long way of config params for kafka producer and consumer
    val kProdProps = Properties().apply {
        set(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kEnv.brokersURL)
        set(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        set(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

        set(ProducerConfig.ACKS_CONFIG, "all")
        set(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1)

        set(ProducerConfig.CLIENT_ID_CONFIG, "kafkaTopicWriter")
    }

    val kConsProps = Properties().apply {
        set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kEnv.brokersURL)
        set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
        set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

        // guarantee for being alone for this topic
        set(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString())
        set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

        // only commit after successful put to MQ
        set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        set(ConsumerConfig.CLIENT_ID_CONFIG, "kafkaTopicListener")

        // poll only one record of
        set(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1)
    }

    val connectionFactory: ConnectionFactory = ActiveMQConnectionFactory("vm://localhost?broker.persistent=false")
    val queueName = "kafkaEvents"

    describe("Kafka topic listener to transform to jms backend tests") {

        val data = (1..10).map {"data-$it"}
        val waitPatience = 100L

        xcontext("send ${data.size} data elements to kafka and receive them") {

            beforeGroup {
                kEnv.start()
            }

            it("should receive ${data.size} data elements") {

                val events = mutableListOf<String>()

                Channels<String,String>(1).use { c ->

                    runBlocking {

                        // kick of asynchronous task for receiving data from kafka
                        val consumer = kafkaTopicListenerAsync<String, String>(
                                kConsProps,
                                topic,
                                waitPatience,
                                c.kafkaEvents,
                                c.commitAction,
                                c.status)

                        if (c.status.receive() == Problem) return@runBlocking

                        //kick of asynchronous task for sending data to kafka
                        val producer = kafkaTopicWriterAsync(kProdProps,topic,"key", data)

                        while (events.size < data.size && (c.status.poll()?.let { it } != Problem))
                            c.kafkaEvents.receive().also {
                                events.add(it)
                                c.commitAction.send(DoCommit)
                            }

                        producer.cancelAndJoin()
                        consumer.cancelAndJoin()
                    }
                }

                events shouldEqual data
            }

            it("should not receive any data when all data is already committed") {

                val events = mutableListOf<String>()

                Channels<String,String>(1).use { c ->

                    runBlocking {

                        // kick of asynchronous task for receiving data from kafka
                        val consumer = kafkaTopicListenerAsync<String, String>(
                                kConsProps,
                                topic,
                                waitPatience,
                                c.kafkaEvents,
                                c.commitAction,
                                c.status)

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

        xcontext("send ${data.size} data elements, receive and transform them") {

            beforeGroup {
                kEnv.start()
            }

            it("should receive ${data.size} data elements, transformed to uppercase") {

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
                        val consumer = kafkaTopicListenerAsync<String, String>(
                                kConsProps,
                                topic,
                                250,
                                c.kafkaEvents,
                                c.commitAction,
                                c.status)

                        if (c.status.receive() == Problem) {
                            transformer.cancelAndJoin()
                            return@runBlocking
                        }

                        // kick of asynchronous task for sending data to kafka
                        val producer = kafkaTopicWriterAsync(kProdProps, topic, "key", data)

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

                transformed shouldEqual data.map { it.toUpperCase() }
            }

            afterGroup {
                kEnv.tearDown()
            }
        }

        context("basic send ${data.size} data elements, receive, transform, and send to jms") {

            beforeGroup {
                kEnv.start()
            }

            it("should receive ${data.size} data elements, transformed to uppercase") {

                val manager = managePipelineAsync<String,String,String>(
                        kConsProps,
                        topic,
                        { s -> s.toUpperCase() },
                        connectionFactory,
                        queueName,
                        { s -> s })

                val producer = kafkaTopicWriterAsync(kProdProps, topic, "key", data)

                runBlocking {

                    // helper object to get jms queue size
                    val qSize = EmbeddedActiveMQ(connectionFactory, queueName).use {
                        while (it.qSize < data.size) delay(waitPatience)
                        it.qSize
                    }

                    producer.cancelAndJoin()
                    manager.cancelAndJoin()

                    qSize
                } shouldEqualTo data.size
            }

            afterGroup {
                kEnv.tearDown()
            }
        }
    }
})