package no.nav.integrasjon.test

import kotlinx.coroutines.experimental.cancelAndJoin
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
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

object KafkaTopicListenerSpec : Spek({

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

    // Setting up embedded Apache ActiveMQ and queue name to use
    val connectionFactory = ActiveMQConnectionFactory("vm://localhost?broker.persistent=false")
    val queueName = "kafkaEvents"

    describe("Kafka topic listener to transform to jms backend tests") {

        val data = (1..10).map {"data-$it"}
        val waitPatience = 100L

        xcontext("basic send ${data.size} data elements & receive them") {

            beforeGroup { kEnv.start() }

            val kafkaEvents = Channel<String>()
            val kListenerCommit = Channel<CommitAction>()

            it("should receive ${data.size} data elements") {

                //kick of asynchronous task for sending data to kafka
                val producer = kafkaTopicWriterAsync(kProdProps,topic,"key", data)

                // kick of asynchronous task for receiving data from kafka
                val consumer = kafkaTopicListenerAsync<String, String>(
                        kConsProps,
                        topic,
                        waitPatience,
                        kafkaEvents,
                        kListenerCommit)

                var events = mutableListOf<String>()

                runBlocking {

                    while (events.size < data.size) {
                        (kafkaEvents.poll())?.let {
                            events.add(it)
                            kListenerCommit.send(DoCommit)
                        }
                        delay(waitPatience)
                    }

                    producer.cancelAndJoin()
                    consumer.cancelAndJoin()
                }

                events shouldEqual data
            }

            it("should not receive any data when all data is already consumed") {

                // kick of asynchronous task for receiving data from kafka
                val consumer = kafkaTopicListenerAsync<String, String>(
                        kConsProps,
                        topic,
                        waitPatience,
                        kafkaEvents,
                        kListenerCommit)

                var imPatience = 0
                var events = mutableListOf<String>()

                runBlocking {

                    while (events.isEmpty() && imPatience < 10) {
                        (kafkaEvents.poll())?.let {
                            events.add(it)
                            kListenerCommit.send(DoCommit)
                        }
                        delay(waitPatience)
                        imPatience++
                    }
                    consumer.cancelAndJoin()
                }

                events.isEmpty() shouldEqualTo true
            }

            afterGroup {
                kEnv.tearDown()
                kafkaEvents.close()
                kListenerCommit.close()
            }
        }

        xcontext("basic send ${data.size} data elements, receive and transform them") {

            beforeGroup { kEnv.start() }

            val kafkaEvents = Channel<String>()
            val kListenerCommit = Channel<CommitAction>()
            val trfEvents = Channel<EventTransformed>()

            it("should receive ${data.size} data elements, transformed to uppercase") {

                // kick of transformer
                val transformer = eventTransformerAsync(kafkaEvents,trfEvents)

                // kick of asynchronous task for sending data to kafka
                val producer = kafkaTopicWriterAsync(kProdProps, topic, "key", data)

                // kick of asynchronous task for receiving data from kafka
                val consumer = kafkaTopicListenerAsync<String, String>(
                        kConsProps,
                        topic,
                        waitPatience,
                        kafkaEvents,
                        kListenerCommit)

                var events = mutableListOf<String>()

                runBlocking {

                    while (events.size < data.size) {
                        (trfEvents.poll())?.let {
                            events.add(it.value)
                            kListenerCommit.send(DoCommit)
                        }
                        delay(waitPatience)
                    }

                    producer.cancelAndJoin()
                    consumer.cancelAndJoin()
                    transformer.cancelAndJoin()
                }

                events shouldEqual data.map { it.toUpperCase() }
            }

            afterGroup {
                kEnv.tearDown()
                kafkaEvents.close()
                kListenerCommit.close()
                trfEvents.close()
            }
        }

        context("basic send ${data.size} data elements, receive, transform and to jms") {

            beforeGroup { kEnv.start() }

            val kafkaEvents = Channel<String>()
            val kListenerCommit = Channel<CommitAction>()
            val trfEvents = Channel<EventTransformed>()

            it("should receive ${data.size} data elements, transformed to uppercase") {

                // kick of asynchronous jms writer
                val jmsWriter = jmsWriterAsync(
                        connectionFactory,
                        queueName,
                        trfEvents,
                        kListenerCommit)

                // kick of transformer
                val transformer = eventTransformerAsync(kafkaEvents,trfEvents)

                // kick of asynchronous task for sending data to kafka
                val producer = kafkaTopicWriterAsync(kProdProps, topic, "key", data)

                // kick of asynchronous task for receiving data from kafka
                val consumer = kafkaTopicListenerAsync<String, String>(
                        kConsProps,
                        topic,
                        waitPatience,
                        kafkaEvents,
                        kListenerCommit)

                // help object to evaluate finish line
                val embMQ = EmbeddedActiveMQ(connectionFactory, queueName)

                runBlocking {

                    // check elements on queue
                    while (embMQ.qSize < data.size) {
                        delay(waitPatience)
                    }

                    producer.cancelAndJoin()
                    consumer.cancelAndJoin()
                    transformer.cancelAndJoin()
                    jmsWriter.cancelAndJoin()
                }

                embMQ.qSize shouldEqualTo data.size
            }

            afterGroup {
                kEnv.tearDown()
                kafkaEvents.close()
                kListenerCommit.close()
                trfEvents.close()
            }
        }
    }
})