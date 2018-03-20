@file:Suppress("UNCHECKED_CAST")

package no.nav.integrasjon.test

import kotlinx.coroutines.experimental.cancelAndJoin
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.withTimeoutOrNull
import no.nav.common.KafkaEnvironment
import no.nav.integrasjon.kafka.KafkaClientProperties
import no.nav.integrasjon.kafka.KafkaEvents
import no.nav.integrasjon.kafka.KafkaTopicConsumer
import no.nav.integrasjon.manager.Channels
import no.nav.integrasjon.manager.Problem
import no.nav.integrasjon.manager.Ready
import org.amshove.kluent.shouldContainAll
import org.amshove.kluent.shouldEqualTo
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.context
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import java.util.*
import no.nav.integrasjon.test.utils.D.kPData
import no.nav.integrasjon.test.utils.produceAndConsumeKTC

object KafkaTopicConsumerSpec : Spek({

    //val log = KotlinLogging.logger {  }

    // create the topics to be created in kafka env
    val topics = KafkaEvents.values().map { KafkaTopicConsumer.event2Topic(it) }

    val kEnv = KafkaEnvironment(topics = topics, withSchemaRegistry = true)

    // create a map of non-production kafka client properties

    val kCPPType = mutableMapOf<KafkaEvents, KafkaClientProperties>()

    KafkaEvents.values().filter { !it.value.production }.forEach {

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

    val waitPatience = 100L

    describe("KafkaTopicConsumer tests") {

        beforeGroup {
            kEnv.start()
        }

        context("send string elements to kafka and receive them") {

            it("should receive ${kPData[KafkaEvents.STRING]!!.size} string elements") {

                val data = kPData[KafkaEvents.STRING]!! as List<String>

                produceAndConsumeKTC(
                        kCPPType[KafkaEvents.STRING]!!,
                        "key",
                        data
                ) shouldContainAll data
            }

            it("should not receive any data when all data is already committed") {

                val events = mutableListOf<String>()

                Channels<String>(1).use { c ->

                    runBlocking {

                        // kick of asynchronous task for receiving data from kafka
                        val consumer = KafkaTopicConsumer.init<String, String>(kCPPType[KafkaEvents.STRING]!!)
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

            it("should receive ${kPData[KafkaEvents.INT]!!.size} integer elements") {

                val data = kPData[KafkaEvents.INT]!! as List<Int>

                produceAndConsumeKTC(
                        kCPPType[KafkaEvents.INT]!!,
                        "key",
                        data
                ) shouldContainAll data
            }

            it("should not receive any data when all data is already committed") {

                val events = mutableListOf<Int>()

                Channels<Int>(1).use { c ->

                    runBlocking {

                        // kick of asynchronous task for receiving data from kafka
                        val consumer = KafkaTopicConsumer.init<String, Int>(kCPPType[KafkaEvents.INT]!!)
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

            it("should receive ${kPData[KafkaEvents.AVRO]!!.size} integer elements") {

                val data = kPData[KafkaEvents.AVRO]!! as List<GenericRecord>

                produceAndConsumeKTC(
                        kCPPType[KafkaEvents.AVRO]!!,
                        "key",
                        data
                ) shouldContainAll data
            }
        }

        afterGroup {
            kEnv.tearDown()
        }
    }

})