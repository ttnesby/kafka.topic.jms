package no.nav.integrasjon.test

import kotlinx.coroutines.experimental.*
import no.nav.common.KafkaEnvironment
import no.nav.integrasjon.jms.ExternalAttachmentToJMS
import no.nav.integrasjon.jms.JMSProperties
import no.nav.integrasjon.jms.JMSTextMessageWriter
import no.nav.integrasjon.kafka.KafkaClientProperties
import no.nav.integrasjon.kafka.KafkaEvents
import no.nav.integrasjon.kafka.KafkaTopicConsumer
import no.nav.integrasjon.manager.ManagePipeline
import no.nav.integrasjon.test.utils.EmbeddedActiveMQ
import no.nav.integrasjon.test.utils.KafkaTopicProducer
import no.nav.integrasjon.test.utils.getFileAsString
import org.amshove.kluent.shouldContainAll
import org.amshove.kluent.shouldEqual
import org.amshove.kluent.shouldEqualTo
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.*
import java.io.File
import java.util.*
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

    val kPData = mutableMapOf<KafkaEvents, List<Any>>()

    kPData[KafkaEvents.STRING] = (1..100).map {"data-$it"}
    kPData[KafkaEvents.INT] = (1..100).map { it }

    val schema = Schema.Parser().parse(File("src/main/resources/external_attachment.avsc"))

    kPData[KafkaEvents.AVRO] = (1..100).map {
        GenericData.Record(schema).apply {
            put("batch", "batch-$it")
            put("sc", "sc-$it")
            put("sec", "sec-$it")
            put("archRef", "archRef-$it")
        }
    }

    kPData[KafkaEvents.MUSIC] = (1..100).map {
        GenericData.Record(schema).apply {
            put("batch", getFileAsString("src/test/resources/musicCatalog.xml"))
            put("sc", "TESTONLY")
            put("sec", "sec-$it")
            put("archRef", "archRef-$it")
        }
    }

    val dataOppf = mutableListOf<GenericRecord>()

     (1..25).forEach {
         dataOppf.add(GenericData.Record(schema).apply {
            put("batch", getFileAsString("src/test/resources/oppfolging_2913_02.xml"))
            put("sc","2913")
            put("sec","2")
            put("archRef","test")
         })

         dataOppf.add(GenericData.Record(schema).apply {
             put("batch", getFileAsString("src/test/resources/oppfolging_2913_03.xml"))
             put("sc","2913")
             put("sec","3")
             put("archRef","test")
         })

         dataOppf.add(GenericData.Record(schema).apply {
             put("batch", getFileAsString("src/test/resources/oppfolging_2913_04.xml"))
             put("sc","2913")
             put("sec","4")
             put("archRef","test")
         })

         dataOppf.add(GenericData.Record(schema).apply {
             put("batch", getFileAsString("src/test/resources/oppfolging_navoppfplan_rapportering_sykemeldte.xml"))
             put("sc","NavOppfPlan")
             put("sec","rapportering_sykemeldte")
             put("archRef","test")
         })
    }

    kPData[KafkaEvents.OPPFOLGINGSPLAN] = dataOppf

    kPData[KafkaEvents.BANKKONTONR] = (1..100).map {
        GenericData.Record(schema).apply {
            put("batch", getFileAsString("src/test/resources/bankkontonummer_2896_87.xml"))
            put("sc","2896")
            put("sec","87")
            put("archRef","test")
        }
    }

    kPData[KafkaEvents.MAALEKORT] = (1..100).map {
        GenericData.Record(schema).apply {
            put("batch", getFileAsString("src/test/resources/maalekort_4711_01.xml"))
            put("sc", "4711")
            put("sec", "1")
            put("archRef", "test")
        }
    }

    kPData[KafkaEvents.BARNEHAGELISTE] = (1..100).map {
        GenericData.Record(schema).apply {
            put("batch", getFileAsString("src/test/resources/barnehageliste_4795_01.xml"))
            put("sc", "4795")
            put("sec", "1")
            put("archRef", "test")
        }
    }

    val waitPatience = 100L
    val patienceLimit = 7_000L


    describe("Kafka topic listener transforming events to jms backend tests") {

        beforeGroup {
            kEnv.start()
        }

        context("send string elements, receive, transform, and send to jms") {

            it("should receive ${dataStr.size} string elements, transformed to uppercase") {

                val manager = ManagePipeline.init<String,String>(
                        kCPPType[KafkaEvents.STRING]!!, TrfString()).manageAsync()
                val producer = KafkaTopicProducer.init<String,String>(
                        kCPPType[KafkaEvents.STRING]!!, "key").produceAsync(dataStr)

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

                val manager = ManagePipeline.init<String,Int>(kCPPType[KafkaEvents.INT]!!, TrfInt()).manageAsync()
                val producer = KafkaTopicProducer.init<String,Int>(
                        kCPPType[KafkaEvents.INT]!!, "key").produceAsync(dataInt)

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

                val manager = ManagePipeline.init<String,GenericRecord>(
                        kCPPType[KafkaEvents.AVRO]!!, TrfAvro()).manageAsync()
                val producer = KafkaTopicProducer.init<String,GenericRecord>(
                        kCPPType[KafkaEvents.AVRO]!!,
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

        context("send music avro ext. attachment elements, receive, transform, and send to jms") {

            it("should receive ${dataMusic.size} elements, transformed to html") {

                val manager = ManagePipeline.init<String,GenericRecord>(
                        kCPPType[KafkaEvents.MUSIC]!!,
                        ExternalAttachmentToJMS(
                                jmsDetails,
                                KafkaEvents.MUSIC))
                        .manageAsync()

                val producer = KafkaTopicProducer.init<String,GenericRecord>(
                        kCPPType[KafkaEvents.MUSIC]!!,
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
                        kCPPType[KafkaEvents.OPPFOLGINGSPLAN]!!,
                        ExternalAttachmentToJMS(
                                jmsDetails,
                                KafkaEvents.OPPFOLGINGSPLAN))
                        .manageAsync()

                val producer = KafkaTopicProducer.init<String,GenericRecord>(
                        kCPPType[KafkaEvents.OPPFOLGINGSPLAN]!!,
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
                        kCPPType[KafkaEvents.MAALEKORT]!!,
                        ExternalAttachmentToJMS(
                                jmsDetails,
                                KafkaEvents.MAALEKORT))
                        .manageAsync()

                val producer = KafkaTopicProducer.init<String,GenericRecord>(
                        kCPPType[KafkaEvents.MAALEKORT]!!,
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
                } shouldEqual dataOther.size
            }
        }

        afterGroup {
            kEnv.tearDown()
        }
    }
})