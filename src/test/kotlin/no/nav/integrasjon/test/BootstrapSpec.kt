@file:Suppress("UNCHECKED_CAST")

package no.nav.integrasjon.test

import com.ibm.mq.jms.MQConnectionFactory
import com.ibm.msg.client.wmq.WMQConstants
import com.ibm.msg.client.wmq.compat.base.internal.MQC
import no.nav.integrasjon.Bootstrap
import no.nav.integrasjon.FasitProperties
import no.nav.integrasjon.jms.JMSProperties
import no.nav.integrasjon.kafka.KafkaClientProperties
import no.nav.integrasjon.kafka.KafkaEvents
import no.nav.integrasjon.test.utils.D
import no.nav.integrasjon.test.utils.KafkaTopicProducer
import org.amshove.kluent.shouldEqualTo
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.xdescribe
import java.util.*

/**
 * This object tests the boostrap object
 * Prerequities
 * - local kafka environment running using default ports
 * - local mq running using Docker image from IBM, using default settings
 */

object BootstrapSpec : Spek({

    // simulate the main function - setting fasit properties manually

    val fp = FasitProperties(
            mqQueueManagerName = "QM1",
            mqHostname = "localhost",
            mqPort = 1414,
            mqChannel = "DEV.APP.SVRCONN",
            mqUsername = "app",
            mqPassword = "",
            outputQueueName = "DEV.QUEUE.2",
            kafkaEvent = "OPPFOLGINGSPLAN" // see KafkaTopicConsumer::event2Topic
    )

    val kafkaProps = KafkaClientProperties(
            Properties().apply {
                set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://localhost:9092")
                set("schema.registry.url","http://localhost:8081")
                set(ConsumerConfig.CLIENT_ID_CONFIG, "kafkaTopicConsumer")
            },
            KafkaEvents.valueOf(fp.kafkaEvent)
    )

    // set relevant jms properties
    val jmsProps = JMSProperties(
            MQConnectionFactory().apply {
                hostName = fp.mqHostname
                port = fp.mqPort
                queueManager = fp.mqQueueManagerName
                channel = fp.mqChannel
                transportType = WMQConstants.WMQ_CM_CLIENT
                connectionNameList = "localhost(${fp.mqPort}),localhost(1415)"
                //ccdturl = TODO - how to get this from the MQ manager...
                clientReconnectOptions = WMQConstants.WMQ_CLIENT_RECONNECT_Q_MGR // will try to reconnect
                clientReconnectTimeout = 60 // reconnection attempts for 5 minutes
                ccsid = 1208
                setIntProperty(WMQConstants.JMS_IBM_ENCODING, MQC.MQENC_NATIVE)
                setIntProperty(WMQConstants.JMS_IBM_CHARACTER_SET, 1208)
            },
            fp.outputQueueName,
            fp.mqUsername,
            fp.mqPassword
    )

    val data = D.kPData[KafkaEvents.OPPFOLGINGSPLAN]!! as List<GenericRecord>
    val prodProps = KafkaClientProperties(
            Properties().apply {
                set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://localhost:9092")
                set("schema.registry.url","http://localhost:8081")
                set(ConsumerConfig.CLIENT_ID_CONFIG, "kafkaTopicProducer")
            },
            KafkaEvents.valueOf(fp.kafkaEvent)
    )

    // trigger the producer
/*    KafkaTopicProducer.init<String,GenericRecord>(
            prodProps,
            "key",
            untilShutdown = true,
            delayTime = 1_000).produceAsync(data)*/

    xdescribe("Test of boostrap") {
        it("Just starting boostrap") {

            Bootstrap.invoke(kafkaProps, jmsProps)

            true shouldEqualTo true
        }
    }

})