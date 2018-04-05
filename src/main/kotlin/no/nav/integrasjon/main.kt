package no.nav.integrasjon

import com.ibm.mq.jms.MQConnectionFactory
import com.ibm.msg.client.wmq.WMQConstants
import com.ibm.msg.client.wmq.compat.base.internal.MQC
import no.nav.integrasjon.jms.JMSProperties
import no.nav.integrasjon.kafka.KafkaClientProperties
import no.nav.integrasjon.kafka.KafkaEvents
import java.util.*


fun main(args: Array<String>) {

    val fp = FasitProperties()

    if (fp.isEmpty) throw IllegalStateException("Missing fasit properties - $fp")

    val prodEvents = KafkaEvents.values().filter { it.value.production }

    if (fp.kafkaEvent !in prodEvents.map { it.value.name } )
        throw IllegalStateException("Incorrect kafka event as fasit property, ${fp.kafkaEvent} NOT IN " +
                "${prodEvents.map { it.value.name }}")

    // TODO - how to get the kafka brokers and schema reg?
    val kafkaProps = KafkaClientProperties(
            Properties(), // brokers, schema reg, client id
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
                clientReconnectOptions = WMQConstants.WMQ_CLIENT_RECONNECT // will try to reconnect
                clientReconnectTimeout = 600 // reconnection attempts for 10 minutes
                ccsid = 1208
                setIntProperty(WMQConstants.JMS_IBM_ENCODING, MQC.MQENC_NATIVE)
                setIntProperty(WMQConstants.JMS_IBM_CHARACTER_SET, 1208)
            },
            fp.outputQueueName,
            fp.mqUsername,
            fp.mqPassword
    )

    Bootstrap.invoke(kafkaProps, jmsProps)
}

