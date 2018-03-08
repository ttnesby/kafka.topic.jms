package no.nav.integrasjon

import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.util.*
import javax.jms.Session
import javax.jms.TextMessage

fun main(args: Array<String>) {

    // define kafka consumer properties

/*    val kafkaConsumerProps = Properties().apply {
        set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "") //TODO how to get a list of brokers
        set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
        set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

        // guarantee for being alone for this topic
        set(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString())
        set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

        // only commit after successful put to MQ
        set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        set(ConsumerConfig.CLIENT_ID_CONFIG, "kafka.topic.mq")

        // poll only one record at the time
        set(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1)
    }*/

    val connectionFactory = ActiveMQConnectionFactory("vm://localhost?broker.persistent=false")
    val connection = connectionFactory.createConnection()
    connection.start()

    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val q = session.createQueue("test")

    //session.createBrowser(q).enumeration.toList().size

    val p = session.createProducer(q)

    (1..100).forEach {
        p.send(session.createTextMessage("data_$it"))
    }

    p.close()

    val c = session.createConsumer(q)

    (1..100).forEach {
        val m = c.receive(1_000)
        m?.let { println((it as TextMessage).text) }
    }
    c.receiveNoWait()

    c.close()

    session.close()
    connection.close()
}