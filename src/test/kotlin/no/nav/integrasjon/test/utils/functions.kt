package no.nav.integrasjon.test.utils

import kotlinx.coroutines.experimental.cancelAndJoin
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.withTimeoutOrNull
import no.nav.integrasjon.jms.ExternalAttachmentToJMS
import no.nav.integrasjon.jms.JMSProperties
import no.nav.integrasjon.kafka.KafkaClientProperties
import no.nav.integrasjon.kafka.KafkaEvents
import no.nav.integrasjon.kafka.KafkaTopicConsumer
import no.nav.integrasjon.manager.Channels
import no.nav.integrasjon.manager.ManagePipeline
import no.nav.integrasjon.manager.Problem
import no.nav.integrasjon.manager.Ready
import org.apache.avro.generic.GenericRecord
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

fun getFileAsString(filePath: String) = String(Files.readAllBytes(Paths.get(filePath)), StandardCharsets.UTF_8)

fun xmlOneliner(xml: String): String {

    var betweeTags = false
    val str = StringBuilder()

    tailrec fun iter(i: CharIterator): String = if (!i.hasNext()) str.toString() else {
        val c = i.nextChar()

        when(c) {
            '<' -> {
                betweeTags = false
                str.append(c)
            }
            '>' -> {
                betweeTags = true
                str.append(c)
            }
            ' ' -> if (!betweeTags) str.append(c)
            '\n' -> if (!betweeTags) str.append(c)
            else -> str.append(c)
        }
        iter(i)
    }

    return iter(xml.iterator())
}

inline fun <reified K,reified V>produceAndConsumeKTC(
        cliProps: KafkaClientProperties,
        key: K,
        data: List<V>): List<V> {

    val events = mutableListOf<V>()

    Channels<V>(1).use { c ->

        runBlocking {

            // kick of asynchronous task for receiving data from kafka
            val consumer = KafkaTopicConsumer.init<K, V>(cliProps)
                    .consumeAsync(c.toDownstream,c.fromDownstream,c.toManager)

            if (c.toManager.receive() == Problem) return@runBlocking

            //kick of asynchronous task for sending data to kafka
            val producer = KafkaTopicProducer.init<K,V>(
                    cliProps, key).produceAsync(data)

            withTimeoutOrNull(7_000L) {
                while (events.size < data.size && (c.toManager.poll()?.let { it } != Problem))
                    c.toDownstream.receive().also {
                        events.add(it)
                        c.fromDownstream.send(Ready)
                    }
            }

            producer.cancelAndJoin()
            consumer.cancelAndJoin()
        }
    }

    return events.toList()
}

fun produceToJMSMP(
        cliProps: KafkaClientProperties,
        jmsProps: JMSProperties,
        kafkaEvent: KafkaEvents,
        data: List<GenericRecord>): Int {

    val manager = ManagePipeline.init<String, GenericRecord>(
            cliProps,
            ExternalAttachmentToJMS(jmsProps, kafkaEvent)
    ).manageAsync()

    val producer = KafkaTopicProducer.init<String, GenericRecord>(cliProps, "key").produceAsync(data)

    return runBlocking {
        EmbeddedActiveMQ(jmsProps).use { eMQ ->

            withTimeoutOrNull(7_000) {
                while (eMQ.queue.size < data.size && manager.isActive) delay(100)
            }

            producer.cancelAndJoin()
            manager.cancelAndJoin()

            eMQ.queue.size
        }
    }
}
