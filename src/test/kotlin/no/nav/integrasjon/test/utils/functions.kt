package no.nav.integrasjon.test.utils

import kotlinx.coroutines.experimental.cancelAndJoin
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.withTimeoutOrNull
import no.nav.integrasjon.Problem
import no.nav.integrasjon.Ready
import no.nav.integrasjon.Status
import no.nav.integrasjon.jms.ExternalAttachmentToJMS
import no.nav.integrasjon.jms.JMSProperties
import no.nav.integrasjon.kafka.KafkaClientProperties
import no.nav.integrasjon.kafka.KafkaEvents
import no.nav.integrasjon.kafka.KafkaTopicConsumer
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

    val toDownstream = Channel<V>()
    val status = Channel<Status>()

    //kick of asynchronous task for sending data to kafka
    val producer = KafkaTopicProducer.init<K, V>(
            cliProps, key).produceAsync(data)

    // kick of asynchronous task for receiving data from kafka
    KafkaTopicConsumer.init<K, V>(
            cliProps,
            toDownstream,
            status).use {

        runBlocking {

            withTimeoutOrNull(7_000L) {
                while (events.size < data.size && (status.poll()?.let { it } != Problem))
                    toDownstream.receive().also {
                        events.add(it)
                        status.send(Ready)
                    }
            }

            producer.cancelAndJoin()
        }
    }

    toDownstream.close()
    status.close()

    return events.toList()
}

fun produceToJMSMP(
        cliProps: KafkaClientProperties,
        jmsProps: JMSProperties,
        kafkaEvent: KafkaEvents,
        data: List<GenericRecord>): Int {

    val producer = KafkaTopicProducer.init<String, GenericRecord>(cliProps, "key").produceAsync(data)
    val status = Channel<Status>()
    var result = 0

    runBlocking {
        ExternalAttachmentToJMS(jmsProps, status, kafkaEvent).use { jms ->

            if (status.receive() == Problem) return@runBlocking

            KafkaTopicConsumer.init<String, GenericRecord>(cliProps, jms.data, status).use {

                EmbeddedActiveMQ(jmsProps).use { eMQ ->

                    withTimeoutOrNull(7_000) {
                        while (eMQ.queue.size < data.size) delay(100)
                    }
                    producer.cancelAndJoin()

                    result = eMQ.queue.size
                }
            }
        }
    }
    status.close()

    return result
}
