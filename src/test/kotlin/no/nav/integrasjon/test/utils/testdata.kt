package no.nav.integrasjon.test.utils

import no.nav.integrasjon.kafka.KafkaEvents
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import java.io.File

object D {
    val kPData = mutableMapOf<KafkaEvents, List<Any>>().apply {

        this[KafkaEvents.STRING] = (1..100).map { "data-$it" }
        this[KafkaEvents.INT] = (1..100).map { it }

        val schema = Schema.Parser().parse(File("src/main/resources/external_attachment.avsc"))

        this[KafkaEvents.AVRO] = (1..100).map {
            GenericData.Record(schema).apply {
                put("batch", "batch-$it")
                put("sc", "sc-$it")
                put("sec", "sec-$it")
                put("archRef", "archRef-$it")
            }
        }

        this[KafkaEvents.MUSIC] = (1..100).map {
            GenericData.Record(schema).apply {
                put("batch", getFileAsString("src/test/resources/musicCatalog.xml"))
                put("sc", "TESTONLY")
                put("sec", "sec-$it")
                put("archRef", "archRef-$it")
            }
        }

        val dataOppf = mutableListOf<GenericRecord>()

        (1..500).forEach {
            dataOppf.add(GenericData.Record(schema).apply {
                put("batch", getFileAsString("src/test/resources/oppfolging_2913_02.xml"))
                put("sc", "2913")
                put("sec", "2")
                put("archRef", "test")
            })

            dataOppf.add(GenericData.Record(schema).apply {
                put("batch", getFileAsString("src/test/resources/oppfolging_2913_03.xml"))
                put("sc", "2913")
                put("sec", "3")
                put("archRef", "test")
            })

            dataOppf.add(GenericData.Record(schema).apply {
                put("batch", getFileAsString("src/test/resources/oppfolging_2913_04.xml"))
                put("sc", "2913")
                put("sec", "4")
                put("archRef", "test")
            })

            dataOppf.add(GenericData.Record(schema).apply {
                put("batch", getFileAsString("src/test/resources/oppfolging_navoppfplan_rapportering_sykemeldte.xml"))
                put("sc", "NavOppfPlan")
                put("sec", "rapportering_sykemeldte")
                put("archRef", "test")
            })
        }

        this[KafkaEvents.OPPFOLGINGSPLAN] = dataOppf

        this[KafkaEvents.BANKKONTONR] = (1..100).map {
            GenericData.Record(schema).apply {
                put("batch", getFileAsString("src/test/resources/bankkontonummer_2896_87.xml"))
                put("sc", "2896")
                put("sec", "87")
                put("archRef", "test")
            }
        }

        this[KafkaEvents.MAALEKORT] = (1..100).map {
            GenericData.Record(schema).apply {
                put("batch", getFileAsString("src/test/resources/maalekort_4711_01.xml"))
                put("sc", "4711")
                put("sec", "1")
                put("archRef", "test")
            }
        }

        this[KafkaEvents.BARNEHAGELISTE] = (1..100).map{
            GenericData.Record(schema).apply {
                put("batch", getFileAsString("src/test/resources/barnehageliste_4795_01.xml"))
                put("sc", "4795")
                put("sec", "1")
                put("archRef", "test")
            }
        }
    }
}