package no.nav.integrasjon

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import java.io.File
import kotlin.reflect.full.starProjectedType

fun main(args: Array<String>) {

    val schemaFile = File("src/main/resources/external_attachment.avsc")
    val parser = Schema.Parser()
    val schema = parser.parse(schemaFile)

    val extAtt = GenericData.Record(schema)

    extAtt.apply {
        put("batch","test")
        put("sc","1")
        put("sec","1-1")
        put("archRef","abc")
    }

    println(GenericRecord::class.starProjectedType)

    println(extAtt.toString())

}