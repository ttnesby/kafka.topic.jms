package no.nav.integrasjon.kafka

import org.apache.avro.generic.GenericRecord
import kotlin.reflect.KType
import kotlin.reflect.full.starProjectedType

/**
 * A couple of functions for setting the correct deserialize or serialize values in properties by use
 * of reflection
 */

fun getKafkaDeserializer(type: KType) = when (type) {
    String::class.starProjectedType -> "org.apache.kafka.common.serialization.StringDeserializer"
    Int::class.starProjectedType -> "org.apache.kafka.common.serialization.IntegerDeserializer"
    Long::class.starProjectedType -> "org.apache.kafka.common.serialization.LongDeserializer"
    Float::class.starProjectedType -> "org.apache.kafka.common.serialization.FloatDeserializer"
    Double::class.starProjectedType -> "org.apache.kafka.common.serialization.DoubleDeserializer"
    GenericRecord::class.starProjectedType -> "io.confluent.kafka.serializers.KafkaAvroDeserializer"
    else -> throw IllegalArgumentException("Invalid KType in getKafkaDeserializer")
}

fun getKafkaSerializer(type: KType) = when (type) {
    String::class.starProjectedType -> "org.apache.kafka.common.serialization.StringSerializer"
    Int::class.starProjectedType -> "org.apache.kafka.common.serialization.IntegerSerializer"
    Long::class.starProjectedType -> "org.apache.kafka.common.serialization.LongSerializer"
    Float::class.starProjectedType -> "org.apache.kafka.common.serialization.FloatSerializer"
    Double::class.starProjectedType -> "org.apache.kafka.common.serialization.DoubleSerializer"
    GenericRecord::class.starProjectedType -> "io.confluent.kafka.serializers.KafkaAvroSerializer"
    else -> throw IllegalArgumentException("Invalid KType in getKafkaSerializer")
}