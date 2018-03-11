package no.nav.integrasjon

import kotlin.reflect.KType
import kotlin.reflect.full.starProjectedType

fun getKafkaDeserializer(type: KType) = when (type) {
    String::class.starProjectedType -> "org.apache.kafka.common.serialization.StringDeserializer"
    Int::class.starProjectedType -> "org.apache.kafka.common.serialization.IntegerDeserializer"
    Long::class.starProjectedType -> "org.apache.kafka.common.serialization.LongDeserializer"
    Float::class.starProjectedType -> "org.apache.kafka.common.serialization.FloatDeserializer"
    Double::class.starProjectedType -> "org.apache.kafka.common.serialization.DoubleDeserializer"
    Byte::class.starProjectedType -> "org.apache.kafka.common.serialization.BytesDeserializer"
    else -> throw IllegalArgumentException("Invalid KType in getKafkaDeserializer")
}

fun getKafkaSerializer(type: KType) = when (type) {
    String::class.starProjectedType -> "org.apache.kafka.common.serialization.StringSerializer"
    Int::class.starProjectedType -> "org.apache.kafka.common.serialization.IntegerSerializer"
    Long::class.starProjectedType -> "org.apache.kafka.common.serialization.LongSerializer"
    Float::class.starProjectedType -> "org.apache.kafka.common.serialization.FloatSerializer"
    Double::class.starProjectedType -> "org.apache.kafka.common.serialization.DoubleSerializer"
    Byte::class.starProjectedType -> "org.apache.kafka.common.serialization.BytesSerializer"
    else -> throw IllegalArgumentException("Invalid KType in getKafkaSerializer")
}