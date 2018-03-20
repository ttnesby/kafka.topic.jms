package no.nav.integrasjon.kafka

/**
 * KafkaEvents is an enum class listing the supported type of kafka events in this application
 */

data class Scope(
        val production: Boolean,
        val name: String
)

enum class KafkaEvents(val value: Scope) {
    OPPFOLGINGSPLAN(Scope(true,"OPPFOLGINGSPLAN")),
    BANKKONTONR(Scope(true,"BANKKONTONR")),
    MAALEKORT(Scope(true,"MAALEKORT")),
    BARNEHAGELISTE(Scope(true,"BARNEHAGELISTE")),
    STRING(Scope(false,"STRING")),
    INT(Scope(false, "INT")),
    AVRO(Scope(false,"AVRO")),
    MUSIC(Scope(false,"MUSIC"))
}