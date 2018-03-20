package no.nav.integrasjon

/**
 * FasitProperties is a data class hosting relevant fasit properties required by this application
 */
data class FasitProperties(
    val mqQueueManagerName: String = System.getenv("MQGATEWAY04_NAME")?.toString() ?: "",
    val mqHostname: String = System.getenv("MQGATEWAY04_HOSTNAME")?.toString() ?: "",
    val mqPort: Int = System.getenv("MQGATEWAY04_PORT")?.toInt() ?: 0,
    val mqChannel: String = System.getenv("MQGATEWAY04_CHANNEL")?.toString() ?: "",
    val mqUsername: String = System.getenv("SRVAPPSERVER_USERNAME")?.toString() ?: "",
    val mqPassword: String = System.getenv("SRVAPPSERVER_PASSWORD")?.toString() ?: "",
    val outputQueueName: String = System.getenv("OUTPUT_QUEUE_NAME")?.toString() ?: "",
    val kafkaEvent: String = System.getenv("KAFKA_EVENT")?.toString() ?: "",

    val isEmpty: Boolean = mqQueueManagerName.isEmpty() && mqHostname.isEmpty() && mqPort == 0 && mqChannel.isEmpty()
            && outputQueueName.isEmpty() && kafkaEvent.isEmpty()
)