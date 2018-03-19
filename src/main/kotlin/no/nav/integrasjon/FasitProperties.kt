package no.nav.integrasjon

class FasitProperties {
    val mqQueueManagerName = System.getenv("MQGATEWAY04_NAME")
    val mqHostname = System.getenv("MQGATEWAY04_HOSTNAME")
    val mqPort = System.getenv("MQGATEWAY04_PORT").toInt()
    val mqChannel = System.getenv("MQGATEWAY04_CHANNEL")
    val mqUsername = System.getenv("SRVAPPSERVER_USERNAME")
    val mqPassword = System.getenv("SRVAPPSERVER_PASSWORD")
    val outputQueueName = System.getenv("OUTPUT_QUEUE_NAME")
}