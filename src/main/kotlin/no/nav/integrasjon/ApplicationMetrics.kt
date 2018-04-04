package no.nav.integrasjon

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Gauge
import kotlinx.coroutines.experimental.*
import mu.KotlinLogging
import javax.management.MBeanServerConnection
import javax.management.ObjectName
import javax.management.remote.JMXConnectorFactory
import javax.management.remote.JMXServiceURL

class ApplicationMetrics : AutoCloseable {

    private val serviceURL = "service:jmx:rmi:///jndi/rmi://:50367/jmxrmi"
    private val url = JMXServiceURL(serviceURL)
    private val jmxc = JMXConnectorFactory.connect(url, null)
            ?: throw IllegalStateException("Cannot connect to $serviceURL!")

    val collectorRegistry = CollectorRegistry.defaultRegistry
            ?: throw IllegalStateException("Cannot create prometheus collector registry!")

    private val asyncProcess: Job

    init {
        log.info { "Starting" }
        asyncProcess = kafkaMBeanAsync()
    }

    override fun close() = runBlocking {
        log.info { "Closing" }
        asyncProcess.cancelAndJoin()
        log.info { "Closed" }
    }

    val isActive
        get() = asyncProcess.isActive

    private fun kafkaMBeanAsync() = async {

        fun getAttrValue(mbsc: MBeanServerConnection, beanName: ObjectName, attrName: String): Double =
                mbsc.getAttribute(beanName, attrName) as Double

        try {

            jmxc.use { jmxc ->

                val mbsc = jmxc.mBeanServerConnection

                delay(1_000) // wait a little for kafka Mbeans to be ready....

                val beanName = ObjectName("kafka.consumer:type=consumer-fetch-manager-metrics,client-id=kafkaTopicConsumer")
                val kc = mbsc.getMBeanInfo(beanName)

                val attrs = kc.attributes
                attrs.sortBy { it.name }

                val listGauges =attrs.map {attr ->
                    log.info { "Adding gauge ${attr.name.replace("-","_").replace(".","_")}" }
                    Gauge.build(
                            attr.name.replace("-","_").replace(".","_"),
                            attr.description).namespace("kafka_metrics").register()
                }.toList()

                var allGood = true

                log.info("@start of kafkaMBeanAsync")

                while (isActive && allGood) {

                    listGauges.forEachIndexed { index, gauge ->
                        try { gauge.set(getAttrValue(mbsc,beanName,attrs[index].name)) }
                        catch (e: Exception) {
                            log.error("Exception", e)
                            log.error("Prepare for shutdown")
                            allGood = false
                        }
                    }
                    delay(250)
                }
                //attrs.forEach { str.append("${it.name} = ${getAttrValue(mbsc, beanName, it.name)}\n") }
            }
        }
        catch (e: Exception) {
            when (e) {
                is JobCancellationException -> {/* it's ok to be cancelled by manager*/ }
                else -> log.error("Exception", e)
            }
        }
        finally {
            log.info("@end of kafkaMBeanAsync - goodbye!")
        }
    }

    companion object {
        private val log = KotlinLogging.logger { }
    }
}