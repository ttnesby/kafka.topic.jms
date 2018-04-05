package no.nav.integrasjon

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import mu.KotlinLogging
import javax.management.MBeanServerConnection
import javax.management.ObjectName
import javax.management.remote.JMXConnectorFactory
import javax.management.remote.JMXServiceURL

class ApplicationMetrics(jmsMetric: ReceiveChannel<JMSMetric>) : AutoCloseable {

    private val serviceURL = "service:jmx:rmi:///jndi/rmi://:50367/jmxrmi"
    private val url = JMXServiceURL(serviceURL)
    private val jmxc = JMXConnectorFactory.connect(url, null)
            ?: throw IllegalStateException("Cannot connect to $serviceURL!")

    val collectorRegistry = CollectorRegistry.defaultRegistry
            ?: throw IllegalStateException("Cannot create prometheus collector registry!")

    private val asyncProcess: Job

    init {
        log.info { "Starting" }
        asyncProcess = kafkaMBeanAsync(jmsMetric)
    }

    override fun close() = runBlocking {
        log.info { "Closing" }
        asyncProcess.cancelAndJoin()
        log.info { "Closed" }
    }

    val isActive
        get() = asyncProcess.isActive

    private fun kafkaMBeanAsync(jmsMetric: ReceiveChannel<JMSMetric>) = async {

        fun getAttrValue(mbsc: MBeanServerConnection, beanName: ObjectName, attrName: String): Double =
                mbsc.getAttribute(beanName, attrName) as Double

        try {

            jmxc.use { jmxc ->

                val mbsc = jmxc.mBeanServerConnection

                delay(1_000) // wait a little for kafka Mbeans to be ready....

                val bnCFMM = ObjectName(
                        "kafka.consumer:type=consumer-fetch-manager-metrics,client-id=kafkaTopicConsumer")

                val bnCCM = ObjectName(
                        "kafka.consumer:type=consumer-coordinator-metrics,client-id=kafkaTopicConsumer")

                val attrCFMM = mbsc.getMBeanInfo(bnCFMM).attributes
                attrCFMM.sortBy { it.name }

                val attrCCM = mbsc.getMBeanInfo(bnCCM).attributes
                attrCCM.sortBy { it.name }

                val listGaugesCFMM = attrCFMM.map { attr ->
                    val name = attr.name.replace("-" ,"_").replace(".","_")
                    log.info { "Adding gauge $name" }
                    Gauge
                            .build(name, attr.description)
                            .namespace("kafka_metrics")
                            .register()
                }.toList()

                val listGaugesCCM = attrCCM.map { attr ->
                    val name = attr.name.replace("-" ,"_").replace(".","_")
                    log.info { "Adding gauge $name" }
                    Gauge
                            .build(name, attr.description)
                            .namespace("kafka_metrics")
                            .register()
                }.toList()

                log.info { "Adding counter sent_to_jms" }
                val sentToJMS = Counter.build("sent_to_jms", "No of TextMessages sent to JMS backend")
                        .namespace("jms_metrics").register()

                var allGood = true

                log.info("@start of kafkaMBeanAsync")

                while (isActive && allGood) {

                    //update gauges from kafka fetch manager metrics
                    listGaugesCFMM.forEachIndexed { index, gauge ->
                        try { gauge.set(getAttrValue(mbsc,bnCFMM,attrCFMM[index].name)) }
                        catch (e: Exception) {
                            log.error("Exception", e)
                            log.error("Prepare for shutdown")
                            allGood = false
                        }
                    }

                    //update gauges from kafka coordinator metric
                    listGaugesCCM.forEachIndexed { index, gauge ->
                        try { gauge.set(getAttrValue(mbsc, bnCCM, attrCCM[index].name)) }
                        catch (e: Exception) {
                            log.error("Exception", e)
                            log.error("Prepare for shutdown")
                            allGood = false
                        }
                    }

                    jmsMetric.poll()?.let { sentToJMS.inc() }

                    delay(100)
                }
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