package no.nav.integrasjon

/**
 * JMSMetric is a simple sealed class with one value
 * - SentToJMS
 */
sealed class JMSMetric
object SentToJMS: JMSMetric()