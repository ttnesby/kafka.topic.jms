package no.nav.integrasjon

/**
 * Status is a simple sealed class with two values
 * - Ready for next message
 * - Problem has occurred
 */
sealed class Status
object Problem: Status()
object Ready: Status()
