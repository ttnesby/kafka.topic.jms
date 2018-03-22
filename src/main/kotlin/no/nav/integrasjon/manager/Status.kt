package no.nav.integrasjon.manager

sealed class Status
object Problem: Status()
object Ready: Status()
