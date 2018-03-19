package no.nav.integrasjon.test.utils

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

fun getFileAsString(filePath: String) = String(Files.readAllBytes(Paths.get(filePath)), StandardCharsets.UTF_8)

fun xmlOneliner(xml: String): String {

    var betweeTags = false
    val str = StringBuilder()

    tailrec fun iter(i: CharIterator): String = if (!i.hasNext()) str.toString() else {
        val c = i.nextChar()

        when(c) {
            '<' -> {
                betweeTags = false
                str.append(c)
            }
            '>' -> {
                betweeTags = true
                str.append(c)
            }
            ' ' -> if (!betweeTags) str.append(c)
            '\n' -> if (!betweeTags) str.append(c)
            else -> str.append(c)
        }
        iter(i)
    }

    return iter(xml.iterator())
}