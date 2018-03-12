package no.nav.integrasjon

import java.io.File
import javax.xml.transform.TransformerFactory
import java.io.StringWriter
import java.io.StringReader
import java.nio.file.Files
import java.util.stream.Collectors
import javax.xml.transform.OutputKeys


fun main(args: Array<String>) {

    try {
        val xml= Files.lines(File("src/test/resources/musicCatalog.xml").toPath())
                .collect(Collectors.joining("\n"))

        println(xml)

        val writer = StringWriter()
        val tFactory = TransformerFactory.newInstance()
        val transformer = tFactory.newTransformer(
                javax.xml.transform.stream.StreamSource("src/test/resources/musicCatalog.xsl")).apply {
            setOutputProperty(OutputKeys.INDENT, "yes")
        }

        transformer.transform(
                javax.xml.transform.stream.StreamSource(StringReader(xml)),
                javax.xml.transform.stream.StreamResult(writer))

        val result = writer.toString()

        println(result)

    } catch (e: Exception) {
        e.printStackTrace()
    }

}