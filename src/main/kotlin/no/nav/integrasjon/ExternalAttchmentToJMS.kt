package no.nav.integrasjon

import org.apache.avro.generic.GenericRecord
import java.io.StringReader
import java.io.StringWriter
import javax.xml.transform.TransformerFactory

class ExternalAttchmentToJMS(
        jmsDetails: JMSDetails,
        xsltFilePath: String) : JMSTextMessageWriter<GenericRecord>(jmsDetails) {

    private val tFactory = TransformerFactory.newInstance()
    private val transformer = tFactory.newTransformer(
            javax.xml.transform.stream.StreamSource(xsltFilePath))

    override fun transform(event: GenericRecord): Result {

        val resultWriter = StringWriter()

        return try {
            transformer.transform(
                javax.xml.transform.stream.StreamSource(StringReader(event["batch"].toString())),
                javax.xml.transform.stream.StreamResult(resultWriter))
            Result(
                    status = true,
                    txtMsg = session.createTextMessage().apply {
                        this.text = resultWriter.toString()
                    }
            )
        }
        catch (e: Exception) {
            log.error("Exception during transform", e)
            Result(
                    status = false,
                    txtMsg = session.createTextMessage().apply { this.text = "Exception!" }
            )
        }
    }
}