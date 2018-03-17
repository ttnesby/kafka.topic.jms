package no.nav.integrasjon

import org.apache.avro.generic.GenericRecord
import java.io.StringReader
import java.io.StringWriter

class ExternalAttachmentToJMS(
        jmsDetails: JMSDetails,
        xsltFilePath: String) : JMSTextMessageWriter<GenericRecord>(jmsDetails) {

    //Substituted TransformerFactory.newInstance() with saxon,
    // support for xsl version 2 and 3
    private val xFactory = net.sf.saxon.TransformerFactoryImpl()
    private val xslt = xFactory.newTransformer(
            javax.xml.transform.stream.StreamSource(xsltFilePath))

    override fun transform(event: GenericRecord): Result {

        if (event["sc"].toString() in listOf("2913","NavOppfPlan")) {

            val xe = XMLExtractor(event["batch"].toString())

            // prepare for parameters to the xsl document, programmatic is easier than xls...
            xslt.apply {
                setParameter("ServiceCode", xe.serviceCode)
                setParameter("Reference", xe.reference)
                setParameter("FormData", xe.formData)
                setParameter("ArchiveReference", xe.attachment.archiveReference)
                setParameter("FileName", xe.attachment.fileName)
                setParameter("FileContent", xe.attachment.fileContent)
                setParameter("OrgNo", xe.orgNo)
                setParameter("Guuid", java.util.UUID.randomUUID().toString())
            }
        }

        val resultWriter = StringWriter()

        return try {
            xslt.transform(
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