package no.nav.integrasjon.jms

import no.nav.integrasjon.kafka.KafkaEvents
import org.apache.avro.generic.GenericRecord
import java.io.StringReader
import java.io.StringWriter

class ExternalAttachmentToJMS(
        jmsProperties: JMSProperties,
        kafkaEvent: KafkaEvents) : JMSTextMessageWriter<GenericRecord>(jmsProperties) {

    private val xsltFilePath = when (kafkaEvent) {
        KafkaEvents.OPPFOLGINGSPLAN -> "src/main/resources/altinn2eifellesformat2018_03_16.xsl"
        KafkaEvents.BANKKONTONR -> "src/main/resources/altinn2eifellesformat2018_03_16.xsl"
        KafkaEvents.MUSIC -> "src/test/resources/musicCatalog.xsl"
        else -> ""
    }

    // Substituted TransformerFactory.newInstance() with saxon,
    // support for xsl version 2 and 3 with better support for diverse functions
    private val xFactory = net.sf.saxon.TransformerFactoryImpl()
    private val xslt = xFactory.newTransformer(javax.xml.transform.stream.StreamSource(xsltFilePath))

    override fun transform(event: GenericRecord): Result {

        val xml = event["batch"].toString()
        val xe = XMLExtractor(xml)

        return when(event["sc"].toString()) {

            // TESTONLY - see test cases
            "TESTONLY" -> xslTransform(xml)

            // bankkontonummer - no file attachment even though it's available...
            "2896" -> {
                xslt.apply {
                    setParameter("ServiceCode", xe.serviceCode)
                    setParameter("Reference", xe.reference)
                    setParameter("FormData", xe.formData)
                    setParameter("ArchiveReference", "")
                    setParameter("FileName", "")
                    setParameter("FileContent", "")
                    setParameter("OrgNo", xe.orgNo)
                    setParameter("Guuid", java.util.UUID.randomUUID().toString())
                }
                xslTransform(xml)
            }

            // Oppfolgingsplan service edition code 2,3,4 - no attachment available
            "2913" -> {
                xslt.apply {
                    setParameter("ServiceCode", xe.serviceCode)
                    setParameter("Reference", xe.reference)
                    setParameter("FormData", xe.formData)
                    setParameter("ArchiveReference", "")
                    setParameter("FileName", "")
                    setParameter("FileContent", "")
                    setParameter("OrgNo", xe.orgNo)
                    setParameter("Guuid", java.util.UUID.randomUUID().toString())
                }
                xslTransform(xml)
            }

            // Oppfolgingsplan service edition code rapportering-sykmeldte - with attachment
            "NavOppfPlan" -> {
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
                xslTransform(xml)
            }

            // Maalekort
            "4711" -> Result(
                    status = true,
                    txtMsg = session.createTextMessage().apply { this.text = xe.formData })

            // Barnehageliste
            "4795" -> Result(
                    status = true,
                    txtMsg = session.createTextMessage().apply { this.text = xe.formData })

            else -> {
                log.error("Unknown service code in transform")
                Result(
                        status = false,
                        txtMsg = session.createTextMessage().apply { this.text = "Unknown service code!" }
                )
            }
        }
    }

    private fun xslTransform(xml: String): Result {

        val resultWriter = StringWriter()

        return try {
            xslt.transform(
                    javax.xml.transform.stream.StreamSource(StringReader(xml)),
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
                    txtMsg = session.createTextMessage().apply { this.text = "Transform exception!" }
            )
        }
    }
}