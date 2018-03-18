package no.nav.integrasjon.jms

import mu.KotlinLogging
import java.io.StringReader
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamReader
import javax.xml.stream.events.XMLEvent

/**
 * This class is a sax parser restricted to Altinn ReceiveOnlineBatchExternalAttachment interface
 * See OnlineBatchReceiver.wsdl, OnlineBatchReceiver.xsd and genericbatch<version>.xsd
 *
 * In addition, there are more pragmatic restrictions according to how things are working today
 *  - Assuming only one DataBatch/DataUnits/DataUnit - ref genericbatch
 *  - Assuming only one Attachments/Attachment - ref genericbatch
 *
 * @param xmlFile is a string containing the batch from OnlineBatchReceiver.xsd
 * @constructor creates an object with public properties
 *
 * @property serviceCode - see genericbatch
 * @property reference - see genericbatch
 * @property formData - see genericbatch
 * @property attachment - see genericbatch
 * @property orgNo - from the FormData payload in genericBatch
 *
 * The properties are used as import parameters to src/main/resources/altinn2eifellesformat201803_16.xsl
 *
 * The supported set of Altinn service codes, service edition code, xsd
 * - 2913, 4, Oppfolgingsplan201607_2.xsd
 * - 2913, 3, Oppfolgingsplan2_4M.xsd
 * - 2913, 2, Oppfolgingsplan_Altinn.xsd
 * - NavOppfPlan, rapportering_sykemeldte, sbl_oppfolgingsplan_4.xsd
 * - 2896, 87, Bankkontonummer_M.xsd
 *
 */
class XMLExtractor(xmlFile: String) {

    // reader for genericbatch
    private val xRDBatch: XMLStreamReader = XMLInputFactory
            .newFactory().createXMLStreamReader(StringReader(xmlFile))

    // type of xml tag
    private enum class XType { ELEM, CDATA, ATTACH}

    // due to sax parsing, the elements to extract MUST BE IN ORDER
    val serviceCode = getElem<String>(xRDBatch,"ServiceCode", XType.ELEM)
    val reference = getElem<String>(xRDBatch,"Reference", XType.ELEM)
    val formData: String

    // NB!! Slight difference between Altinn messages and NAV message (re/mis use of Altinn interface)
    // FormData ending from Altinn messages - </ns4:melding>]]]]>><![CDATA[]]<![CDATA[>
    // FormData ending for NavOppfPlan - &lt;/OppfolgingsplanMetadata&gt;</FormData>

    init {
        formData = when(serviceCode) {
            "NavOppfPlan" -> getElem(
                    xRDBatch,
                    "FormData",
                    XType.CDATA,
                    "",
                    { r ->
                        val str = StringBuilder()
                        while (r.hasNext() && r.eventType != XMLEvent.END_ELEMENT) {
                            str.append(r.text)
                            r.next()
                        }
                        str.toString().trim()
                    }
            )
            else -> getElem(
                    xRDBatch,
                    "FormData",
                    XType.CDATA,
                    "",
                    { r ->
                        val str = StringBuilder()
                        while (r.hasNext() && r.eventType != XMLEvent.END_ELEMENT) {
                            str.append(r.text)
                            r.next()
                        }
                        str.toString().trim().dropLast(3)
                    }
            )
        }
    }

    data class Attachment(
            val archiveReference: String = "",
            val fileName: String = "",
            val fileContent: String = ""
    )

    val attachment = getElem(
            xRDBatch,
            "Attachment",
            XType.ATTACH,
            Attachment(),
            { r ->
                // read attributes before entering file content
                val archRef = r.getAttributeValue(0)
                val fName = r.getAttributeValue(1)
                r.next()
                Attachment(archRef, fName, r.text)
            }
    )

    // reader for the content in FormData
    private val xRFData: XMLStreamReader = XMLInputFactory
            .newFactory().createXMLStreamReader(StringReader(formData))

    // This is the only field extracted from FormData payload
    // NB!! There are slight differences in how the payloads are modelling orgno - see init below
    val orgNo: String

    init {

        orgNo = when(serviceCode) {
            "NavOppfPlan" -> getElem(xRFData,"bedriftsNr", XType.ELEM)
            "2896" -> getElem(xRFData,"organisasjonsnummer", XType.ELEM)
            else -> getElem(xRFData,"orgnr", XType.ELEM)
        }
    }

    // Accepting unchecked type casting for INTERNAL function

    private fun <T>getElem(
            xr: XMLStreamReader,
            name: String,
            type: XType,
            @Suppress("UNCHECKED_CAST") notFound: T = "" as T,
            @Suppress("UNCHECKED_CAST") found: (r: XMLStreamReader) -> T = { r -> r.text as T }
    ): T =
        try {
            tailrec fun iterElem(): T =

                    if (!xr.hasNext())
                        notFound
                    else {
                        xr.next()

                        if (xr.eventType == XMLEvent.START_ELEMENT && xr.localName == name)
                            when (type) {
                                XType.ELEM -> {
                                    xr.next() // go to content of element
                                    found(xr)
                                }
                                XType.CDATA -> {
                                    xr.next() // go to CDATA section
                                    found(xr)
                                }
                                XType.ATTACH -> found(xr) // must read attributes - no next
                            }
                        else
                            iterElem()
                    }

            iterElem()
        }
        catch (e: Exception) {
            log.error("Exception during xml sax parsing", e)
            notFound
        }

    companion object {
        private val log = KotlinLogging.logger {  }
    }
}