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
 * In addition, there are additional pragmatic restrictions according to how things are working today
 *  - Assuming only one DataBatch/DataUnits/DataUnit - ref genericbatch with multiple
 *  - Assuming only one Attachments/Attachment - ref genericbatch with multiple
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
 * - 4711, 1, Maalekort
 * - 4796, 1, Barnehageliste
 */
class XMLExtractor(xmlFile: String) {

    // reader for genericbatch
    private val xRDBatch: XMLStreamReader = XMLInputFactory
            .newFactory().createXMLStreamReader(StringReader(xmlFile))

    // different types of XML elements to find
    private enum class XType { ELEM, CDATA, ATTACH}

    // due to sax parsing, the elements to extract !!MUST BE IN ORDER!!
    val serviceCode = getElem<String>(xRDBatch,"ServiceCode", XType.ELEM)
    val reference = getElem<String>(xRDBatch,"Reference", XType.ELEM)
    val formData: String

    // NB!! Slight difference between Altinn messages and NAV message
    // FormData ending from Altinn messages - </ns4:melding>]]]]>><![CDATA[]]<![CDATA[>
    // FormData ending for NavOppfPlan - &lt;/OppfolgingsplanMetadata&gt;</FormData> (reuse/misuse of Altinnn interface)

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

    // Attachment is a data class for hosting a couple of attributes and file content of attachment in Altinn message
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
        // avoid premature end of file message from stream reader if FormData is empty
        orgNo = if (formData.isNotEmpty()) when(serviceCode) {
            "NavOppfPlan" -> getElem(xRFData,"bedriftsNr", XType.ELEM)
            "2896" -> getElem(xRFData,"organisasjonsnummer", XType.ELEM)
            else -> getElem(xRFData,"orgnr", XType.ELEM)
        }
        else ""

        xRDBatch.close()
        xRFData.close()
    }

    /**
     * getElem is a private generic function extracting values from XML
     * @param T is the type of data to return. Either string (element value or CDATA) or Attachment
     * @param xr is the XMLStreamReader
     * @param name is the name of the tag to find
     * @param notFound is the value of type T to return if the tag is not found
     * @param found is a lambda function returning the data if tag is found
     * @return returning data of type T
     *
     * Accepting unchecked type casting due to full control internally
     */

    private fun <T>getElem(
            xr: XMLStreamReader,
            name: String,
            type: XType,
            @Suppress("UNCHECKED_CAST") notFound: T = "" as T,
            @Suppress("UNCHECKED_CAST") found: (r: XMLStreamReader) -> T = { r -> r.text as T }
    ): T =
        try {

            /**
             * iterElem is a tail recursive function
             * @return data of type T
             *
             * Just cruising downwards in the XMLStream (xr)
             * - look for the tag name, then apply the found function
             * - otherwise return notFounc
             */
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

            // invoke the function
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