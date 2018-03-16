package no.nav.integrasjon

import mu.KotlinLogging
import java.io.StringReader
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamReader
import javax.xml.stream.events.XMLEvent

sealed class XMLType
object XElem : XMLType()
object XCDATA : XMLType()

/**
 * This class is a sax parser restricted to Altinn ReceiveOnlineBatchExternalAttachment ONLY
 * Limited to certain use cases
 */
class XMLExtractor(xmlFile: String) {


    // reader for the Altinn attachment message - see batch in ReceiveOnlineBatchExternalAttachment
    private val xRDBatch: XMLStreamReader = XMLInputFactory
            .newFactory().createXMLStreamReader(StringReader(xmlFile))

    // due to sax parsing, the elements order is VITAL
    val serviceCode = getElem(xRDBatch,"ServiceCode", XElem)
    val reference = getElem(xRDBatch,"Reference", XElem)
    val formData = getElem(xRDBatch,"FormData", XCDATA)

    data class Attachment(
            val archiveReference: String = "",
            val fileName: String = "",
            val fileContent: String = ""
    )

    val attachment = getAttach(xRDBatch,"Attachment")


    // reader for the FormData section
    private val xRFData: XMLStreamReader = XMLInputFactory
            .newFactory().createXMLStreamReader(StringReader(formData))

    val orgNo: String

    init {

        orgNo = if (serviceCode == "NavOppfPlan")
            getElem(xRFData,"bedriftsNr", XElem)
        else
            getElem(xRFData,"orgnr", XElem)
    }


    private fun getElem(xr: XMLStreamReader, name: String, type: XMLType): String =

        try {

            val strBuilder = StringBuilder()

            tailrec fun iterCDATA(): String =
                    if (!xr.hasNext())
                        ""
                    else {
                        xr.next()
                        if (xr.eventType == XMLEvent.END_ELEMENT)
                            strBuilder.toString().trim().dropLast(3)
                        else {
                            strBuilder.append(xr.text)
                            iterCDATA()
                        }
                    }

            tailrec fun iterElem(elem: String, type: XMLType): String =

                    if (!xr.hasNext())
                        ""
                    else {
                        xr.next()

                        if (xr.eventType == XMLEvent.START_ELEMENT && xr.localName == elem)
                            when (type) {
                                XElem -> {
                                    xr.next()
                                    xr.text
                                }
                                XCDATA ->  iterCDATA()
                            }
                        else
                            iterElem(elem, type)
                    }

            iterElem(name, type)
        }
        catch (e: Exception) {
            log.error("Exception during xml sax parsing", e)
            ""
        }

    private fun getAttach(xr: XMLStreamReader, elem: String): Attachment =
            try{
                tailrec fun iter(): Attachment =
                        if (!xr.hasNext())
                            Attachment()
                        else {
                            xr.next()

                            if (xr.eventType == XMLEvent.START_ELEMENT && xr.localName == elem) {
                                // read attributes before entering file content
                                val archRef = xr.getAttributeValue(0)
                                val fName = xr.getAttributeValue(1)
                                xr.next()
                                Attachment(
                                        archRef,
                                        fName,
                                        xr.text
                                )

                            }
                            else
                                iter()
                        }
                iter()
            }
            catch (e: Exception) {
                log.error("Exception during xml sax parsing", e)
                Attachment()
            }

    companion object {
        private val log = KotlinLogging.logger {  }
    }
}