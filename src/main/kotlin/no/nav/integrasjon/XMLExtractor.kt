package no.nav.integrasjon

import java.io.StringReader
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamReader
import javax.xml.stream.events.XMLEvent

sealed class XMLType
object XElem : XMLType()
object XCDATA : XMLType()
object XAttr : XMLType()
object XAttach : XMLType()

class XMLExtractor(xmlFile: String) {


    private val xmlReader: XMLStreamReader = XMLInputFactory.newFactory().createXMLStreamReader(StringReader(xmlFile))

    val serviceCode = getElem("ServiceCode", XElem)
    val reference = getElem("Reference", XElem)
    val formData = getElem("FormData", XCDATA)


    fun getElem(name: String, type: XMLType): String =

        try {

            var strBuilder = StringBuilder()

            tailrec fun iterCDATA(): String =
                    if (!xmlReader.hasNext())
                        ""
                    else {
                        xmlReader.next()
                        if (xmlReader.eventType == XMLEvent.END_ELEMENT)
                            strBuilder.toString().trim().dropLast(3)
                        else {
                            strBuilder.append(xmlReader.text)
                            iterCDATA()
                        }
                    }

            tailrec fun iterElem(elem: String, type: XMLType): String =

                    if (!xmlReader.hasNext())
                        ""
                    else {
                        xmlReader.next()

                        if (xmlReader.eventType == XMLEvent.START_ELEMENT && xmlReader.localName == elem)
                            if (type == XElem) {
                                xmlReader.next()
                                xmlReader.text
                            } else iterCDATA()
                        else
                            iterElem(elem, type)
                    }

            iterElem(name, type)
        }
        catch (e: Exception) {
            //TODO log
            ""
        }

    companion object {



    }
}