package no.nav.integrasjon.test

import mu.KotlinLogging
import no.nav.integrasjon.jms.XMLExtractor
import no.nav.integrasjon.test.utils.getFileAsString
import org.amshove.kluent.shouldBeEqualTo
import org.amshove.kluent.shouldEqualTo
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.context
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it

object XMLExtractorSpec : Spek({

    val log = KotlinLogging.logger {  }

    describe("XMLExtractor tests") {

        context("iterate all types of altinn message payloads") {

            // oppfolging
            it("2913_04 should contain non-empty ServiceCode, Reference, FormData and OrgNo") {

                val xmlFile = getFileAsString("src/test/resources/oppfolging_2913_04.xml")

                val ex = XMLExtractor(xmlFile)

                log.debug { ex.formData }

                ex.serviceCode shouldBeEqualTo "2913"
                ex.reference shouldBeEqualTo "77423963"
                ex.formData.isNotEmpty() shouldEqualTo true
                ex.orgNo shouldBeEqualTo "987654321"
            }

            // oppfolging
            it("2913_03 should contain non-empty ServiceCode, Reference, FormData and OrgNo") {

                val xmlFile = getFileAsString("src/test/resources/oppfolging_2913_03.xml")

                val ex = XMLExtractor(xmlFile)

                log.debug { ex.formData }

                ex.serviceCode shouldBeEqualTo "2913"
                ex.reference shouldBeEqualTo "77423963"
                ex.formData.isNotEmpty() shouldEqualTo true
                ex.orgNo shouldBeEqualTo "987654321"
            }

            // oppfolging
            it("2913_02 should contain non-empty ServiceCode, Reference, FormData and OrgNo") {

                val xmlFile = getFileAsString("src/test/resources/oppfolging_2913_02.xml")

                val ex = XMLExtractor(xmlFile)

                log.debug { ex.formData }

                ex.serviceCode shouldBeEqualTo "2913"
                ex.reference shouldBeEqualTo "77426094"
                ex.formData.isNotEmpty() shouldEqualTo true
                ex.orgNo shouldBeEqualTo "973123456"
            }

            // oppfolging
            it("navoppfplan_... should contain non-empty ServiceCode, Reference, FormData, " +
                    "OrgNo and attachment") {

                val xmlFile = getFileAsString(
                        "src/test/resources/oppfolging_navoppfplan_rapportering_sykemeldte.xml")

                val ex = XMLExtractor(xmlFile)

                log.debug { ex.formData }
                log.debug { ex.attachment.fileContent }

                ex.serviceCode shouldBeEqualTo "NavOppfPlan"
                ex.reference shouldBeEqualTo "rapportering-sykmeldte"
                ex.formData.isNotEmpty() shouldEqualTo true
                ex.attachment.archiveReference shouldBeEqualTo "170314125626-974114127"
                ex.attachment.fileName shouldBeEqualTo "20170314114144191.pdf"
                ex.attachment.fileContent.isNotEmpty() shouldEqualTo true
                ex.orgNo shouldBeEqualTo "90012345"
            }

            // bankkontonr
            it("2896_87 should contain non-empty ServiceCode, Reference, FormData, OrgNo and attachment") {

                val xmlFile = getFileAsString("src/test/resources/bankkontonummer_2896_87.xml")

                val ex = XMLExtractor(xmlFile)

                log.debug { ex.formData }
                log.debug { ex.attachment.fileContent }

                ex.serviceCode shouldBeEqualTo "2896"
                ex.reference shouldBeEqualTo "77424064"
                ex.formData.isNotEmpty() shouldEqualTo true
                ex.attachment.archiveReference shouldBeEqualTo "AR186469935"
                ex.attachment.fileName shouldBeEqualTo "PDF_186469935.pdf"
                ex.attachment.fileContent.isNotEmpty() shouldEqualTo true
                ex.orgNo shouldBeEqualTo "973094718"
            }

            // maalekort
            it("4711_01 should contain non-empty ServiceCode, Reference, FormData and attach.") {

                val xmlFile = getFileAsString("src/test/resources/maalekort_4711_01.xml")

                val ex = XMLExtractor(xmlFile)

                log.debug { ex.formData }

                ex.serviceCode shouldBeEqualTo "4711"
                ex.reference shouldBeEqualTo "77424064"
                ex.formData.isNotEmpty() shouldEqualTo true
                ex.attachment.archiveReference shouldBeEqualTo "AR186469935"
                ex.attachment.fileName shouldBeEqualTo "PDF_186469935.pdf"
                ex.attachment.fileContent.isNotEmpty() shouldEqualTo true
                ex.orgNo shouldBeEqualTo ""
            }

            // barnehageliste
            it("4795_01 should contain non-empty ServiceCode, Reference, FormData and attach.") {

                val xmlFile = getFileAsString("src/test/resources/barnehageliste_4795_01.xml")

                val ex = XMLExtractor(xmlFile)

                log.debug { ex.formData }

                ex.serviceCode shouldBeEqualTo "4795"
                ex.reference shouldBeEqualTo "77424064"
                ex.formData.isNotEmpty() shouldEqualTo true
                ex.attachment.archiveReference shouldBeEqualTo "AR186469935"
                ex.attachment.fileName shouldBeEqualTo "PDF_186469935.pdf"
                ex.attachment.fileContent.isNotEmpty() shouldEqualTo  true
                ex.orgNo shouldBeEqualTo ""
            }
        }
    }
})