package no.nav.integrasjon.test

import mu.KotlinLogging
import no.nav.integrasjon.jms.XMLExtractor
import org.amshove.kluent.shouldBeEqualTo
import org.amshove.kluent.shouldEqualTo
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.context
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

object XMLExtractorSpec : Spek({

    val log = KotlinLogging.logger {  }

    describe("XMLExtractor tests") {

        context("oppfolging and bank account no tests") {

            it("2913_04 should contain non-empty ServiceCode, Reference, FormData and OrgNo") {

                val xmlFile = String(
                        Files.readAllBytes(Paths.get("src/test/resources/oppfolging_2913_04.xml")),
                        StandardCharsets.UTF_8
                )

                val ex = XMLExtractor(xmlFile)

                log.debug { ex.formData }

                ex.serviceCode shouldBeEqualTo "2913"
                ex.reference shouldBeEqualTo "77423963"
                ex.formData.isNotEmpty() shouldEqualTo true
                ex.orgNo shouldBeEqualTo "987654321"
            }

            it("2913_03 should contain non-empty ServiceCode, Reference, FormData and OrgNo") {

                val xmlFile = String(
                        Files.readAllBytes(Paths.get("src/test/resources/oppfolging_2913_03.xml")),
                        StandardCharsets.UTF_8
                )

                val ex = XMLExtractor(xmlFile)

                log.debug { ex.formData }

                ex.serviceCode shouldBeEqualTo "2913"
                ex.reference shouldBeEqualTo "77423963"
                ex.formData.isNotEmpty() shouldEqualTo true
                ex.orgNo shouldBeEqualTo "987654321"
            }

            it("2913_02 should contain non-empty ServiceCode, Reference, FormData and OrgNo") {

                val xmlFile = String(
                        Files.readAllBytes(Paths.get("src/test/resources/oppfolging_2913_02.xml")),
                        StandardCharsets.UTF_8
                )

                val ex = XMLExtractor(xmlFile)

                log.debug { ex.formData }

                ex.serviceCode shouldBeEqualTo "2913"
                ex.reference shouldBeEqualTo "77426094"
                ex.formData.isNotEmpty() shouldEqualTo true
                ex.orgNo shouldBeEqualTo "973123456"
            }

            it("navoppfplan_... should contain non-empty ServiceCode, Reference, FormData, OrgNo and attachment") {

                val xmlFile = String(
                        Files.readAllBytes(Paths.get("src/test/resources/oppfolging_navoppfplan_rapportering_sykemeldte.xml")),
                        StandardCharsets.UTF_8
                )

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

            it("2896_87 should contain non-empty ServiceCode, Reference, FormData, OrgNo and attachment") {

                val xmlFile = String(
                        Files.readAllBytes(Paths.get("src/test/resources/bankkontonummer_2896_87.xml")),
                        StandardCharsets.UTF_8
                )

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
        }
    }
})