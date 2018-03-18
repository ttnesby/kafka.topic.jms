<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
				xmlns:fn="http://www.w3.org/2005/xpath-functions" 
                exclude-result-prefixes="fn" version="2.0">

    <xsl:output
        method="xml"
        version="1.1"
        encoding="UTF-8"
        indent="yes"
    />
    
    <!--
        This stylesheet is based on parameters
        A lot eaiser to handle the details in kotlin 
        versus xsl. 
        Continueous delivery makes code changes a lot easier...
        
        This xsl replace the following set of XSL`s from 'old' Altinnkanal
        - oppfolgingsplan201607_2
        - oppfolgingsplan2_4M
        - oppfolgingsplan_Altinn
        - sbl_oppfolgingsplan_v4
        - bankkontonummer_m
    -->
    
	<xsl:param name="ServiceCode"/>
	<xsl:param name="Reference"/>
    <xsl:param name="FormData"/>
    <xsl:param name="ArchiveReference"/>
    <xsl:param name="FileName"/>
    <xsl:param name="FileContent"/>    
    <xsl:param name="OrgNo"/>
    <xsl:param name="Guuid"/>
    
    <xsl:template match="/">

        <EI_fellesformat xmlns="http://www.nav.no/xml/eiff/2/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            <NavInnDokument xmlns="http://www.nav.no/xml/NavDokument/1/">
                <DokumentInfo>
                    <dokumentType>
                        <xsl:copy-of select="$ServiceCode" ></xsl:copy-of>
                    </dokumentType>
                    <dokumentreferanse>
                        <xsl:copy-of select="$Reference" ></xsl:copy-of>
                    </dokumentreferanse>
                    <dokumentDato>
                        <xsl:value-of select="current-dateTime()"></xsl:value-of>
                    </dokumentDato>
                </DokumentInfo>
                <Avsender>
                    <id>
                        <idNr><xsl:copy-of select="$OrgNo"></xsl:copy-of></idNr>
                        <idType>ENH</idType>
                    </id>
                </Avsender>
                <Godkjenner />
                <Innhold>
                   <xsl:value-of select="$FormData" disable-output-escaping="yes"/>
                </Innhold>
                <xsl:if test="string-length($FileContent) > 0">
                <Vedlegg>
                    <xsl:attribute name="referanse">
                        <xsl:copy-of select="$ArchiveReference"/>
                    </xsl:attribute>
                    <xsl:attribute name="filnavn">
                        <xsl:copy-of select="$FileName"/>
                    </xsl:attribute>
                    <xsl:copy-of select="$FileContent"/>
                </Vedlegg>
                </xsl:if>
            </NavInnDokument>
            <MottakenhetBlokk>
                <xsl:attribute name="ediLoggId">
                    <xsl:value-of select="concat('kafka2jms-',current-dateTime(),'-',$Guuid)"/>
                </xsl:attribute>
            </MottakenhetBlokk>
        </EI_fellesformat>
            
    </xsl:template>
</xsl:stylesheet>
