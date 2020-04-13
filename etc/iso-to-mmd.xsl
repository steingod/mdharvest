<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet version="1.0" 
    xmlns="http://www.met.no/schema/mmd" 
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
    xmlns:gco="http://www.isotc211.org/2005/gco" 
    xmlns:gmd="http://www.isotc211.org/2005/gmd"
    xmlns:gmi="http://www.isotc211.org/2005/gmi"
    xmlns:gml="http://www.opengis.net/gml/3.2"
    xmlns:srv="http://www.isotc211.org/2005/srv"
    xmlns:mmd="http://www.met.no/schema/mmd"
    xmlns:mapping="http://www.met.no/schema/mmd/iso2mmd">

    <xsl:output method="xml" encoding="UTF-8" indent="yes" />

    <!--
    <xsl:template match="/[name() = 'gmd:MD_Metadata' or name() = 'gmi:MI_Metadata']">
    -->
    <xsl:template match="gmd:MD_Metadata | gmi:MI_Metadata">
        <xsl:element name="mmd:mmd">
            <xsl:apply-templates select="gmd:fileIdentifier/gco:CharacterString" />
            <xsl:apply-templates select="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation" />
            <xsl:apply-templates select="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:abstract" />
            <xsl:element name="mmd:metadata_status">Active</xsl:element>
            <xsl:apply-templates select="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:status"/>
            <!-- If /gmd:MD_Metadata/gmd:status is not available, check
                 further and add default -->
<!-- for test purposes...
            <mmd:dataset_production_status>In Work</mmd:dataset_production_status>
-->
            <xsl:element name="mmd:collection">ADC</xsl:element>
            <xsl:apply-templates select="gmd:dateStamp" />
            <xsl:apply-templates select="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent" />
            <xsl:apply-templates select="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:topicCategory/gmd:MD_TopicCategoryCode" />
            <xsl:apply-templates select="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords[./gmd:keyword/gmd:type/gmd:MD_KeywordTypeCode = 'project']" />
            <xsl:element name="mmd:keywords">
                <xsl:attribute name="vocabulary">gcmd</xsl:attribute>
                <xsl:apply-templates select="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:keyword/gco:CharacterString[contains(.,'EARTH SCIENCE &gt;')]" />
            </xsl:element>
            <xsl:element name="mmd:keywords">
                <xsl:attribute name="vocabulary">none</xsl:attribute>
                <xsl:apply-templates select="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:keyword/gco:CharacterString[not(contains(.,'EARTH SCIENCE &gt;'))]" />
            </xsl:element>
            <!--
            <mmd:metadata_version>1</mmd:metadata_version>
            -->
            <xsl:apply-templates select="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:language" />
            
            <xsl:apply-templates select="gmd:contact/gmd:CI_ResponsibleParty" />
            
            
            <xsl:element name="mmd:geographic_extent">
                <xsl:apply-templates select="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox" />
                <xsl:apply-templates select="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_BoundingPolygon/gmd:polygon" />
            </xsl:element>
            
            <xsl:apply-templates select="gmd:distributionInfo/gmd:MD_Distribution/gmd:distributor/gmd:MD_Distributor" />
            <xsl:apply-templates select="gmd:distributionInfo/gmd:MD_Distribution/gmd:transferOptions/gmd:MD_DigitalTransferOptions/gmd:onLine" />
            <xsl:apply-templates select="gmd:identificationInfo/srv:SV_ServiceIdentification/srv:containsOperations/srv:SV_OperationMetadata/srv:connectPoint" />

            <xsl:apply-templates select="gmd:dataSetURI/gco:CharacterString" />
            <xsl:apply-templates select="gmd:distributionInfo/gmd:MD_Distribution/gmd:transferOptions/gmd:MD_DigitalTransferOptions/gmd:onLine/gmd:CI_OnlineResource" />
            
            <xsl:apply-templates select="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceConstraints/gmd:MD_LegalConstraints/gmd:accessConstraints" />
            <xsl:apply-templates select="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceConstraints/gmd:MD_LegalConstraints/gmd:useLimitation" />
                        
            
        </xsl:element>
    </xsl:template>

    <xsl:template match="gmd:fileIdentifier/gco:CharacterString">
        <xsl:element name="mmd:metadata_identifier">
            <xsl:value-of select="." />
        </xsl:element>
    </xsl:template>

    <xsl:template match="gmd:citation">
        <xsl:element name="mmd:title">
            <xsl:attribute name="xml:lang">en</xsl:attribute>
            <xsl:value-of select="gmd:CI_Citation/gmd:title/gco:CharacterString" />
        </xsl:element>
    </xsl:template>

    <xsl:template match="gmd:abstract">
        <xsl:element name="mmd:abstract">
            <xsl:attribute name="xml:lang">en</xsl:attribute>
            <xsl:value-of select="gco:CharacterString" />
        </xsl:element>
    </xsl:template>

    <xsl:template match="gmd:dateStamp">
        <xsl:element name="mmd:last_metadata_update">
        <xsl:choose>
            <xsl:when test="gco:DateTime">
                <xsl:value-of select="gco:DateTime" />
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="gco:Date" />
            </xsl:otherwise>
        </xsl:choose>
        </xsl:element>

        <!--
        <xsl:element name="mmd:last_metadata_update"><xsl:value-of select="gco:Date" /> </xsl:element>
    -->
    </xsl:template>

    <xsl:template match="gmd:language">
        <xsl:element name="mmd:dataset_language">
            <xsl:choose>
                <xsl:when test="gmd:LanguageCode">
                    <xsl:value-of select="gmd:LanguageCode" />    
                </xsl:when>
                <xsl:otherwise>
                    <xsl:value-of select="gco:CharacterString" />
                </xsl:otherwise>
            </xsl:choose>
            
        </xsl:element>    
    </xsl:template>

    <xsl:template match="gmd:status">        
        <xsl:variable name="iso_status" select="normalize-space(gmd:MD_ProgressCode)" />
        <xsl:variable name="iso_status_mapping" select="document('')/*/mapping:dataset_status[@iso=$iso_status]" />
        <xsl:value-of select="$iso_status_mapping" />
        <xsl:element name="mmd:dataset_production_status">
            <xsl:value-of select="$iso_status_mapping/@mmd"></xsl:value-of>                    
        </xsl:element>    
    </xsl:template>

    <!-- mapping between iso and mmd dataset statuses -->
    <mapping:dataset_status iso="completed" mmd="Complete" />
    <mapping:dataset_status iso="historicalArchive" mmd="Complete" />
    <mapping:dataset_status iso="obsolete" mmd="Obsolete" />
    <mapping:dataset_status iso="onGoing" mmd="In Work" />
    <mapping:dataset_status iso="planned" mmd="Planned" />
    <mapping:dataset_status iso="required" mmd="Planned" />
    <mapping:dataset_status iso="underDevelopment" mmd="Planned" />

    <xsl:template match="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:topicCategory/gmd:MD_TopicCategoryCode">
        <xsl:element name="mmd:iso_topic_category">
            <xsl:value-of select="." />
        </xsl:element>
    </xsl:template>    
    
    <xsl:template match="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent">    
        <xsl:element name="mmd:temporal_extent">        
            <xsl:element name="mmd:start_date">
                <xsl:value-of select="gml:TimePeriod/gml:beginPosition" />
            </xsl:element>
            <xsl:element name="mmd:end_date">
                <xsl:value-of select="gml:TimePeriod/gml:endPosition" />
            </xsl:element>
        </xsl:element>    
    </xsl:template>
    
    <xsl:template match="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox">    
        <xsl:element name="mmd:rectangle">
            <xsl:element name="mmd:north">
                <xsl:value-of select="gmd:northBoundLatitude/gco:Decimal" />
            </xsl:element>
            <xsl:element name="mmd:south">
                <xsl:value-of select="gmd:southBoundLatitude/gco:Decimal" />
            </xsl:element>
            <xsl:element name="mmd:west">
                <xsl:value-of select="gmd:westBoundLongitude/gco:Decimal" />
            </xsl:element>
            <xsl:element name="mmd:east">
                <xsl:value-of select="gmd:eastBoundLongitude/gco:Decimal" />
            </xsl:element>    
        </xsl:element>
    </xsl:template>
    
    <xsl:template match="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_BoundingPolygon/gmd:polygon">
        <xsl:element name="mmd:polygon">
            <xsl:copy-of select="gml:Polygon" />                
        </xsl:element>
    </xsl:template>
    
    <xsl:template match="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceConstraints/gmd:MD_LegalConstraints/gmd:accessConstraints">
        <xsl:element name="mmd:access_constraint">
            <xsl:choose>
                <xsl:when test="gmd:MD_RestrictionCode[@codeListValue='otherConstraints']">
                    <xsl:value-of select="../gmd:otherConstraints/gco:CharacterString" />
                </xsl:when>
                <xsl:otherwise>
                    <xsl:value-of select="gmd:MD_RestrictionCode/@codeListValue" />
                </xsl:otherwise>
            </xsl:choose>        
        </xsl:element>
    </xsl:template>
    
    <xsl:template match="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceConstraints/gmd:MD_LegalConstraints/gmd:useLimitation">
        <xsl:element name="mmd:use_constraint">
            <xsl:choose>
                <xsl:when test="gmd:MD_RestrictionCode[@codeListValue='otherConstraints']">
                    <xsl:value-of select="../gmd:otherConstraints/gco:CharacterString" />
                </xsl:when>
                <xsl:otherwise>
                    <xsl:value-of select="gco:CharacterString" />
                </xsl:otherwise>
            </xsl:choose>        
        </xsl:element>
    </xsl:template>    
    
    <xsl:template match="gmd:contact/gmd:CI_ResponsibleParty">
        <xsl:element name="mmd:personnel">
            <xsl:element name="mmd:role">
                <xsl:choose>
                    <xsl:when test="gmd:role/gmd:CI_RoleCode[@codeListValue='principalInvestigator']">
                        <xsl:text>Principal investigator</xsl:text>
                    </xsl:when>
                    <xsl:when test="gmd:role/gmd:CI_RoleCode[@codeListValue='pointOfContact']">
                        <xsl:text>Technical contact</xsl:text>
                    </xsl:when>
                    <xsl:when test="gmd:role/gmd:CI_RoleCode[@codeListValue='author']">
                        <xsl:text>Metadata author</xsl:text>
                    </xsl:when>                    
                    <xsl:otherwise>
                        <xsl:text>Technical contact</xsl:text>
                    </xsl:otherwise>
                </xsl:choose>            
            </xsl:element>
            
            <xsl:element name="mmd:name">
                <xsl:value-of select="gmd:individualName/gco:CharacterString" />
            </xsl:element>
            
            <xsl:element name="mmd:organisation">
                <xsl:value-of select="gmd:organisationName/gco:CharacterString" />
            </xsl:element>
            
            <xsl:element name="mmd:email">
                <xsl:value-of select="gmd:contactInfo/gmd:CI_Contact/gmd:address/gmd:CI_Address/gmd:electronicMailAddress/gco:CharacterString" />
            </xsl:element>

            <xsl:element name="mmd:phone">
                <xsl:value-of select="gmd:contactInfo/gmd:CI_Contact/gmd:phone/gmd:CI_Telephone/gmd:voice/gco:CharacterString" />
            </xsl:element>

            <xsl:element name="mmd:fax">
                <xsl:value-of select="gmd:contactInfo/gmd:CI_Contact/gmd:phone/gmd:CI_Telephone/gmd:facsimile/gco:CharacterString" />
            </xsl:element>

            <xsl:element name="mmd:contact_address">
                <xsl:element name="mmd:address">
                    <xsl:value-of select="gmd:contactInfo/gmd:CI_Contact/gmd:address/gmd:CI_Address/gmd:deliveryPoint/gco:CharacterString" />
                </xsl:element>
                <xsl:element name="mmd:city">
                    <xsl:value-of select="gmd:contactInfo/gmd:CI_Contact/gmd:address/gmd:CI_Address/gmd:city/gco:CharacterString" />
                </xsl:element>
                <xsl:element name="mmd:province_or_state">
                    <xsl:value-of select="gmd:contactInfo/gmd:CI_Contact/gmd:address/gmd:CI_Address/gmd:administrativeArea/gco:CharacterString" />
                </xsl:element>
                <xsl:element name="mmd:postal_code">
                    <xsl:value-of select="gmd:contactInfo/gmd:CI_Contact/gmd:address/gmd:CI_Address/gmd:postalCode/gco:CharacterString" />
                </xsl:element>
                <xsl:element name="mmd:country">
                    <xsl:value-of select="gmd:contactInfo/gmd:CI_Contact/gmd:address/gmd:CI_Address/gmd:country/gco:CharacterString" />
                </xsl:element>            
            </xsl:element>            
            
        </xsl:element>
    </xsl:template>
    
    <!-- get data access from NILU -->
    <xsl:template match="gmd:identificationInfo/srv:SV_ServiceIdentification/srv:containsOperations/srv:SV_OperationMetadata/srv:connectPoint">
        <xsl:element name="mmd:data_access">
            <xsl:element name="mmd:type">
                <xsl:variable name="external_name" select="normalize-space(gmd:CI_OnlineResource/gmd:protocol/gco:CharacterString)" />
                <xsl:variable name="protocol_mapping" select="document('')/*/mapping:protocol_names[@external=$external_name]" />
                <xsl:value-of select="$protocol_mapping" />
                <xsl:value-of select="$protocol_mapping/@mmd"></xsl:value-of> 
            </xsl:element>
            <xsl:element name="mmd:resource">
                <xsl:value-of select="gmd:CI_OnlineResource/gmd:linkage/gmd:URL" />
            </xsl:element>
        </xsl:element>
    </xsl:template>

    
    <!-- Extract information on host data center -->
    <xsl:template match="gmd:distributionInfo/gmd:MD_Distribution/gmd:distributor/gmd:MD_Distributor">
        <xsl:element name="mmd:data_center">
            <xsl:element name="mmd:data_center_name">
                <xsl:element name="mmd:short_name">
                    <!--xsl:value-of select=""/-->
                </xsl:element>
                <xsl:element name="mmd:long_name">
                    <xsl:value-of select="gmd:distributorContact/gmd:CI_ResponsibleParty/gmd:organisationName/gco:CharacterString"/>
                </xsl:element>
            </xsl:element>
            <xsl:element name="mmd:data_center_url">
                <xsl:value-of select="gmd:distributorContact/gmd:CI_ResponsibleParty/gmd:contactInfo/gmd:CI_Contact/gmd:onlineResource/gmd:CI_OnlineResource/gmd:linkage/gmd:URL"/>
            </xsl:element>
        </xsl:element>
    </xsl:template>

    <!-- Extract information on online resources -->
    <!-- mapping between protocol names -->
    <mapping:protocol_names external="OPeNDAP:OPeNDAP" mmd="OPeNDAP" />
    <mapping:protocol_names external="file" mmd="HTTP" />
    <mapping:protocol_names external="OGC:WFS" mmd="OGC WFS" />
    <mapping:protocol_names external="WWW:DOWNLOAD-1.0-http--download" mmd="HTTP" />
    <mapping:protocol_names external="csv" mmd="HTTP" />

    <xsl:template match="gmd:distributionInfo/gmd:MD_Distribution/gmd:transferOptions/gmd:MD_DigitalTransferOptions/gmd:onLine">
        <!-- need some hacks here to handle WGMS data, will also need to
             translate protocol identifications -->

        <xsl:element name="mmd:data_access">
            <xsl:element name="mmd:type">
                <xsl:variable name="external_name" select="normalize-space(gmd:CI_OnlineResource/gmd:protocol/gco:CharacterString)" />
                <xsl:variable name="protocol_mapping" select="document('')/*/mapping:protocol_names[@external=$external_name]" />
                <xsl:value-of select="$protocol_mapping" />
                <xsl:value-of select="$protocol_mapping/@mmd"></xsl:value-of> 
                <!--
                <xsl:value-of select="gmd:CI_OnlineResource/gmd:protocol/gco:CharacterString" />
                -->
            </xsl:element>
            <!--xsl:element name="mmd:name">
                <xsl:value-of select="gmd:CI_OnlineResource/gmd:name/gco:CharacterString" />
            </xsl:element-->
            <xsl:element name="mmd:resource">
                <xsl:value-of select="gmd:CI_OnlineResource/gmd:linkage/gmd:URL" />
            </xsl:element>                        
            <xsl:element name="mmd:description">
                <xsl:value-of select="gmd:CI_OnlineResource/gmd:description/gco:CharacterString" />
            </xsl:element>                                    
        </xsl:element>
    </xsl:template>

    <xsl:template match="gmd:dataSetURI/gco:CharacterString">
        <xsl:element name="mmd:related_information">
            <xsl:element name="mmd:type">Dataset landing page</xsl:element>
            <xsl:element name="mmd:description">NA</xsl:element>
            <xsl:element name="mmd:resource">
                <xsl:value-of select="."/>
            </xsl:element>
        </xsl:element>
    </xsl:template>

    <xsl:template match="gmd:distributionInfo/gmd:MD_Distribution/gmd:transferOptions/gmd:MD_DigitalTransferOptions/gmd:onLine/gmd:CI_OnlineResource">
        <!-- to extract landing pages from GeoNetwork -->
        <!--
        <xsl:if test="gmd:protocol/gco:CharacterString and gmd:linkage/gmd:URL">
        <xsl:if test="gmd:function/gmd:CI_OnLineFunctionCode">
            <xsl:element name="mmd:related_information">
                <xsl:text>Min test</xsl:text>
            </xsl:element>
        </xsl:if>
        -->
        <xsl:if test="gmd:function/gmd:CI_OnLineFunctionCode/@codeListValue='information' and gmd:description/gco:CharacterString='Extended human readable information about the dataset'">
            <xsl:element name="mmd:related_information">
                <xsl:element name="mmd:type">Dataset landing page</xsl:element>
                <xsl:element name="mmd:description">
                    <xsl:value-of select="gmd:description/gco:CharacterString"/>
                </xsl:element>
                <xsl:element name="mmd:resource">
                    <xsl:value-of select="gmd:linkage/gmd:URL"/>
                </xsl:element>
            </xsl:element>
        </xsl:if>
    </xsl:template>
    
    <xsl:template match="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:keyword/gco:CharacterString[contains(.,'EARTH SCIENCE &gt;')]">
        <xsl:element name="mmd:keyword">
            <xsl:value-of select="."/>
        </xsl:element>
    </xsl:template>
    <xsl:template match="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:keyword/gco:CharacterString[not(contains(.,'EARTH SCIENCE &gt;'))]">
        <xsl:element name="mmd:keyword">
            <xsl:value-of select="."/>
        </xsl:element>
    </xsl:template>
    
    <!--
    <xsl:template match="gmd:keyword">
        <xsl:element name="mmd:keyword">
            <xsl:value-of select="gco:CharacterString" />
        </xsl:element>
    </xsl:template>
    -->

    <xsl:template match="gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords[./gmd:type/gmd:MD_KeywordTypeCode = 'project']">
        <xsl:element name="mmd:project">
            <xsl:element name="mmd:short_name">
                <xsl:value-of select="gmd:keyword/gco:CharacterString" />
            </xsl:element>
            <xsl:element name="mmd:long_name">
                <xsl:value-of select="gmd:keyword/gco:CharacterString" />
            </xsl:element>
        </xsl:element>
    </xsl:template>
    
</xsl:stylesheet>
