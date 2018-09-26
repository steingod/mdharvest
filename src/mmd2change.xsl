<?xml version="1.0" encoding="UTF-8"?>

<!--
XSLT to add new elements, like collection keywords etc to existing documents. Should be aligned with the MMD XSD at any time.

Øystein Godøy, METNO/FOU, 2018-03-04 
-->

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:mmd="http://www.met.no/schema/mmd"
    version="1.0">

    <xsl:output method="xml" encoding="UTF-8" indent="yes" version="1.0" omit-xml-declaration="no" />
    <!--xsl:output method="xml" encoding="UTF-8" indent="yes" version="1.0" /-->
    <xsl:strip-space elements="*"/>

    <xsl:template match="/mmd:mmd">

        <xsl:element name="mmd:mmd" version="1.0" xmlns:mmd="http://www.met.no/schema/mmd">

            <xsl:copy-of select="mmd:metadata_identifier" copy-namespaces="no"/>
            <xsl:copy-of select="mmd:title" />
            <xsl:copy-of select="mmd:abstract" />
            <xsl:copy-of select="mmd:metadata_status" />
            <xsl:copy-of select="mmd:dataset_production_status"  copy-namespaces="no"/>
            <xsl:copy-of select="mmd:collection" copy-namespaces="no"/>
            <!--
            <xsl:element name="mmd:collection">YOPP</xsl:element>
            -->
            <xsl:copy-of select="mmd:last_metadata_update" />
            <xsl:copy-of select="mmd:temporal_extent" />
            <xsl:copy-of select="mmd:iso_topic_category" />
            <xsl:copy-of select="mmd:keywords" />
            <xsl:copy-of select="mmd:operational_status" />
            <xsl:copy-of select="mmd:dataset_language" />
            <xsl:copy-of select="mmd:geographic_extent" copy-namespaces="no"/>
            <xsl:copy-of select="mmd:access_constraint" />
            <xsl:copy-of select="mmd:use_constraint" />
            <xsl:copy-of select="mmd:project" />
            <xsl:copy-of select="mmd:activity_type" />
            <xsl:copy-of select="mmd:instrument" />
            <xsl:copy-of select="mmd:platform" />
            <xsl:copy-of select="mmd:related_information" />
            <!--
            <xsl:element name="mmd:related_information">
                <xsl:element name="type">Dataset landing page</xsl:element>
                <xsl:element name="description">THREDDS data server landing page</xsl:element>
                <xsl:element name="resource"><xsl:value-of select="mmd:data_access[mmd:type = 'HTTP']/mmd:resource"/></xsl:element>
            </xsl:element>
            -->
            <xsl:copy-of select="mmd:personnel" />
            <xsl:copy-of select="mmd:dataset_citation" />
            <xsl:copy-of select="mmd:data_access" />
            <xsl:copy-of select="mmd:reference" />
            <xsl:copy-of select="mmd:data_center" />
            <!--
            <xsl:element name="mmd:data_access">
                <xsl:element name="mmd:type">OGC WMS</xsl:element>
                <xsl:element name="mmd:description"></xsl:element>
                <xsl:element name="mmd:resource"><xsl:value-of select="mmd:data_access[mmd:type = 'OPeNDAP']/mmd:resource"/></xsl:element>
            </xsl:element>
            -->
            <xsl:copy-of select="mmd:related_dataset"/>
            <xsl:copy-of select="mmd:cloud_cover"/>
            <xsl:copy-of select="mmd:scene_cover"/>

        </xsl:element>
    </xsl:template>

</xsl:stylesheet>
