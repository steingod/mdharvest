""" Script for harvesting metadata
    Inspired by:
        - harvest-metadata from https://github.com/steingod/mdharvest/tree/master/src
        - code from http://lightonphiri.org/blog/metadata-harvesting-via-oai-pmh-using-python

AUTOR: Trygve Halsne, 25.01.2017

USAGE:
    - input must have metadataPrefix?

COMMENTS:
    - Implement it object oriented by means of classes
    - Implement hprotocol: OGC-CSW, OpenSearch, ISO 19115
    - Does OGC-CSW metadata have some kind of resumptionToken analog?
"""

# List all recordsets: http://arcticdata.met.no/metamod/oai?verb=ListRecords&set=nmdc&metadataPrefix=dif
# List identifier: http://arcticdata.met.no/metamod/oai?verb=GetRecord&identifier=urn:x-wmo:md:no.met.arcticdata.test3::ADC_svim-oha-monthly&metadataPrefix=dif
# Recordset with resumptionToken: http://union.ndltd.org/OAI-PMH/?verb=ListRecords&metadataPrefix=oai_dc
# Recordset with DIF elements and resumptionToken (Slow server..): http://ws.pangaea.de/oai/provider?verb=ListRecords&metadataPrefix=dif
# Recordset with DIF elements and resumptionToken: https://esg.prototype.ucar.edu/oai/repository.htm?verb=ListRecords&metadataPrefix=dif
# Recordset with gcmd(DIF) elements: http://oai.nerc-bas.ac.uk:8080/oai/provider?verb=ListRecords&metadataPrefix=gcmd

# OGC-CSW recordset: http://metadata.bgs.ac.uk/geonetwork/srv/en/csw?SERVICE=CSW&VERSION=2.0.2&request=GetRecords&constraintLanguage=CQL_TEXT&typeNames=csw:Record&resultType=results&outputSchema=http://www.isotc211.org/2005/gmd

import urllib2 as ul2
import urllib as ul
from xml.dom.minidom import parseString
import codecs
import sys
from datetime import datetime


class MetadataHarvester(object):
    def __init__(self, baseURL, records, outputDir, hProtocol): # add outputname also?
        """ set variables in class """
        self.baseURL = baseURL
        self.records = records
        self.outputDir = outputDir
        self.hProtocol = hProtocol

    def harvest(self):
        """ Inititates harvester. Chooses strategy depending on
            harvesting  protocol
        """
        baseURL, records, hProtocol = self.baseURL, self.records, self.hProtocol

        if hProtocol == 'OAI-PMH':
            # Could/should be more sophistiated by means of deciding url properties
            getRecordsURL = str(baseURL + records)
            print "Harvesting metadata from: \n\tURL: %s \n\tprotocol: %s \n" % (getRecordsURL,hProtocol)
            start_time = datetime.now()

            # Initial phase
            resumptionToken = self.oaipmh_resumptionToken(getRecordsURL)
            dom = self.harvestContent(getRecordsURL)
            if dom != None:
                self.oaipmh_writeDIFtoFile(dom)
            pageCounter = 1

            while resumptionToken != []:
                print "\n"
                print "Handeling resumptionToken: %.0f \n" % pageCounter
                resumptionToken = ul.urlencode({'resumptionToken':resumptionToken}) # create resumptionToken URL parameter
                getRecordsURLLoop = str(baseURL+'?verb=ListRecords&'+resumptionToken)
                dom = self.harvestContent(getRecordsURLLoop)
                if dom != None:
                    self.oaipmh_writeDIFtoFile(dom)
                else:
                    print "dom = " + str(dom) + ', for page ' + str(pageCounter)

                resumptionToken = self.oaipmh_resumptionToken(getRecordsURLLoop)
                pageCounter += 1

            print "\n\nHarvesting took: %s [h:mm:ss]" % str(datetime.now()-start_time)

        elif hProtocol == 'OGC-CSW':
            getRecordsURL = str(baseURL + records)
            print "Harvesting metadata from: \n\tURL: %s \n\tprotocol: %s \n" % (getRecordsURL,hProtocol)
            start_time = datetime.now()
            dom = self.harvestContent(getRecordsURL)
            if dom != None:
                self.ogccsw_writeCSWISOtoFile(dom)

            print "\n\nHarvesting took: %s [h:mm:ss]" % str(datetime.now()-start_time)
        else:
            print 'Protocol %s is not accepted.' % hProtocol
            exit()

    def ogccsw_writeCSWISOtoFile(self,dom):
        """ Write CSW-ISO elements in dom to file """
        print("Writing CSW ISO metadata elements to disk... ")

        mD_metadata_elements = dom.getElementsByTagName('gmd:MD_Metadata')
        mDsize = mD_metadata_elements.length
        size_idInfo = dom.getElementsByTagName('gmd:identificationInfo').length
        print "\tFound %.f ISO records." %mDsize

        counter = 1
        if mDsize>0:
            for md_element in mD_metadata_elements:
                # Check if element contains valid metadata
                idInfo = md_element.getElementsByTagName('gmd:identificationInfo')
                if idInfo !=[]:
                    sys.stdout.write('\tWriting CSW-ISO elements %.f / %d \r' %(counter,size_idInfo))
                    sys.stdout.flush()
                    counter += 1


    def oaipmh_writeDIFtoFile(self,dom):
        """ Write DIF elements in dom to file """
        print "Writing DIF elements to disk... "

        record_elements = dom.getElementsByTagName('record')
        size_dif = dom.getElementsByTagName('DIF').length

        if size_dif != 0:
            counter = 1
            for record in record_elements:
                for child in record.childNodes:
                    if str(child.nodeName) == 'header':
                        has_attrib = child.hasAttributes()
                        for gchild in child.childNodes:
                            if gchild.nodeName == 'identifier':
                                id_text = gchild.childNodes[0].nodeValue
                                break;

                if not has_attrib:
                    sys.stdout.write('\tWriting DIF elements %.f / %d \r' %(counter,size_dif))
                    sys.stdout.flush()
                    dif = record.getElementsByTagName('DIF')[0]
                    #tmp_fname ='dif_test_' + str(id_text) + '.xml'
                    tmp_fname ='dif_test_' + str(counter) + '.xml'
                    output = codecs.open(tmp_fname ,'w','utf-8')
                    dif.writexml(output)
                    output.close()
                    counter += 1
                # Temporary break
                if counter == 3:
                    break;
        else:
            print "\trecords did not contain DIF elements"

    def harvestContent(self,URL):
        try:
            file = ul2.urlopen(URL,timeout=40)
            data = file.read()
            file.close()
            return parseString(data)
        except ul2.HTTPError:
            print("There was an error with the URL request. " +
                  "Could not open or parse content from: \t\n %s" % URL)

    def oaipmh_resumptionToken(self,URL):
        try:
            file = ul2.urlopen(URL, timeout=40)
            data = file.read()
            file.close()
            dom = parseString(data)

            if dom.getElementsByTagName('resumptionToken').length == 0:
                return dom.getElementsByTagName('resumptionToken')
            else:
                if dom.getElementsByTagName('resumptionToken')[0].firstChild != None:
                    return dom.getElementsByTagName('resumptionToken')[0].firstChild.nodeValue
                else:
                    return []
        except ul2.HTTPError:
            print "There was an error with the URL request"


#baseURL = 'https://esg.prototype.ucar.edu/oai/repository.htm'
#records = '?verb=ListRecords&metadataPrefix=dif'

def main():
    baseURL = 'http://oai.nerc-bas.ac.uk:8080/oai/provider'
    records='?verb=ListRecords&metadataPrefix=gcmd'
    outputDir = 'tmp'
    hProtocol = 'OAI-PMH'

    mh = MetadataHarvester(baseURL,records, outputDir, hProtocol)
    mh.harvest()

    baseURL = 'http://metadata.bgs.ac.uk/geonetwork/srv/en/csw'
    records = '?SERVICE=CSW&VERSION=2.0.2&request=GetRecords&constraintLanguage=CQL_TEXT&typeNames=csw:Record&resultType=results&outputSchema=http://www.isotc211.org/2005/gmd'
    outputDir = 'tmp'
    hProtocol = 'OGC-CSW'

    mh2 = MetadataHarvester(baseURL,records, outputDir, hProtocol)
    mh2.harvest()

if __name__ == '__main__':
    main()

#baseURL =  'http://dalspace.library.dal.ca:8080/oai/request'
#arguments = '?verb=ListRecords&metadataPrefix=oai_dc'

#baseURL = 'http://union.ndltd.org/OAI-PMH/'
#getRecordsURL = str(baseURL+'?verb=ListRecords&metadataPrefix=oai_dc')
