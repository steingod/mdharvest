#!/usr/bin/python2.7
""" Script for harvesting metadata
    Inspired by:
        - harvest-metadata from https://github.com/steingod/mdharvest/tree/master/src
        - code from http://lightonphiri.org/blog/metadata-harvesting-via-oai-pmh-using-python

AUTHOR: Trygve Halsne, 25.01.2017

USAGE:
    - Currently initiaded with internal methods in class

COMMENTS (for further development):
    - Implement it object oriented by means of classes
    - Implement hprotocol: OGC-CSW, OpenSearch, ISO 19115
    - Does OGC-CSW metadata have some kind of resumptionToken analog?
    - When writing OAI-PMH records: Check if record contains header and has status=deleted..
"""

import urllib2 as ul2
import urllib as ul
from xml.dom.minidom import parseString
import codecs
import sys
import getopt
from datetime import datetime
import lxml.etree as ET

def usage():
    print sys.argv[0]+" [options] input"
    print "\t-h|--help: dump this information"
    print "\t-p|--protocol: specify the protocol to use"
    print "\t-u|--url: specify the URL to use for the service provider"
    print "\t-d|--destination: specify the local destination"
    sys.exit(2)

class MetadataHarvester(object):
    """ Creates metadata-harvester object with methods for harvesting and writing
    """
    def __init__(self, baseURL, records, outputDir, hProtocol, username=None, pw=None):
        """ set variables in class """
        self.baseURL = baseURL
        self.records = records
        self.outputDir = outputDir
        self.hProtocol = hProtocol
        self.username = username
        self.pw = pw

    def harvest(self):
        """ Inititates harvester. Chooses strategy depending on
            harvesting  protocol
        """
        baseURL, records, hProtocol, uname, pw = self.baseURL, self.records, self.hProtocol, self.username, self.pw

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
                print "Handling resumptionToken: %.0f \n" % pageCounter
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

            print "\n\nHarvesting took: %s [h:mm:ss]\n" % str(datetime.now()-start_time)

        elif hProtocol == "OpenSearch":
            getRecordsURL = str(baseURL + records)
            print "Harvesting metadata from: \n\tURL: %s \n\tprotocol: %s \n" % (getRecordsURL,hProtocol)
            start_time = datetime.now()

            dom = self.harvestContent(getRecordsURL,credentials=True,uname=uname,pw=pw)
            if dom != None:
                self.openSearch_writeENTRYtoFile(dom)

            # get all results by iteration
            tree = ET.fromstring(dom.toxml())
            nsmap = tree.nsmap
            default_ns = nsmap.pop(None)

            totalResults = int(tree.xpath('./opensearch:totalResults',namespaces=nsmap)[0].text)
            startIndex = int(tree.xpath('./opensearch:startIndex',namespaces=nsmap)[0].text)
            itemsPerPage = int(tree.xpath('./opensearch:itemsPerPage',namespaces=nsmap)[0].text)

            current_results = itemsPerPage

            # looping through the rest of the results updating start and rows values
            if totalResults > itemsPerPage:
                print "\nCould not display all results on single page. Starts iterating..."
            while current_results < totalResults:
                print "\n\n\tHandling results (%s - %s) / %s" %(current_results, current_results + itemsPerPage, totalResults)
                from_to = "?start=%s&rows=%s&" % (current_results,itemsPerPage)
                getRecordsURLLoop = str(baseURL + from_to + records[1:])
                dom = self.harvestContent(getRecordsURLLoop,credentials=True,uname=uname,pw=pw)
                if dom != None:
                    self.openSearch_writeENTRYtoFile(dom)
                current_results += itemsPerPage

            print "\n\nHarvesting took: %s [h:mm:ss]\n" % str(datetime.now()-start_time)

        else:
            print '\nProtocol %s is not accepted.' % hProtocol
            exit()


    def openSearch_writeENTRYtoFile(self,dom):
        """ Write OpenSearch ENTRY elements in fom to file"""
        print("Writing OpenSearch ENTRY metadata elements to disk... ")

        entries = dom.getElementsByTagName('entry')
        print "\tFound %.f ENTRY elements." % entries.length
        counter = 1
        has_fname = False
        for entry in entries:
            # Depending on usage; choose naming convention
            fname = entry.getElementsByTagName('title')[0].childNodes[0].nodeValue
            if fname != None:
                has_fname = True
            """
            # Use UUID as naming convention
            str_elements = entry.getElementsByTagName('str')
            for s in reversed(str_elements):
                if s.getAttribute('name') == 'uuid':
                    fname = s.childNodes[0].nodeValue
                    has_fname = True
                    break;
            """
            if has_fname:
                sys.stdout.write('\tWriting OpenSearch ENTRY elements %.f / %d \r' %(counter,entries.length))
                sys.stdout.flush()
                self.write_to_file(entry,fname)
                counter += 1
            else:
                sys.stdout.write('\tNo filename availible. Not able to write OpenSearch entry (%s) to file' % dom)

            # Temporary break when testing
            #if counter == 5:
            #    break;

    def ogccsw_writeCSWISOtoFile(self,dom):
        """ Write CSW-ISO elements in dom to file """
        print("Writing CSW ISO metadata elements to disk... ")

        mD_metadata_elements = dom.getElementsByTagName('gmd:MD_Metadata')
        mDsize = mD_metadata_elements.length
        size_idInfo = dom.getElementsByTagName('gmd:identificationInfo').length
        print "\tFound %.f ISO records." %mDsize

        if mDsize>0:
            counter = 1
            for md_element in mD_metadata_elements:
                # Check if element contains valid metadata
                idInfo = md_element.getElementsByTagName('gmd:identificationInfo')

                try:
                    # Use unique ID as filename
                    fileIdentifier = md_element.getElementsByTagName('gmd:fileIdentifier')[0]
                    cs = fileIdentifier.getElementsByTagName('gco:CharacterString')[0]
                    fname = cs.firstChild.nodeValue
                except:
                    print "\n\tMetadata element did not contain unique ID"
                    fname = "tmp_" + str(counter)
                    continue

                if idInfo !=[]:
                    sys.stdout.write('\tWriting CSW-ISO elements %.f / %d \r' %(counter,size_idInfo))
                    sys.stdout.flush()
                    self.write_to_file(md_element,fname)
                    counter += 1
                # Temporary break for testing
                if counter == 3:
                    break;


    def oaipmh_writeDIFtoFile(self,dom):
        """ Write DIF elements in dom to file """
        print "Writing DIF elements to disk... "

        record_elements = dom.getElementsByTagName('record')
        size_dif = dom.getElementsByTagName('DIF').length

        if size_dif != 0:
            counter = 1
            for record in record_elements:
                for child in record.childNodes:
                    # If record contains header, check if dataset is deleted. (Not implemented)
                    if str(child.nodeName) == 'header':
                        has_attrib = child.hasAttributes()

                if not has_attrib:
                    sys.stdout.write('\tWriting DIF elements %.f / %d \r' %(counter,size_dif))
                    sys.stdout.flush()
                    dif = record.getElementsByTagName('DIF')[0]
                    #use unique filename
                    fname = dif.getElementsByTagName('Entry_ID')[0].childNodes[0].nodeValue
                    self.write_to_file(dif,fname)
                    counter += 1
                # Temporary break for testing
                if counter == 10:
                    break;
        else:
            print "\trecords did not contain DIF elements"

    def write_to_file(self, root, fname):
        """ Function for storing harvested metadata to file
            - root: root Element to be stored. <DOM Element>
            - fname: unique id. <String>
            - output_path: output directory. <String>
        """
        outputDir = self.outputDir
        total_fname = outputDir + fname + '.xml'
        output = codecs.open(total_fname ,'w','utf-8')
        output.write(root.toxml())
        output.close()

    def harvestContent(self,URL,credentials=False,uname="foo",pw="bar"):
        """ Function for harvesting content from URL."""
        try:
            if not credentials:
                file = ul2.urlopen(URL,timeout=40) # Timeout depends on user
                data = file.read()
                file.close()
                return parseString(data)
            else:
                p = ul2.HTTPPasswordMgrWithDefaultRealm()
                p.add_password(None, URL, uname, pw)
                handler = ul2.HTTPBasicAuthHandler(p)
                opener = ul2.build_opener(handler)
                ul2.install_opener(opener)
                return parseString(ul2.urlopen(URL).read())
        except ul2.HTTPError:
            print("There was an error with the URL request. " +
                  "Could not open or parse content from: \n\t %s" % URL)

    def oaipmh_resumptionToken(self,URL):
        """ Function for handeling resumptionToken in OAI-PMH"""
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


def main(argv):
    ''' Main method with examples for isolated running of script'''

    # Parse command line arguments
    try:
        opts, args = getopt.getopt(argv,"hp:u:d:",
                ["help","protocol","url","destination"])
    except getopt.GetoptError:
        usage()

    pflg = uflg = dflg = False
    for opt, arg in opts:
        if opt == ("-h","--help"):
            usage()
        elif opt in ("-p","--protocol"):
            srcprotocol = arg
            pflg =True
        elif opt in ("-u","--url"):
            srcurl = arg
            uflg =True
        elif opt in ("-d","--destination"):
            destination = arg
            dflg =True

    if not pflg:
        usage()
    elif not uflg:
        usage()
    elif not dflg:
        usage()

    if srcprotocol == "OAI-PMH":
        request = "?verb=ListRecords&metadataPrefix=dif"
    elif srcprotocol == "OGC-CSW":
        request = "?SERVICE=CSW&VERSION=2.0.2&request=GetRecords&constraintLanguage=CQL_TEXT&typeNames=csw:Record&resultType=results&outputSchema=http://www.isotc211.org/2005/gmd"
    else:
        print "Unrecognised request"

    print "Request: "+srcprotocol

    mh = MetadataHarvester(srcurl,request,destination,srcprotocol)
    mh.harvest()

    sys.exit()

    #baseURL = 'http://oai.nerc-bas.ac.uk:8080/oai/provider'
    #records='?verb=ListRecords&metadataPrefix=gcmd'
    #baseURL = 'http://union.ndltd.org/OAI-PMH/'
    #records = '?verb=ListRecords&metadataPrefix=oai_dc'
    #baseURL = 'https://esg.prototype.ucar.edu/oai/repository.htm'
    #records = '?verb=ListRecords&metadataPrefix=dif'
    #outputDir = 'output/'
    #hProtocol = 'OAI-PMH'

    #mh = MetadataHarvester(baseURL,records, outputDir, hProtocol)
    #mh.harvest()

    '''
    baseURL = 'http://metadata.bgs.ac.uk/geonetwork/srv/en/csw'
    records = '?SERVICE=CSW&VERSION=2.0.2&request=GetRecords&constraintLanguage=CQL_TEXT&typeNames=csw:Record&resultType=results&outputSchema=http://www.isotc211.org/2005/gmd'
    outputDir = 'output/'
    hProtocol = 'OGC-CSW'

    mh2 = MetadataHarvester(baseURL,records, outputDir, hProtocol)
    mh2.harvest()
    '''

    with open('myValues.txt','r') as code:
        cred_tmp = code.readline()

    cred = cred_tmp.split(';')
    baseURL = 'https://colhub.met.no/search'
    records = '?q=*'
    #records = '?q=platformname:Sentinel-1%20AND%20ingestionDate:[NOW-2DAY%20TO%20NOW]'
    #records = '?q=S2A_MSIL1C_20170116T105401_N0204_R051_T32VNM_20170116T105355'
    #records = '?q=S1A_IW_GRDM_1SDV_20161206T060347_20161206T060447_014255_0170E4_A49B'
    #records = '?q=S2A_MSIL1C*%20AND%20footprint:%22Intersects(POLYGON((-10.25%2071.41,%20-10.20%2070.61,%20-6.79%2070.60,%20-6.70%2071.44,%20-10.25%2071.41)))%22%20AND%20ingestiondate:[NOW-3HOUR%20TO%20NOW]'
    #records = '?q=S2A_OPER_PRD_MSIL1C_PDMC_20160211T195349_R051_V20160211T105155_20160211T105155'
    #records = '?q=S2B_MSIL1C_20170820T115639_N0205_R066_T35XMG_20170820T115639'
    #records = '?q=S2B_MSIL1C*%20AND%20(%20footprint:"Intersects(POLYGON((3.04675852309276%2071.68036004870032,41.54285227309276%2071.68036004870032,41.54285227309276%2081.47413661551582,3.04675852309276%2081.47413661551582,3.04675852309276%2071.68036004870032)))"%20)%20AND%20ingestiondate:[NOW-60HOUR%20TO%20NOW-5HOUR]'

    outputDir = 'output/'
    hProtocol = 'OpenSearch'
    mh3 = MetadataHarvester(baseURL,records, outputDir, hProtocol,cred[0],cred[1])
    mh3.harvest()
if __name__ == '__main__':
    main(sys.argv[1:])

# Some TEMPORARY VALUES
# List all recordsets: http://arcticdata.met.no/metamod/oai?verb=ListRecords&set=nmdc&metadataPrefix=dif
# List identifier: http://arcticdata.met.no/metamod/oai?verb=GetRecord&identifier=urn:x-wmo:md:no.met.arcticdata.test3::ADC_svim-oha-monthly&metadataPrefix=dif
# Recordset with resumptionToken: http://union.ndltd.org/OAI-PMH/?verb=ListRecords&metadataPrefix=oai_dc
# Recordset with DIF elements and resumptionToken (Slow server..): http://ws.pangaea.de/oai/provider?verb=ListRecords&metadataPrefix=dif
# Recordset with DIF elements and resumptionToken: https://esg.prototype.ucar.edu/oai/repository.htm?verb=ListRecords&metadataPrefix=dif
# Recordset with gcmd(DIF) elements: http://oai.nerc-bas.ac.uk:8080/oai/provider?verb=ListRecords&metadataPrefix=gcmd
# OGC-CSW recordset: http://metadata.bgs.ac.uk/geonetwork/srv/en/csw?SERVICE=CSW&VERSION=2.0.2&request=GetRecords&constraintLanguage=CQL_TEXT&typeNames=csw:Record&resultType=results&outputSchema=http://www.isotc211.org/2005/gmd
# OpenSearch from sentinel scihub: https://scihub.copernicus.eu/dhus/search?q=S2A*
