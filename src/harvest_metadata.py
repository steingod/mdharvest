#!/usr/bin/python
# -*- coding: UTF-8 -*-
""" Script for harvesting metadata
    Inspired by:
        - harvest-metadata from https://github.com/steingod/mdharvest/tree/master/src
        - code from http://lightonphiri.org/blog/metadata-harvesting-via-oai-pmh-using-python

AUTHOR:     Trygve Halsne, 25.01.2017
UPDATED:    Øystein Godøy, METNO/FOU, 2017-12-12 
            Multiple..
            Øystein Godøy, METNO/FOU, 2018-03-27 
                First version suitable for regular use for OAI-PMH.
            Øystein Godøy, METNO/FOU, 2018-05-09 
                Working version for OAI-PMH with lxml
            Øystein Godøy, METNO/FOU, 2018-05-10 
                Working version with OGC CSW as well
            Øystein Godøy, METNO/FOU, 2019-06-03 
                Better handling of character encoding.

USAGE:
    - See usage
    - Currently initiated with internal methods in class

COMMENTS (for further development):
    - Rewrite to lxml started for OpenSearch
    - Rename dom elements when completed, and remove DOM requirement...
    - Rename file to avoid using dash...
    - self-numRecHarv is incorrect when harvest fails
"""

import urllib2 as ul2
import urllib as ul
from xml.dom.minidom import parseString # To be removed
import codecs
import sys
import os
import getopt
from datetime import datetime
import lxml.etree as ET

def usage():
    print sys.argv[0]+" [options] input"
    print "\t-h|--help: dump this information"
    print "\t-p|--protocol: specify the protocol to use"
    print "\t-s|--set: specify set to harvest (only used for OAI-PMH)"
    print "\t-f|--format: specify the file format to use (only used for OAI-PMH)"
    print "\t-u|--url: specify the URL to use for the service provider"
    print "\t-d|--destination: specify the local destination"
    sys.exit(2)

class MetadataHarvester(object):
    """ 
    Creates metadata-harvester object with methods for harvesting and writing
    """
    def __init__(self, baseURL, records, outputDir, hProtocol, 
            srcfmt = None, username=None, pw=None):
        """ set variables in class """
        self.baseURL = baseURL
        self.records = records
        self.outputDir = outputDir
        self.hProtocol = hProtocol
        self.srcfmt = srcfmt
        self.username = username
        self.pw = pw
        self.numRecHarv = 0

    def harvest(self):
        """ 
        Inititates harvester. Chooses strategy depending on
        harvesting  protocol
        """
        baseURL, records, hProtocol, uname, pw = self.baseURL, self.records, self.hProtocol, self.username, self.pw

        self.numRecHarv = 0
        if hProtocol == 'OAI-PMH':
            # Could/should be more sophistiated by means of deciding url
            # properties
            getRecordsURL = str(baseURL + records)
            print "Harvesting metadata from: \n\tURL: %s \n\tprotocol: %s \n" % (getRecordsURL,hProtocol)
            start_time = datetime.now()

            # Initial phase
            dom = self.harvestContent(getRecordsURL)
            if dom != None:
                if "dif" in self.srcfmt:
                    self.oaipmh_writeDIFtoFile(dom)
                elif "iso" in self.srcfmt:
                    self.oaipmh_writeISOtoFile(dom)
                else:
                    raise "Metadata format not supported yet."
            else:
                print "Server is not responding properly"
                raise IOError("Server to harvest is not responding properly")
                return(0)
            pageCounter = 1
            # Check for resumptionToken
            resumptionToken = dom.find(
                    'oai:ListRecords/oai:resumptionToken',
                    namespaces={'oai':'http://www.openarchives.org/OAI/2.0/'})
            if resumptionToken != None:
                resumptionToken = resumptionToken.text

            # Manage resumptionToken, i.e. segmentation of results in
            # pages
            while resumptionToken != None:
                print "\n\tHandling resumptionToken: %.0f" % pageCounter
                resumptionToken = ul.urlencode({'resumptionToken':resumptionToken}) # create resumptionToken URL parameter
                getRecordsURLLoop = str(baseURL+'?verb=ListRecords&'+resumptionToken)
                dom = self.harvestContent(getRecordsURLLoop)
                if dom != None:
                    if "dif" in self.srcfmt:
                        self.oaipmh_writeDIFtoFile(dom)
                    elif "iso" in self.srcfmt:
                        self.oaipmh_writeISOtoFile(dom)
                    else:
                        raise "Metadata format not supported yet."
                else:
                    print "dom = " + str(dom) + ', for page ' + str(pageCounter)

                resumptionToken = dom.find(
                        'oai:ListRecords/oai:resumptionToken',
                        namespaces={'oai':
                            'http://www.openarchives.org/OAI/2.0/'})
                if resumptionToken != None:
                    resumptionToken = resumptionToken.text
                pageCounter += 1

            print "Harvesting completed"
            print "Harvesting took: %s [h:mm:ss]" % str(datetime.now()-start_time)
            print "Number of records successfully harvested", self.numRecHarv

        elif hProtocol == 'OGC-CSW':
            getRecordsURL = str(baseURL + records)
            print "Harvesting metadata from: \n\tURL: %s \n\tprotocol: %s \n" % (getRecordsURL,hProtocol)
            start_time = datetime.now()
            dom = self.harvestContent(getRecordsURL)
            if dom == None:
                print "Server is not responding properly"
                raise IOError("Server to harvest is not responding properly")
                return(0)
            cswHeader = dom.find('csw:SearchResults',
                    namespaces={'csw':'http://www.opengis.net/cat/csw/2.0.2'})
            if cswHeader == None:
                print "Could not parse header response"
                sys.exit(2)
            numRecs = int(cswHeader.get("numberOfRecordsMatched"))
            nextRec =  int(cswHeader.get('nextRecord'))
            self.numRecsReturned = int(cswHeader.get('numberOfRecordsReturned'))
            if dom != None:
                self.ogccsw_writeCSWISOtoFile(dom)
            while nextRec < numRecs:
                getRecordsURLNew = getRecordsURL
                getRecordsURLNew += '&startposition='
                getRecordsURLNew += str(nextRec)
                dom = self.harvestContent(getRecordsURLNew)
                cswHeader = dom.find('csw:SearchResults',
                        namespaces={'csw':'http://www.opengis.net/cat/csw/2.0.2'})
                nextRec =  int(cswHeader.get('nextRecord'))
                self.numRecsReturned = int(cswHeader.get('numberOfRecordsReturned'))
                self.ogccsw_writeCSWISOtoFile(dom)
                if nextRec == 0:
                    break

            print "Harvesting completed"
            print "Harvesting took: %s [h:mm:ss]" % str(datetime.now()-start_time)
            print "Number of records successfully harvested", self.numRecHarv

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
            raise IOError("Protocol is not accepted")

        return(self.numRecHarv)

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
        myns = {
                'csw':'http://www.opengis.net/cat/csw/2.0.2',
                'gmd':'http://www.isotc211.org/2005/gmd',
                'gco':'http://www.isotc211.org/2005/gco',
                'gml':'http://www.opengis.net/gml/3.2'
                }

        record_elements = dom.xpath('/csw:GetRecordsResponse/csw:SearchResults/gmd:MD_Metadata', 
                namespaces=myns)
        # This won't work as NumberOfRecordsReturned is not updated for
        # the last request...
##       if len(record_elements) != self.numRecsReturned:
##           print "Mismatch in number of records, bailing out"
##           sys.exit(2)
        print "\tNumber of records found",len(record_elements)

        numRecs = len(record_elements)

        counter = 0
        for record in record_elements:
            cswid = record.find('gmd:fileIdentifier/gco:CharacterString',
                    namespaces=myns)
            if cswid == None:
                print "Skipping record, no FileID"
                continue
            # Dump to file...
            self.write_to_file(record, cswid.text)
            counter += 1
        print "\tNumber of records written", counter
        self.numRecHarv += counter
        return

    def oaipmh_writeISOtoFile(self, dom):
        """ 
        Write ISO elements in dom to file 
        """
        myns = {
                'oai':'http://www.openarchives.org/OAI/2.0/',
                'gmd':'http://www.isotc211.org/2005/gmd',
                'gmi':'http://www.isotc211.org/2005/gmi',
                'gco':'http://www.isotc211.org/2005/gco',
                'gml':'http://www.opengis.net/gml/3.2'
                }
        record_elements =  dom.xpath('/oai:OAI-PMH/oai:ListRecords/oai:record', 
                namespaces=myns)
        print "\tNumber of records found",len(record_elements)+1
        size_dif = len(record_elements)

        counter = 0
        if size_dif != 0:
            counter = 1
            for record in record_elements:
                # Check header if deleted
                datestamp = record.find('oai:header/oai:datestamp',
                        namespaces={'oai':'http://www.openarchives.org/OAI/2.0/'}).text
                oaiid = record.find('oai:header/oai:identifier',
                        namespaces={'oai':'http://www.openarchives.org/OAI/2.0/'}).text
                delete_status = record.find("oai:header[@status='deleted']",
                        namespaces={'oai':'http://www.openarchives.org/OAI/2.0/'})
                # Need to add handling of deleted records,
                # i.e. modify old records...
                # Challenges arise when oaiid and isoid are different as
                # isoid is used as the filename...
                if delete_status != None:
                    print "This record has been deleted"+"\n\t",oaiid
                    #print ">>>status", counter, delete_status, len(delete_status)
                isoid = record.find('oai:metadata/gmi:MI_Metadata/gmd:fileIdentifier/gco:CharacterString',
                        namespaces=myns)
                if isoid == None:
                    isoid = record.find('oai:metadata/gmd:MD_Metadata/gmd:fileIdentifier/gco:CharacterString',
                            namespaces=myns)
                    print('Here I am',isoid)
                if isoid == None:
                    print "Skipping record, no ISO ID"
                    continue
                isoid = isoid.text
                isorec = record.find('oai:metadata/gmd:MD_Metadata',
                        namespaces=myns)
                if isorec == None:
                    isorec = record.find('oai:metadata/gmi:MI_Metadata',
                            namespaces=myns)
                if isorec == None:
                    continue

                # Dump to file
                self.write_to_file(isorec, isoid)
            print "\tNumber of records written to files", counter
        else:
            print "\trecords did not contain ISO elements"

        self.numRecHarv += counter

        return


    def oaipmh_writeDIFtoFile(self,dom):
        """ 
        Write DIF elements in dom to file 
        """

        myns = {
                'oai':'http://www.openarchives.org/OAI/2.0/',
                'dif':'http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/'
                }
        record_elements =  dom.xpath('/oai:OAI-PMH/oai:ListRecords/oai:record', 
                namespaces=myns)
        print "\tNumber of records found",len(record_elements)+1
        size_dif = len(record_elements)

        counter = 0
        if size_dif != 0:
            for record in record_elements:
                # Check header if deleted
                #print ET.tostring(record)
                datestamp = record.find('oai:header/oai:datestamp',
                        namespaces={'oai':'http://www.openarchives.org/OAI/2.0/'})
                if datestamp != None:
                    datestamp = datestamp.text
                oaiid = record.find('oai:header/oai:identifier',
                        namespaces={'oai':'http://www.openarchives.org/OAI/2.0/'}).text
                delete_status = record.find("oai:header[@status='deleted']",
                        namespaces={'oai':'http://www.openarchives.org/OAI/2.0/'})
                # Need to add handling of deleted records,
                # i.e. modify old records...
                # Cahallenges arise when oaiid and difid are different as
                # difid is used as the filename...
                if delete_status != None:
                    print "This record has been deleted"+"\n\t",oaiid
                    #print ">>>status", counter, delete_status, len(delete_status)
                difid = record.find('oai:metadata/dif:DIF/dif:Entry_ID',
                        namespaces=myns)
                if difid == None:
                    print "Skipping record, no DIF ID"
                    continue
                difid = difid.text
##               if oaiid != difid:
##                   if difid not in oaiid:
##                       print "\tErrors in identifiers, skipping record!!!"
##                       print "\toaiid", oaiid
##                       print "\tdifid", difid
##                       continue
##                   else:
##                       print "Mismatch between OAI and DIF identifiers, using DIF identifiers"
                difrec = record.find('oai:metadata/dif:DIF',
                        namespaces=myns)

                # Dump to file
                self.write_to_file(difrec, difid)
                counter += 1
        else:
            print "\tRecords did not contain DIF elements"

        print "\tNumber of records written to files", counter
        self.numRecHarv += counter
        return

    def write_to_file(self, record, myid):
        """ Function for storing harvested metadata to file
            - root: root Element to be stored. <DOM Element>
            - fname: unique id. <String>
            - output_path: output directory. <String>
        """
        if not os.path.isdir(self.outputDir):
           try:
               os.makedirs(self.outputDir)
           except:
               print "Could not create output directory"
               sys.exit(2)

        myid = myid.replace('/','-')
        myid = myid.replace(':','-')
        myid = myid.replace('.','-')
        filename = self.outputDir+'/'+myid+'.xml'
        outputstr = ET.ElementTree(record)
        try:
            outputstr.write(filename, pretty_print=True,
                    xml_declaration=True, standalone=None, 
                    encoding="UTF-8")
        except:
            print "Could not create output file", filename
            raise
            sys.exit(2)
        return

    def harvestContent(self,URL,credentials=False,uname="foo",pw="bar"):
        """ Function for harvesting content from URL."""
        try:
            if not credentials:
                myfile = ul2.urlopen(URL,timeout=60) # Timeout depends on user
                #data = myfile.read()
                myencoding = myfile.headers.getparam('charset')
                myparser = ET.XMLParser(ns_clean=True, encoding=myencoding)
                #data =  myfile.read().decode(myfile.headers.getparam('charset'))
                #f = open('myharvest.xml','w+')
                #f.write(data.encode('utf-8'))
                #f.close()
                #sys.exit()
                try:
                    data = ET.parse(myfile, myparser)
                except Exception as e:
                    print('Parsing the harvested information failed due to', e)
                myfile.close()
                return data
            else:
                # Not working with lxml
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
        """ Function for handling resumptionToken in OAI-PMH"""
        #print "Now in resumptionToken..."
        try:
            file = ul2.urlopen(URL, timeout=60)
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
        opts, args = getopt.getopt(argv,"hp:f:s:u:d:",
                ["help","protocol","format","set","url","destination"])
    except getopt.GetoptError:
        usage()

    pflg = uflg = dflg = fflg = sflg = False
    for opt, arg in opts:
        if opt == ("-h","--help"):
            usage()
        elif opt in ("-p","--protocol"):
            srcprotocol = arg
            pflg =True
        elif opt in ("-s","--set"):
            oaiset = arg
            sflg =True
        elif opt in ("-f","--format"):
            srcformat = arg
            fflg =True
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
        if not fflg:
            usage()
        if sflg:
            request = "?verb=ListRecords&metadataPrefix="+srcformat+"&set="+oaiset
        else:
            request = "?verb=ListRecords&metadataPrefix="+srcformat
    elif srcprotocol == "OGC-CSW":
        if not "?SERVICE" in srcurl:
            request = "?SERVICE=CSW&VERSION=2.0.2&request=GetRecords&constraintLanguage=CQL_TEXT&typeNames=csw:Record&resultType=results&outputSchema=http://www.isotc211.org/2005/gmd&elementSetName=full"
        else:
            request = ""
    elif srcprotocol == "OpenSearch":
        request = "?q=*"
    else:
        print "Unrecognised request"

    print "Request: "+srcprotocol

    mh = MetadataHarvester(srcurl,request,destination,srcprotocol,srcformat)
    mh.harvest()

    sys.exit()


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

