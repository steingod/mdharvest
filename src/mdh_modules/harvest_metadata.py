# -*- coding: UTF-8 -*-
""" 
Used by run_harvest.pyUsed by run_harvest.py

COMMENTS (for further development):
    - Rewrite to lxml started for OpenSearch
    - Rename dom elements when completed, and remove DOM requirement...
    - Rename file to avoid using dash...
    - self-numRecHarv is incorrect when harvest fails
"""

import urllib.request as ul
from urllib.parse import urlencode, quote_plus
import ssl
from xml.dom.minidom import parseString # To be removed
import codecs
import sys
import os
import getopt
from datetime import datetime
import lxml.etree as ET
import logging

def parse_cfg(cfgfile):
    # Read config file
    module.logger.info("Reading configuration from %s", cfgfile)
    with open(cfgfile, 'r') as ymlfile:
        cfgstr = yaml.full_load(ymlfile)

    return cfgstr

def initialise_logger(outputfile, name):
    if not outputfile or not name:
        raise IOError("Missing input parameters")
    # Check that logfile exists
    logdir = os.path.dirname(outputfile)
    if not os.path.exists(logdir):
        try:
            os.makedirs(logdir)
        except:
            raise IOError
    # Set up logging
    mylog = logging.getLogger(name)
    mylog.setLevel(logging.INFO)
    #logging.basicConfig(level=logging.INFO, 
    #        format='%(asctime)s - %(levelname)s - %(message)s')
    myformat = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(myformat)
    mylog.addHandler(console_handler)
    file_handler = logging.handlers.TimedRotatingFileHandler(
            outputfile,
            when='w0',
            interval=1,
            backupCount=7)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(myformat)
    mylog.addHandler(file_handler)

    return(mylog)

def check_directories(cfg):
    for section in cfg:
        for name in ['raw','mmd']:
            if not os.path.isdir(cfg[section][name]):
               try:
                   os.makedirs(cfg[section][name])
               except:
                   print("Could not create output directory")
                   return(2)
    return(0)

def setInactive(mmdDir, mmdid, mylog):

    # Create filename from id
    mmdfile = '/'.join([mmdDir, mmdid.replace('.','_')+'.xml'])
    #print('>>>>>>>>>>', mmdfile)

    # Check if file exists
    if os.path.exists(mmdfile):
        mylog.info('Found file: %s', mmdfile)
        try:
            myxml = ET.parse(mmdfile)
        except Exception as e:
            mylog.warn('Could not properly parse: %s', mmdfile)
        myroot = myxml.getroot()
        mystat = myroot.find('mmd:metadata_status', namespaces=myroot.nsmap)
        if mystat is None:
            mylog.info('Nothing to do in %s, no status provided', mmdfile)
            return
        if mystat.text == 'Active':
            mystat.text = 'Inactive'
            mylog.info('%s is set inactive', mmdfile)
            myxml.write(mmdfile, pretty_print=True)
    else:
        mylog.info('No existing file found, probably already deleted.')

    return

class MetadataHarvester(object):
    """ 
    Creates metadata-harvester object with methods for harvesting and writing
    """
    def __init__(self, logname, baseURL, records, outputDir, mmdDir, hProtocol, 
            srcfmt = None, username=None, pw=None):
        """ set variables in class """
        self.logger = logging.getLogger('.'.join([logname,'MetadataHarvester']))
        self.logger.info('Creating an instance of LocalCheckMMD')
        self.baseURL = baseURL
        self.records = records
        self.outputDir = outputDir
        self.mmdDir = mmdDir
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
            if '?' in baseURL:
                self.logger.warning('Seems this request is going towards OAI-PMH and a pycsw implementation, rewriting the request')
                records = records.replace('?','&')
            getRecordsURL = str(baseURL + records)
            self.logger.info("Harvesting metadata from: \n\tURL: %s \n\tprotocol: %s \n", getRecordsURL,hProtocol)
            start_time = datetime.now()

            # Initial phase
            self.logger.info("\n\tURL request: %s",getRecordsURL)
            myxml = self.harvestContent(getRecordsURL)
            if myxml != None:
                if "dif" in self.srcfmt:
                    self.oaipmh_writeDIFtoFile(myxml)
                elif "iso" in self.srcfmt:
                    self.oaipmh_writeISOtoFile(myxml)
                elif "rdf" in self.srcfmt:
                    # Probably should discuss keyword, rdf is quite wide but is used by several for DCAT...
                    self.oaipmh_writeDCATtoFile(myxml)
                else:
                    raise Exception("Metadata format not supported yet.")
            else:
                self.logger.error("Server is not responding properly: %s", myxml)
                raise IOError("Server to harvest is not responding properly")
            pageCounter = 1
            resumptionToken = myxml.find('.//{*}resumptionToken')
            if resumptionToken.text == None or resumptionToken.text == '0':
                self.logger.info("Nothing more to do")
                resumptionToken = None
            else:
                resumptionToken = resumptionToken.text

            self.logger.info("Resumption token found: %s",resumptionToken)

            """
            Manage resumptionToken, i.e. segmentation of results in pages
            """
            while resumptionToken != None:
                self.logger.info("\n\tHandling resumptionToken number: %d", pageCounter)
                # create resumptionToken URL parameter
                #resumptionToken = urlencode({'resumptionToken':resumptionToken})
                resumptionToken = 'resumptionToken='+resumptionToken
                # Ideally this should be handled more smooth
                resumptionTokenSpecialTreatment = ['geonetwork', 'eu-interact', 'nilu']
                #if 'geonetwork' in baseURL:
                if '?' in baseURL:
                    '''
                    Handling pycsw OAI-PMH at NILU (else NILU will trigger next) newest end point
                    '''
                    getRecordsURLLoop = str(getRecordsURL+'&'+resumptionToken)
                elif any(x in baseURL for x in resumptionTokenSpecialTreatment):
                    getRecordsURLLoop = str(baseURL+'?verb=ListRecords&'+resumptionToken)
                else:
                    getRecordsURLLoop = str(getRecordsURL+'&'+resumptionToken)
                self.logger.info("\n\tURL request: %s",getRecordsURLLoop)
                #print(type(getRecordsURLLoop))
                myxml = self.harvestContent(getRecordsURLLoop)
                if myxml != None:
                    if "dif" in self.srcfmt:
                        self.oaipmh_writeDIFtoFile(myxml)
                    elif "iso" in self.srcfmt:
                        self.oaipmh_writeISOtoFile(myxml)
                    elif "rdf" in self.srcfmt:
                        self.oaipmh_writeDCATtoFile(myxml)
                    else:
                        raise Exception("Metadata format not supported yet.")
                else:
                    self.logger.info("myxml = %s, for page %s", str(myxml), str(pageCounter))

                resumptionToken = myxml.find('.//{*}resumptionToken')
                if resumptionToken != None:
                    self.logger.info("Resumption token found: %s",resumptionToken)
                    if resumptionToken.text == '0':
                        resumptionToken = None
                    else:
                        resumptionToken = resumptionToken.text

                pageCounter += 1

            self.logger.info("Harvesting completed")
            self.logger.info("Harvesting took: %s [hh:mm:ss]", str(datetime.now()-start_time))
            self.logger.info("Number of records successfully harvested: %d", self.numRecHarv)

        elif hProtocol == 'OGC-CSW':
            getRecordsURL = str(baseURL + records)
            self.logger.info("Harvesting metadata from: \n\tURL: %s \n\tprotocol: %s \n" % (getRecordsURL,hProtocol))
            start_time = datetime.now()
            dom = self.harvestContent(getRecordsURL)
            if dom == None:
                self.logger.error("Server is not responding properly, skipping this provider...")
                #raise IOError("Server to harvest is not responding properly")
                return
            cswHeader = dom.find('csw:SearchResults',
                    namespaces={'csw':'http://www.opengis.net/cat/csw/2.0.2'})
            if cswHeader == None:
                self.logger.error("Could not parse header response, skipping this provider...")
                return
            numRecsFound = int(cswHeader.get("numberOfRecordsMatched"))
            nextRec =  int(cswHeader.get('nextRecord'))
            self.numRecsReturned = int(cswHeader.get('numberOfRecordsReturned'))
            #print('>>>',numRecsFound,nextRec, self.numRecsReturned)
            if dom != None:
                self.ogccsw_writeCSWISOtoFile(dom)
            if nextRec > 0:
                while nextRec < numRecsFound:
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

            self.logger.info("Harvesting completed")
            self.logger.info("\n\tHarvesting took: %s [h:mm:ss]", str(datetime.now()-start_time))
            self.logger.info("\n\tNumber of records successfully harvested: %d", self.numRecHarv)

        elif hProtocol == "OpenSearch":
            getRecordsURL = str(baseURL + records)
            self.logger.info("Harvesting metadata from: \n\tURL: %s \n\tprotocol: %s \n", (getRecordsURL,hProtocol))
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
                self.logger.info("Could not display all results on single page.  Starts iterating...")
            while current_results < totalResults:
                self.logger.info("\n\n\tHandling results (%s - %s) / %s" %(current_results, current_results + itemsPerPage,
                            totalResults))
                from_to = "?start=%s&rows=%s&" % (current_results,itemsPerPage)
                getRecordsURLLoop = str(baseURL + from_to + records[1:])
                dom = self.harvestContent(getRecordsURLLoop,credentials=True,uname=uname,pw=pw)
                if dom != None:
                    self.openSearch_writeENTRYtoFile(dom)
                current_results += itemsPerPage

            self.logger.info("\n\nHarvesting took: %s [h:mm:ss]\n",  str(datetime.now()-start_time))

        else:
            self.logger.error('Protocol %s is not accepted.', hProtocol)
            raise IOError("Protocol is not accepted")

        return(self.numRecHarv)

    def openSearch_writeENTRYtoFile(self,dom):
        """ Write OpenSearch ENTRY elements in fom to file"""
        self.logger.info("Writing OpenSearch ENTRY metadata elements to disk... ")

        entries = dom.getElementsByTagName('entry')
        self.logger.info("\tFound %d ENTRY elements.", entries.length)
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
        self.logger.info("\n\tNumber of records found: %d",len(record_elements))

        numRecs = len(record_elements)

        counter = 0
        for record in record_elements:
            cswid = record.find('gmd:fileIdentifier/gco:CharacterString',
                    namespaces=myns)
            if cswid == None:
                self.logger.warn("Skipping record, no FileID")
                continue
            # Dump to file...
            self.write_to_file(record, cswid.text)
            counter += 1
        self.logger.info("\n\tNumber of records written: %d", counter)
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
        self.logger.info("\n\tNumber of records found: %d",len(record_elements)+1)
        size_dif = len(record_elements)

        counter = 0
        if size_dif != 0:
            #counter = 1
            for record in record_elements:
                # Check header if deleted
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
                # Challenges arise when oaiid and isoid are different as
                # isoid is used as the filename...
                if delete_status != None:
                    # TODO: Fix MMD records if record is deleted...
                    self.logger.info("This record has been deleted:\n\t%s",oaiid)
                    # Extract identifier. These ID appears like oai:<endpoint>:<id>, need to extract the last part, but keep in mind some data centres use : in identifiers.
                    mmdid = oaiid.split(':',3)[2]
                    # Update MMD record, i.e. set Inactive if existing
                    setInactive(self.mmdDir, mmdid, self.logger)
                    continue
                isoid = record.find('oai:metadata/gmi:MI_Metadata/gmd:fileIdentifier/gco:CharacterString',
                        namespaces=myns)
                if isoid == None:
                    isoid = record.find('oai:metadata/gmd:MD_Metadata/gmd:fileIdentifier/gco:CharacterString',
                            namespaces=myns)
                if isoid == None:
                    self.logger.warn("Skipping record, no ISO ID")
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
                counter += 1
            self.logger.info("\n\tNumber of records written to files: %d", counter)
        else:
            self.logger.info("\n\tRecords did not contain ISO elements")

        self.numRecHarv += counter

        return

    def oaipmh_writeDIFtoFile(self,dom):
        """ 
        Write DIF elements in dom to file 
        """

        myns = {
                'oai':'http://www.openarchives.org/OAI/2.0/',
                'dif':'http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/',
                'xsi':'http://www.w3.org/2001/XMLSchema-instance'
                }
        record_elements =  dom.xpath('/oai:OAI-PMH/oai:ListRecords/oai:record', 
                namespaces=myns)
        self.logger.info("\n\tNumber of records found: %d",len(record_elements))
        size_dif = len(record_elements)

        counter = 0
        if size_dif != 0:
            for record in record_elements:
                # Check header if deleted
                #print(ET.tostring(record))
                #sys.exit(0)
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
                # Challenges arise when oaiid and difid are different as
                # difid is used as the filename...
                # A rewrite to handle DIF10 nested ENTRY_ID element is needed.
                if delete_status != None:
                    # TODO: Fix MMD records if record is deleted...
                    self.logger.info("This record has been deleted:\n\t%s",oaiid)
                    # Extract identifier. These ID appears like oai:<endpoint>:<id>, need to extract the last part, but keep in mind some data centres use : in identifiers.
                    mmdid = oaiid.split(':',3)[2]
                    # Update MMD record, i.e. set Inactive if existing
                    setInactive(self.mmdDir, mmdid, self.logger)
                try:
                    dif = record.find('oai:metadata/dif:DIF', namespaces=myns)
                    difschema = dif.xpath("@xsi:schemaLocation", namespaces=myns)
                except:
                    self.logger.error("Couldn't find DIF schema, skipping record.")
                    continue
                """
                Decide on handling depending on DIF 10 or previous type of record
                """
                if len(difschema) > 0 and "dif_v10" in difschema[0]:
                    difid = record.find('oai:metadata/dif:DIF/dif:Entry_ID/dif:Short_Name', namespaces=myns)
                else:
                    difid = record.find('oai:metadata/dif:DIF/dif:Entry_ID', namespaces=myns)
                if difid == None:
                    self.logger.warn("Skipping record, no DIF ID")
                    continue
                difid = difid.text
                difrec = record.find('oai:metadata/dif:DIF',
                        namespaces=myns)

                # Dump to file
                counter += 1
                print('>>>> Processing file #: ', counter)
                self.write_to_file(difrec, difid)
        else:
            self.logger.info("\n\tRecords did not contain DIF elements")

        self.logger.info("\n\tNumber of records written to files: %d", counter)
        self.numRecHarv += counter
        return

    def oaipmh_writeDCATtoFile(self,dom):
        """ 
        Write DCAT elements in dom to file 
        """
        self.logger.warning('Not implemented yet')
        myns = {
                'oai':'http://www.openarchives.org/OAI/2.0/',
                'dcat':'http://www.w3.org/ns/dcat#',
                'rdf':'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
                }
        record_elements =  dom.xpath('/oai:OAI-PMH/oai:ListRecords/oai:record', 
                namespaces=myns)
        self.logger.info("\n\tNumber of records found: %d",len(record_elements))
        size_rdf = len(record_elements)

        counter = 0
        if size_rdf != 0:
            for record in record_elements:
                # Check header if deleted
                # TODO: Check if used with DCAT
                datestamp = record.find('oai:header/oai:datestamp',
                        namespaces={'oai':'http://www.openarchives.org/OAI/2.0/'})
                if datestamp != None:
                    datestamp = datestamp.text
                oaiid = record.find('oai:header/oai:identifier',
                        namespaces={'oai':'http://www.openarchives.org/OAI/2.0/'}).text
                delete_status = record.find("oai:header[@status='deleted']",
                        namespaces={'oai':'http://www.openarchives.org/OAI/2.0/'})
                if delete_status != None:
                    self.logger.info("This record has been deleted:\n\t%s",oaiid)
                    # Extract identifier. These ID appears like oai:<endpoint>:<id>, need to extract the last part, but keep in mind some data centres use : in identifiers.
                    mmdid = oaiid.split(':',3)[2]
                    # Update MMD record, i.e. set Inactive if existing
                    setInactive(self.mmdDir, mmdid, self.logger)
                # Not sure how identifiers are handled in the stream we have access to so far.
                #dcatid = record.find('oai:metadata/dif:DIF/dif:Entry_ID', namespaces=myns)
                dcatid = oaiid
                if dcatid == None:
                    self.logger.warn("Skipping record, no DIF ID")
                    continue
                dcatrec = record.find('oai:metadata/rdf:RDF', namespaces=myns)
                # TODO: Collect the linked information...
                #print(ET.tostring(dcatrec, pretty_print=True))

                # Dump to file
                counter += 1
                self.write_to_file(dcatrec, dcatid)
        else:
            self.logger.info("\n\tRecords did not contain DIF elements")

        self.logger.info("\n\tNumber of records written to files: %d", counter)
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
               self.logger.error("Could not create output directory: %s", self.outputDir)
               sys.exit(2)

        myid = myid.replace('/','-')
        myid = myid.replace(':','-')
        myid = myid.replace('.','-')
        filename = self.outputDir+'/'+myid+'.xml'
        outputstr = ET.ElementTree(record)
        try:
            self.logger.info('Creating file: %s', filename)
            outputstr.write(filename, pretty_print=True,
                    xml_declaration=True, standalone=None, 
                    encoding="UTF-8")
        except:
            self.logger.error("Could not create output file: %s", filename)
            raise Exception("Could not create output file.")
            sys.exit(2)
        return

    def harvestContent(self,URL,credentials=False,uname="foo",pw="bar"):
        ssl._create_default_https_context = ssl._create_unverified_context        
        """ Function for harvesting content from URL."""
        try:
            if not credentials:
                # Timeout depends on user, 60 seconds is too little for
                # NSIDC and NPOLAR, increasing to 5 minutes
                ##myreq = ul.Request(URL)
                try:
                    with ul.urlopen(URL,timeout=300) as response:
                        ##print('>>>>', response.getheader('Content-Type'))
                        # This is a bit awkward, but in order to improve robustness, multiple checks are required. Could be simplified, but not necessarily more readable.
                        if response.getheader('Content-Type') is None:
                            self.logger.warn('No Content-Type received from the server.')
                            myencoding = 'UTF-8'
                        elif 'charset' in response.getheader('Content-Type'):
                            myencoding = response.getheader('Content-Type').split('=',1)[1] 
                        elif 'application/xml' in response.getheader('Content-Type'):
                            self.logger.warn('No charset provided, assuming UTF-8')
                            myencoding = 'UTF-8'
                        else:
                            self.logger.warn('No Content-Type received from the server. Not sure why we ended up here. Assuming UTF-8')
                            self.logger.warn('Header received: %s', response.getheader('Content-Type'))
                            myencoding = 'UTF-8'
                        #myfile = bytes(response.read())
                        myfile = response.read()
                    myparser = ET.XMLParser(ns_clean=True,
                            encoding=myencoding)
                    try:
                        data = ET.fromstring(myfile,myparser)
                    except Exception as e:
                        self.logger.error('Parsing the harvested information failed due to: %s', e)
                    return data
                except Exception as e:
                    self.logger.error('Couldn not retrieve data: %s', e)
            else:
                # Not working with lxml
                self.logger.warn("Authenticated and authorised use is not implemented yet...")
                return
#                p = ul.HTTPPasswordMgrWithDefaultRealm()
#                p.add_password(None, URL, uname, pw)
#                handler = ul.HTTPBasicAuthHandler(p)
#                opener = ul.build_opener(handler)
#                ul.install_opener(opener)
#                return parseString(ul.urlopen(URL).read())
        except Exception as e:
            self.logger.error("There was an error with the URL request. Could not open or parse content from: \n\t %s\n\t%s", URL, e)

    def oaipmh_resumptionToken(self,URL):
        # Not used currently, to be removed?
        """ Function for handling resumptionToken in OAI-PMH"""
        #print("Now in resumptionToken...", URL)
        try:
            file = ul.request.urlopen(URL, timeout=300)
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
        except ul.error.URLError as e:
            self.logger.error("There was an error with the URL request: %s", e)
