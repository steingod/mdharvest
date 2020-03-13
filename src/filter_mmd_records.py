#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
PURPOSE:
    This is an reimplmentation of the Perl script filter-dif-records used
    to subset the harvested discovery metadata records. It relates only to
    MMD files and can add collection elements to existing files or write
    files to a new repository. Filtering is done on the basis of
    geographical location and/or parameter content as defined by GCMD DIF
    keywords.

AUTHOR:
    Øystein Godøy, METNO/FOU, 2018-03-27 

UPDATED:
    Øystein Godøy, METNO/FOU, 2018-12-12:
        Added more filtering of time specifications.
    Øystein Godøy, METNO/FOU, 2018-12-11
        Added filtering for NORMAP
        Added check of time strings (temporary fix as SolR ingestion is
        wrong)
    Øystein Godøy, METNO/FOU, 2018-12-08 
        Changed name, plus updated
    Øystein Godøy, METNO/FOU, 2018-06-23 
        Added parameter match

NOTES:
    - Not working...
    - Using the same configuration as harvest and transformation
    - Once working for bounding box, create functions for specific
      purposes...
    - Only valid in the Northern hemisphere as of now.
    - Add option to only process new files
    - Add option to add collection from project names (e.g. NERSC Normap)

"""

import sys
import os
import getopt
import lxml.etree as ET
import codecs
import re
import yaml
import datetime
from dateutil.parser import parse

def usage():
    print sys.argv[0]+" [options] input"
    print "\t-h|--help: dump this information"
    print "\t-c|--configuration: specify where to find the configuration file"
    print "\t-l|--collection: specify collection to tag the dataset with"
    print "\t-p|--parameters: specify parameters to filter on (comma separated)"
    print "\t-b|--bounding: specify the bounding box (N, E, S, W) as comma separated list"
    print "\t-g|--gcw: checks cryosphere parameters (and adds GCW collection)"
    print "\t-s|--sios: checks bounding box (and adds SIOS collection)"
    print "\t-n|--nmap: checks project affiliation (and adds NMAP collection)"
    print("Options g, s and n cannot be used simultaneously")
    sys.exit(2)

class LocalCheckMMD():
    def __init__(self, section, mmd_file, bounding, parameters, mycollection,
            project):
        self.section = section
        self.mmd_file = mmd_file
        self.bbox = bounding
        self.params = parameters
        self.coll = mycollection
        self.project = project

    def check_project(self,elements,root):
        projmatch = False
        for proj in self.project:
            if any(proj in mystring.text for mystring in elements):
                projmatch = True

        if projmatch:
            return True
        else:
            return False


    def check_params(self,elements,root):

        parmatch = False

        #print "Now in check params..."
        #for myel in elements:
        #    print ">>>", ET.tostring(myel)
        #    print ">>>", myel.text

        for par in self.params:
            #print ">>>>>", par
            #print type(elements)
            if any(par in mystring.text for mystring in elements):
                parmatch = True
                #print ">>>>>>>>",parmatch

        if parmatch:
            return True
        else:
            return False

    def check_bounding_box(self,elements,root):
        print("####",elements)
        if len(elements) > 1:
            print "Found more than one element, not handling this now..."
            return False
        #print ET.tostring(elements[0],pretty_print=True)
        # Decode bounding box from XML
        thisbb = []
        for el in elements:
            northernmost = float(el.find('mmd:north',namespaces=root.nsmap).text)
            thisbb.append(northernmost)
            easternmost = float(el.find('mmd:east',namespaces=root.nsmap).text)
            thisbb.append(easternmost)
            southernmost = float(el.find('mmd:south',namespaces=root.nsmap).text)
            thisbb.append(southernmost)
            westernmost = float(el.find('mmd:west',namespaces=root.nsmap).text)
            thisbb.append(westernmost)

        # Check bounding box
        # Lists are ordered N, E, S, W
        print "This bounding box",thisbb
        print "Reference bounding box",self.bbox
        if len(thisbb)==0:
            return(False)
        latmatch = False
        lonmatch = False
        if (thisbb[0] < self.bbox[0]) and (thisbb[2] > self.bbox[2]):
            latmatch = True
            print "Latitude match"
        if (thisbb[1] < self.bbox[1]) and (thisbb[3] > self.bbox[3]):
            lonmatch = True
            print "Longitude match"

        if (latmatch and lonmatch):
            return True
        else:
            return False
            
    def check_mmd(self):
        mymatch = False
        mmd_file = self.mmd_file
        tmpcoll = []
        tmpcoll.append(self.coll)
        tree = ET.ElementTree(file=mmd_file)
        root = tree.getroot()
        mynsmap = {'mmd':'http://www.met.no/schema/mmd'}
        #print ET.tostring(root)

        setInactive = False
        cnvDateTime = False
        # Set all NPI records hosted by IMR for NMDC to inactive
        if self.section == "IMR":
            myvalue = tree.find("mmd:data_center/mmd:data_center_name/mmd:long_name",
                namespaces=mynsmap).text
            if myvalue in ["Norwegian Polar Institute"]:
                setInactive = True
        if not setInactive:
            # Check for empty or incomplete bounding box
            # Add check for multiple bounding box
            elements = tree.findall("mmd:geographic_extent",
                    namespaces=mynsmap)
            if len(elements) != 1:
                print "Error in bounding box (too few or too many)..."
                setInactive = True
            bboxfields = ['mmd:north','mmd:east','mmd:south','mmd:west']
            for myel in elements:
                for myfield in bboxfields:
                    myvalue = myel.find('mmd:rectangle/'+myfield,
                            namespaces=root.nsmap).text
                    if myvalue == None or myvalue.isspace():
                        setInactive = True
            # Check for multiple temporal periods
            # This should be removed as this is supported by the MMD
            # specification, but not supproted by the SolR ingestion.
            elements = tree.findall("mmd:temporal_extent",
                    namespaces=mynsmap)
            if len(elements) != 1:
                print "Too few or too many temporal extent elements..."
                setInactive = True
            # Check DateTime strings (to be removed later)
            if not setInactive:
                for item in elements[0].iterdescendants():
                    #print type(item), item.tag, item.text
                    if re.match('\d{4}-\d{2}-\d{2}Z', item.text):
                        #item.text = parse(item.text).date().strftime("%Y-%m-%d")
                        item.text = item.text[:-1]
                        cnvDateTime = True
                    elif re.match('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', item.text):
                        item.text = parse(item.text).date().strftime("%Y-%m-%d")
                        cnvDateTime = True

        if setInactive:
            myelement = tree.find("mmd:metadata_status",
                    namespaces=mynsmap)
            myelement.text = "Inactive"

        #print "#####",setInactive

        # Check parameters,bounding box and project
        if self.params and not setInactive:
            elements = tree.findall("mmd:keywords[@vocabulary='GCMD']/mmd:keyword",
                    namespaces=mynsmap)
        if self.bbox and not setInactive:
            elements = tree.findall('mmd:geographic_extent/mmd:rectangle',
                    namespaces=mynsmap)
        if self.project and not setInactive:
            elements = tree.findall('mmd:project/mmd:short_name',
                    namespaces=mynsmap)
        # Check information found
        # some check of elements content...


        # Decide on test
        if self.bbox and not setInactive:
            if self.check_bounding_box(elements,root):
                mymatch = True
        if self.params and not setInactive:
            if self.check_params(elements,root):
                mymatch = True
        if self.project and not setInactive:
            if self.check_project(elements,root):
                mymatch = True
        if (mymatch == False and 
            setInactive == False and cnvDateTime == False):
            return mymatch

        # Check if the collection is already added, and add if not
        if mymatch:
            for item in tmpcoll:
                myel = '//mmd:collection[text()="'+item+'"]'
                myelement = tree.xpath(myel, namespaces=mynsmap)
                if myelement:
                    print "Already belongs to",item
                    #tmpcoll.remove(item)
                else:
                    # Add new collections
                    myelement = tree.find('mmd:collection', namespaces=mynsmap)
                    if myelement is not None:
                        mycollection = myelement.getparent()
                        mycollection.insert(mycollection.index(myelement),
                                ET.XML("<mmd:collection xmlns:mmd='http://www.met.no/schema/mmd'>"+item+"</mmd:collection>"""))
        #tree = ET.ElementTree(mycollection)
        tree.write(mmd_file, pretty_print=True)

        return mymatch

def main(argv):
    # This is the main method

    # Parse command line arguments
    try:
        opts, args = getopt.getopt(argv,"hc:p:b:l:gsn",
                ["help",
                    "configuration","parameters","bounding",
                    "collection","gcw","sios","nmap"])
    except getopt.GetoptError:
        usage()

    cflg = pflg = bflg = lflg = gflg = sflg = nflg = False
    for opt, arg in opts:
        if opt == ("-h","--help"):
            usage()
        elif opt in ("-c","--configuration"):
            cfgfile = arg
            cflg = True
        elif opt in ("-p","--parameters"):
            parameters = arg
            pflg = True
        elif opt in ("-b","--bounding"):
            bounding = arg
            bflg = True
        elif opt in ("-l","--collection"):
            collection = arg
            lflg = True
        elif opt in ("-g","--gcw"):
            gflg = True
        elif opt in ("-s","--sios"):
            sflg = True
        elif opt in ("-n","--nmap"):
            nflg = True

    if not cflg:
        usage()
    elif not (lflg or gflg or sflg or nflg):
        usage()
    if (nflg and sflg) or (sflg and gflg) or (nflg and gflg):
        usage()

    # Define parameters to find
    # Not working yet...
    if pflg:
        parameters = parameters.split(',')
    else:
        parameters = None

    # If filtering for GCW, parameters and collection are added automatic
    bounding = project = None
    if gflg:
        parameters = ["CRYOSPHERE",
                "TERRESTRIAL HYDROSPHERE &gt; SNOW/ICE",
                "OCEANS &gt; SEA ICE"]
        collection = "GCW"
    if sflg:
        bounding = [90.,40.,70.,-20.]
        collection = "SIOS"
    if nflg:
        project = "NORMAP"
        collection = "NMAP"

    # Define collections to add
    if lflg:
        collection = collection.split(',')
    elif ((not sflg) and (not gflg) and (not nflg)):
        collection = None

    # Read config file
    print "Reading", cfgfile
    with open(cfgfile, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    # Define bounding box
    # Provided as comma separated list (N,E,S,W)
    if bflg:
        bbox = bounding.split(",")
        bounding = [float(i) for i in bbox]

    # Each section is a data centre to handle
    for section in cfg:
        if section not in ["NPI","IMR"]:
            continue
        # Find files to process
        try:
            myfiles = os.listdir(cfg[section]['mmd'])
        except os.error:
            print os.error
            sys.exit(1)

        # Process files, dump valid filenames to file
        f = open("tmpfile.txt","w+")
        i=1
        s = "/"
        for myfile in myfiles:
            if myfile.endswith(".xml"):
                print i, myfile
                i += 1
                file2check = LocalCheckMMD(section,s.join((cfg[section]['mmd'],myfile)),
                        bounding, parameters, collection, project)
                if file2check.check_mmd():
                    print "Success"
                else:
                    print "Failure"
        f.close()


if __name__ == '__main__':
    main(sys.argv[1:])
