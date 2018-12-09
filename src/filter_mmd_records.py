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

def usage():
    print sys.argv[0]+" [options] input"
    print "\t-h|--help: dump this information"
    print "\t-c|--configuration: specify where to find the configuration file"
    print "\t-l|--collection: specify collection to tag the dataset with"
    print "\t-p|--parameters: specify parameters to extract (comma separated)"
    print "\t-b|--bounding: specify the bounding box (N, E, S, W) as comma separated list"
    print "\t-c|--collection: specify the collection to add (comma separated)"
    print "\t-g|--gcw: adds cryosphere parameters (and GCW collection)"
    sys.exit(2)

class CheckMMD():
    def __init__(self, mmd_file, bounding, parameters, collection):
        self.mmd_file = mmd_file
        self.bbox = bounding
        self.params = parameters
        self.coll = collection

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
        tmpcoll = self.coll
        tree = ET.ElementTree(file=mmd_file)
        root = tree.getroot()
        mynsmap = {'mmd':'http://www.met.no/schema/mmd'}
        #print ET.tostring(root)

        setInactive = False
        # Check for empty or incomplete bounding box
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
                if myvalue == None:
                    setInactive = True
        # Check for multiple bounding box
        # Check for multiple temporal periods
        elements = tree.findall("mmd:temporal_extent",
                namespaces=mynsmap)
        if len(elements) != 1:
            print "Too few or too many temporal extent elements..."
            setInactive = True

        if setInactive:
            myelement = tree.find("mmd:metadata_status",
                    namespaces=mynsmap)
            myelement.text = "Inactive"

        # Check bounding box
        if self.params:
            elements = tree.findall("mmd:keywords[@vocabulary='GCMD']/mmd:keyword",
                    namespaces=mynsmap)
        if self.bbox:
            elements = tree.findall('mmd:geographic_extent/mmd:rectangle',
                    namespaces=mynsmap)

        #if not elements:
        #    print "Did not find any elements of the type requested..."
        #    return False
        if self.bbox:
            if self.check_bounding_box(elements,root):
                mymatch = True
        if self.params:
            if self.check_params(elements,root):
                mymatch = True
        if mymatch == False and setInactive == False:
            return mymatch

        # Check if the collection is already added
        #print ">>>>>>>>", mymatch
        for item in tmpcoll:
            #print ">>>>>>>>>>>>>>>>>>>>>>>",item
            myel = '//mmd:collection[text()="'+item+'"]'
            myelement = tree.xpath(myel, namespaces=mynsmap)
            if myelement:
                print "Already belongs to",item
                tmpcoll.remove(item)

        if not tmpcoll:
            print "No collections left to check"
            if not setInactive:
                return mymatch

        # Add new collections
        myelement = tree.find('mmd:collection', namespaces=mynsmap)

        if myelement is None:
            print "No collection found"
            if not setInactive:
                return mymatch
        collection = myelement.getparent()
        for item in tmpcoll:
            collection.insert(collection.index(myelement),
                    ET.XML("<mmd:collection xmlns:mmd='http://www.met.no/schema/mmd'>"+item+"</mmd:collection>"""))
        #print ET.tostring(tree)
        #print "Dumping information to file", mmd_file
        tree = ET.ElementTree(collection)
        #print ">>>> ",self.mmd_file
        tree.write(mmd_file, pretty_print=True)

        return mymatch

def main(argv):
    # This is the main method

    # Parse command line arguments
    try:
        opts, args = getopt.getopt(argv,"hc:p:b:l:g",
                ["help","configuration","parameters","bounding","collection","gcw"])
    except getopt.GetoptError:
        usage()

    cflg = pflg = bflg = lflg = gflg = False
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

    if not cflg:
        usage()
    elif not (lflg or gflg):
        usage()

    # Define parameters to find
    # Not working yet...
    if pflg:
        parameters = parameters.split(',')
    else:
        parameters = None

    # If filtering for GCW, parameters and collection are added automatic
    if gflg:
        parameters = ["CRYOSPHERE",
                "TERRESTRIAL HYDROSPHERE &gt; SNOW/ICE",
                "OCEANS &gt; SEA ICE"]
        collection = "GCW"

    # Define collections to add
    if cflg:
        collection = collection.split(',')
    else:
        collection = None

    # Read config file
    print "Reading", cfgfile
    with open(cfgfile, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    # Define bounding box
    # Provided as comma separated list (S,W,N,E)
    if bflg:
        bbox = bounding.split(",")
        bounding = [float(i) for i in bbox]
    else:
        bounding = None

    # Each section is a data centre to handle
    for section in cfg:
        if section == 'CCIN':
            continue
        if section != 'NPI':
            continue
        # Find files to process
        try:
            myfiles = os.listdir(cfg[section]['mmd'])
        except os.error:
            print os.error
            sys.exit(1)

        # Process files, dump valid filenames to file
        f = open("tmpfile.txt","w+")
        i=0
        s = "/"
        for myfile in myfiles:
            if myfile.endswith(".xml"):
                print i, myfile
                i += 1
                #inxml = ET.parse(s.join((indir,myfile)))
                file2check = CheckMMD(s.join((cfg[section]['mmd'],myfile)),bounding, parameters, collection)
                if file2check.check_mmd():
                    print "Success"
                else:
                    print "Failure"
        f.close()


if __name__ == '__main__':
    main(sys.argv[1:])
