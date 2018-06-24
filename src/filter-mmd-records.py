#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
PURPOSE:
    This is an reimplmentation of the Perl script filter-dif-records used
    to subset the harvested discovery metadata records. It relates only to
    MMD files and can add collection elements to existing files or write
    files to a new repository.

AUTHOR:
    Øystein Godøy, METNO/FOU, 2018-03-27 

UPDATED:
    Øystein Godøy, METNO/FOU, 2018-06-23 
        Added parameter match

NOTES:
    - Not working...
    - Once working for bounding box, create functions for specific
      purposes...
    - Only valid in the Northern hemisphere as of now.

"""

import sys
import os
import getopt
import lxml.etree as ET
import codecs

def usage():
    print sys.argv[0]+" [options] input"
    print "\t-h|--help: dump this information"
    print "\t-i|--indir: specify where to get input"
    print "\t-o|--outdir: specify where to put results"
    print "\t-p|--parameters: specify parameters to extract (comma separated)"
    print "\t-b|--bounding: specify the bounding box (N, E, S, W) as comma separated list"
    print "\t-c|--collection: specify the collection to add (comma separated)"
    sys.exit(2)

class CheckMMD():
    def __init__(self, mmd_file):
        self.mmd_file = mmd_file

    def check_params(self,elements,root,params):

        parmatch = False

        #print "Now in check params..."

        for el in elements:
            #print el.text
            for p in params:
                if p in el.text:
                    parmatch = True

        #print parmatch

        if parmatch:
            return True
        else:
            return False

    def check_bounding_box(self,elements,root,bbox):
        if len(elements) > 1:
            print "Found more than one element, not handling this now..."
            return sys.exit(2)
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
        print "Reference bounding box",bbox
        latmatch = False
        lonmatch = False
        if (thisbb[0] < bbox[0]) and (thisbb[2] > bbox[2]):
            latmatch = True
            print "Latitude match"
        if (thisbb[1] < bbox[1]) and (thisbb[3] > bbox[3]):
            lonmatch = True
            print "Longitude match"

        if (latmatch and lonmatch):
            return True
        else:
            return False
            

    def check_mmd(self, bbox, params, coll):
        mymatch = False
        mmd_file = self.mmd_file
        tree = ET.ElementTree(file=mmd_file)
        root = tree.getroot()
        mynsmap = {'mmd':'http://www.met.no/schema/mmd'}
        #print ET.tostring(root)

        # Check bounding box
        if params:
            #print "Checking for parameters"
            #print params
            elements = tree.findall("mmd:keywords[@vocabulary='GCMD']/mmd:keyword",
                    namespaces=mynsmap)
        if bbox:
            #print "Checking for bounding box"
            #print bbox
            elements = tree.findall('mmd:geographic_extent/mmd:rectangle',
                    namespaces=mynsmap)

        if not elements:
            print "Did not find any elements of the type requested..."
            return False
        if bbox:
            if self.check_bounding_box(elements,root,bbox):
                mymatch = True
        if params:
            if self.check_params(elements,root,params):
                mymatch = True

        # Check if the collection is already added
        for item in coll:
            myel = '//mmd:collection[text()="'+item+'"]'
            myelement = tree.xpath(myel, namespaces=mynsmap)
            if myelement:
                coll.remove(item)

        if not coll:
            return mymatch

        # Add new collections
        myelement = tree.find('mmd:collection', namespaces=mynsmap)

        if myelement is None:
            return mymatch
        collection = myelement.getparent()
        for item in coll:
            collection.insert(collection.index(myelement),
                    ET.XML("<mmd:collection xmlns:mmd='http://www.met.no/schema/mmd'>"+item+"</mmd:collection>"""))
        #print ET.tostring(tree)
        tree = ET.ElementTree(collection)
        tree.write('mynewxml.xml', pretty_print=True)

        sys.exit()
        return mymatch

def main(argv):
    # This is the main method

    # Parse command line arguments
    try:
        opts, args = getopt.getopt(argv,"hi:o:p:b:c:",
                ["help","indir","outdir","parameters","bounding","collection"])
    except getopt.GetoptError:
        usage()

    iflg = oflg = pflg = bflg = cflg = False
    for opt, arg in opts:
        if opt == ("-h","--help"):
            usage()
        elif opt in ("-i","--indir"):
            indir = arg
            iflg = True
        elif opt in ("-o","--outdir"):
            outdir = arg
            oflg = True
        elif opt in ("-p","--parameters"):
            parameters = arg
            pflg = True
        elif opt in ("-b","--bounding"):
            bounding = arg
            bflg = True
        elif opt in ("-c","--collection"):
            collection = arg
            cflg = True

    if not iflg:
        usage()
    elif not cflg:
        usage()

    # Define parameters to find
    # Not working yet...
    if pflg:
        parameters = parameters.split(',')
    else:
        parameters = None
    if cflg:
        collection = collection.split(',')
    else:
        collection = None

    # Define bounding box
    # Provided as comma separated list (S,W,N,E)
    if bflg:
        bbox = bounding.split(",")
        #print bounding
        #print bbox
        bounding = [float(i) for i in bbox]
        #print bounding
    else:
        bounding = None


    # Find files to process
    try:
        myfiles = os.listdir(indir)
    except os.error:
        print os.error
        sys.exit(1)
    
    # Check that the destination exists, create if not
    if not os.path.exists(outdir):
        print "Output directory does not exist, trying to create it..."
        try:
            os.makedirs(outdir)
        except OSError as e:
            print e
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
            file2check = CheckMMD(s.join((indir,myfile)))
            if file2check.check_mmd(bounding, parameters, collection):
                print "Success"
                #f.write(s.join((indir,myfile))+"\n")
            else:
                print "Failure"
    f.close()


if __name__ == '__main__':
    main(sys.argv[1:])
