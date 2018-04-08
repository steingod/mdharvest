#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
PURPOSE:
    This is an reimplmentation of the Perl script filter-dif-records used
    to subset the harvested discovery metadata records.

AUTHOR:
    Øystein Godøy, METNO/FOU, 2018-03-27 

UPDATED:

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
    print "\t-p|--parameters: specify parameters to extract"
    print "\t-b|--bounding: specify the bounding box (N, E, S, W) as comma separated list"
    sys.exit(2)

class CheckMMD():
    def __init__(self, mmd_file):
        self.mmd_file = mmd_file

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
            

    def check_mmd(self, bbox):
        mmd_file = self.mmd_file
        tree = ET.ElementTree(file=mmd_file)
        root = tree.getroot()
        #print ET.tostring(root)

        # Check bounding box
        elements = tree.findall('.//mmd:geographic_extent/mmd:rectangle',namespaces=root.nsmap)
        if not elements:
            print "Did not find any bounding box of type rectangular..."
            return False
        if self.check_bounding_box(elements,root,bbox):
            return True
        else:
            return False

def main(argv):
    # This is the main method

    # Parse command line arguments
    try:
        opts, args = getopt.getopt(argv,"hi:o:p:b:",
                ["help","indir","outdir","parameters","bounding"])
    except getopt.GetoptError:
        usage()

    iflg = oflg = pflg = bflg = False
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

    if not iflg:
        usage()
    elif not oflg:
        usage()

    # Define parameters to find
    # Not working yet...

    # Define bounding box
    # Provided as comma separated list (S,W,N,E)
    if bflg:
        bbox = bounding.split(",")
        #print bounding
        #print bbox
        bounding = [float(i) for i in bbox]
        #print bounding


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
            check_file = CheckMMD(s.join((indir,myfile)))
            if check_file.check_mmd(bounding):
                print "Success"
                f.write(s.join((indir,myfile))+"\n")
            else:
                print "Failure"
    f.close()


if __name__ == '__main__':
    main(sys.argv[1:])
