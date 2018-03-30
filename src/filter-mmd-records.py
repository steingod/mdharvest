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
    print "\t-s|--style: specify the stylesheet to use"
    sys.exit(2)


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
        print bounding
        print bbox


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

    # Process files
    i=0
    s = "/"
    for myfile in myfiles:
        if myfile.endswith(".xml"):
            print i, myfile
            i += 1
            inxml = ET.parse(s.join((indir,myfile)))


if __name__ == '__main__':
    main(sys.argv[1:])
