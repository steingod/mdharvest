#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
PURPOSE:
    This is an reimplementation of the Perl script transform-xml-files
    used to convert discovery metadata harvested from collaborators using
    the harvest tools available in this toolbox.

AUTHOR:
    Øystein Godøy, METNO/FOU, 2018-03-27 

UPDATED:

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
        opts, args = getopt.getopt(argv,"hi:o:s:",
                ["help","indir","outdir","style"])
    except getopt.GetoptError:
        usage()

    iflg = oflg = sflg = False
    for opt, arg in opts:
        if opt == ("-h","--help"):
            usage()
        elif opt in ("-i","--indir"):
            indir = arg
            iflg =True
        elif opt in ("-o","--outdir"):
            outdir = arg
            oflg =True
        elif opt in ("-s","--style"):
            stylesheet = arg
            sflg =True

    if not iflg:
        usage()
    elif not oflg:
        usage()
    elif not sflg:
        usage()

    # Define stylesheet
    try:
        myxslt = ET.parse(stylesheet)
    except ET.error:
        print ET.error

    # Find files to process
    try:
        myfiles = os.listdir(indir)
    except os.error:
        print os.error
    
    # Check that the destination exists, create if not
    if not os.path.exists(indir):
        try:
            os.makedirs(outdir)
        except OSError as e:
            print e

    # Process files
    for file in myfiles:
        if file.endswith(".xml"):
            print file

    sys.exit(0)

    dom = ET.parse(xml_filename)
xslt = ET.parse(xsl_filename)
transform = ET.XSLT(xslt)
newdom = transform(dom)
print(ET.tostring(newdom, pretty_print=True))
