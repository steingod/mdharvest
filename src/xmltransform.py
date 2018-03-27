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
        sys.exit(1)
    mytransform = ET.XSLT(myxslt)

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
            newxml = mytransform(inxml)
            output = codecs.open(s.join((outdir,myfile)),"w", "utf-8")
            output.write(ET.tostring(newxml, pretty_print=True))
            output.close()
            sys.exit(1)

    sys.exit(0)

if __name__ == '__main__':
    main(sys.argv[1:])
