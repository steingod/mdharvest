#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
PURPOSE:
    The purpose of this snippet is to add elements to a MMD file.
    Specifically this addresses the issues of adding/deleting the
    collection keyword to files depending on the results of the
    filter-mmd-records.py output or other filtering mechanisms. The
    easiest way to do this is to use a basic XSLT that is modified for the
    specific purpose. This basic XSLT will be the mmd2sequence.xsl that
    was generated for rearranging the existing MMD documents for
    compliance with the new XSD in March 2018. The main purpose of this
    software is to apply that XSØLT on a list of files.

AUTHOR:
    Øystein Godøy, METNO/FOU, 2018-04-15 

UPDATED:

NOTES:
    - Based on the current version of xmltransform.py 2018-04-15.
    - Changing files in their existing location (should be in a SVN
      repository)
    - Checking whether the requested element is already existing in the
      file (i.e. do not duplicate elements). This is currently not
      supported.

"""

import sys
import os
import getopt
import uuid
import lxml.etree as ET
import codecs

def usage():
    print sys.argv[0]+" [options] input"
    print "\t-h|--help: dump this information"
    print "\t-l|--list: specify list of files to operate on"
    print "\t-s|--style: specify stylesheet to use"
    sys.exit(2)

# Keeping this for now, could be useful in the future. Could also
# potentially be moved to a function library.
def create_uuid(infile,lastupdate):
    string2use = "https://arcticdata.met.no/ds/"+os.path.basename(infile)+"-"
    string2use += lastupdate
    myuuid = uuid.uuid5(uuid.NAMESPACE_URL,string2use)
    return(myuuid)

def main(argv):
    # This is the main method

    # Parse command line arguments
    try:
        opts, args = getopt.getopt(argv,"hl:s:",
                ["help","list","style"])
    except getopt.GetoptError:
        usage()

    lflg = sflg = False
    for opt, arg in opts:
        if opt == ("-h","--help"):
            usage()
        elif opt in ("-l","--list"):
            filelist = arg
            lflg = True
        elif opt in ("-s","--style"):
            stylesheet = arg
            sflg = True

    if not lflg:
        usage()
    elif not sflg:
        usage()

    # Define stylesheet
    try:
        myxslt = ET.parse(stylesheet)
    except ET.XMLSyntaxError as e:
        print "Error parsing stylesheet: ",e
        sys.exit(1)
    mytransform = ET.XSLT(myxslt)

    # Find files to process
    if not os.path.isfile(filelist):
        print "This is not a file"
        sys.exit(2)
    try:
        f = open(filelist,"r")
    except IOerror:
        print "Could not open input file"
        sys.exit(2)
    mylist = f.read().splitlines()
    f.close()

    # Process files
    i=0
    s = "/"
    for myfile in mylist:
        print "Processing ["+myfile+"]", i
        i += 1
        try:
            inxml = ET.parse(myfile)
        except ET.XMLSyntaxError:
            print "Could not parse input XML"
            sys.exit(2)
        ET.cleanup_namespaces(inxml)
        newxml = mytransform(inxml)
        output = codecs.open((myfile),"w", "UTF-8")
        #output = codecs.open(("mynewxml.xml"),"w", "UTF-8")
        output.write(ET.tostring(newxml, pretty_print=True,
            xml_declaration=True))
        output.close()

    sys.exit(0)

if __name__ == '__main__':
    main(sys.argv[1:])
