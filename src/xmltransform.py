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
    Øystein Godøy, METNO/FOU, 2018-04-15 
        Conversion of MM2, two level datasets and creation of identifiers
        are now working. Also processing only one file.
    Øystein Godøy, METNO/FOU, 2018-04-14 
        Support for MM2 conversion (including information from XMD)

NOTES:
    - NA

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
    print "\t-i|--indir: specify where to get input"
    print "\t-o|--outdir: specify where to put results"
    print "\t-s|--style: specify the stylesheet to use"
    print "\t-x|--xmd: input is MM2 with XMD files"
    print "\t-p|--parent: UUID of parent dataset"
    print "\t-f|--file: treat input as file, not directory"
    sys.exit(2)

def create_uuid(infile,lastupdate):
    string2use = "https://arcticdata.met.no/ds/"+os.path.basename(infile)+"-"
    string2use += lastupdate
    myuuid = uuid.uuid5(uuid.NAMESPACE_URL,string2use)
    return(myuuid)

def main(argv):
    # This is the main method

    # Parse command line arguments
    try:
        opts, args = getopt.getopt(argv,"hi:o:s:xp:f",
                ["help","indir","outdir","style","xmd","parent","file"])
    except getopt.GetoptError:
        usage()

    iflg = oflg = sflg = xflg = pflg = fflg = False
    for opt, arg in opts:
        if opt == ("-h","--help"):
            usage()
        elif opt in ("-i","--indir"):
            indir = arg
            iflg = True
        elif opt in ("-o","--outdir"):
            outdir = arg
            oflg = True
        elif opt in ("-s","--style"):
            stylesheet = arg
            sflg = True
        elif opt in ("-x","--xmd"):
            xflg = True
        elif opt in ("-p","--parent"):
            parentUUID = arg
            pflg = True
        elif opt in ("-f","--file"):
            fflg = True

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
    if fflg:
        if not os.path.isfile(indir):
            print "This is not a file"
            sys.exit(2)
        myfiles = [os.path.basename(indir)]
        indir = os.path.dirname(indir)
    else:
        try:
            myfiles = os.listdir(indir)
        except OSError as e:
            print e
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
        xmlfile = s.join((indir,myfile))
        print "Processing",xmlfile, i
        if myfile.endswith(".xml"):
            if xflg:
                xmdfile = s.join((indir,myfile.replace(".xml",".xmd")))
                if not os.path.isfile(xmdfile):
                    print xmdfile, "not found"
                    continue
                xmd = ET.parse(xmdfile)
                xmdlastupdate = xmd.xpath("//ds:info/@datestamp", \
                        namespaces={'ds':'http://www.met.no/schema/metamod/dataset'})[0]
                collection = xmd.xpath("//ds:info/@ownertag", \
                        namespaces={'ds':'http://www.met.no/schema/metamod/dataset'})[0]
                myuuid = create_uuid(xmlfile,xmdlastupdate)
            i += 1
            inxml = ET.parse(xmlfile)
            if xflg:
                if pflg:
                    newxml = mytransform(inxml,
                        xmd=ET.XSLT.strparam(xmdfile),
                        mmdid=ET.XSLT.strparam(str(myuuid)),
                        parentDataset=ET.XSLT.strparam(str(parentUUID)))
                else:
                    newxml = mytransform(inxml,
                        xmd=ET.XSLT.strparam(xmdfile),
                        mmdid=ET.XSLT.strparam(str(myuuid)))
            else:
                newxml = mytransform(inxml)
            output = codecs.open(s.join((outdir,myfile)),"w", "utf-8")
            output.write(ET.tostring(newxml, pretty_print=True))
            output.close()

    sys.exit(0)

if __name__ == '__main__':
    main(sys.argv[1:])
