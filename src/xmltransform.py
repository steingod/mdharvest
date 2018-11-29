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
    Øystein Godøy, METNO/FOU, 2018-11-28 
        Added utilisation of configuration file for mdharvest.
    Øystein Godøy, METNO/FOU, 2018-04-15 
        Conversion of MM2, two level datasets and creation of identifiers
        are now working. Also processing only one file.
    Øystein Godøy, METNO/FOU, 2018-04-14 
        Support for MM2 conversion (including information from XMD)

NOTES:
    - Remove options not used anymore
        - Do we need indir, outdir, parent etc still? Only for MM2/XMD
          conversion?
    - Make methods for both processing of files and modification of XSLTs??

"""

import sys
import os
import getopt
import uuid
import lxml.etree as ET
import codecs
import yaml

def usage():
    print sys.argv[0]+" [options] input"
    print "\t-h|--help: dump this information"
    print "\t-c|--cfg: specify configuration file (same as mdharvest)"
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

def check_directories(cfg):
    for section in cfg:
        for name in ['dest1','dest2','mmd']:
            if not os.path.isdir(cfg[section][name]):
               try:
                   os.makedirs(cfg[section][name])
               except:
                   print "Could not create output directory"
                   return(2)
    return(0)

#class ProcessFiles(object):
def process_files(myflags, myfiles, indir, outdir, mycollections, mytransform):
    """
    Create object and methods
    """

    #def __init__(self, myflags, myfiles, outdir, mycollections, mytransform):
    #    self.


    # Process files
    i=0
    s = "/"
    for myfile in myfiles:
        xmlfile = s.join((indir,myfile))
        print "Processing",xmlfile, i
        if myfile.endswith(".xml"):
            if myflags['xflg']:
                # while testing
                # xmdfile = s.join((indir,myfile.replace(".xml",".xmd")))
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
            if myflags['xflg']:
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

    #return(0)

def main(argv):
    # This is the main method
    mydif = ['dif', 'gcmd']
    myiso = ['iso']

    # Parse command line arguments
    try:
        opts, args = getopt.getopt(argv,"hc:i:o:s:xp:f",
                ["help","indir","outdir","style","xmd","parent","file"])
    except getopt.GetoptError:
        usage()

    cflg = iflg = oflg = sflg = xflg = pflg = fflg = False
    for opt, arg in opts:
        if opt == ("-h","--help"):
            usage()
        elif opt in ("-c","--cfg"):
            cfgfile = arg
            cflg = True
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

    if not cflg:
        usage()
    #elif not iflg:
    #    usage()
    #elif not oflg:
    #    usage()
    #elif not sflg:
    #    usage()
    myflags = {'cflg':cflg, 'xflg':xflg, 'pflg': pflg}

    # Read config file
    print "Reading", cfgfile
    with open(cfgfile, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    # Check that all relevant directories exists...
    if check_directories(cfg):
        print "Something went wrong creating directories"
        sys.exit(2)

    # Each section is a data centre to harvest
    for section in cfg:
        if section != 'NPI':
            continue
        indir = cfg[section]['dest1']
        outdir = cfg[section]['mmd']
        if cfg[section]['mdkw'] in mydif:
            stylesheet =  '../etc/dif-to-mmd.xsl'
        elif cfg[section]['mdkw'] in myiso:
            stylesheet =  '../etc/iso-to-mmd.xsl'
        mycollections = cfg[section]['collection'].replace(' ','')
        print mycollections.split(',')

        # Define stylesheet and modify accordingly
        parser = ET.XMLParser(remove_blank_text=True)
        try:
            myxslt = ET.parse(stylesheet, parser)
        except ET.XMLSyntaxError,e:
            print e
            sys.exit(1)
        myroot = myxslt.getroot()
        # Find the location where to insert element
        if cfg[section]['mdkw'] in mydif:
            myelement = myxslt.find(".//xsl:element[@name='mmd:collection']",
                    namespaces=myroot.nsmap)
            if myelement is None:
                print "Can't find the requested element, bailing out"
                sys.exit(2)
            myparent = myelement.getparent()
            for coll in mycollections.split(','):
                myelem = ET.Element(ET.QName('http://www.w3.org/1999/XSL/Transform','element'),
                        attrib={'name':'mmd:collection'},
                        nsmap=myroot.nsmap)
                myelem.text = coll
                myparent.insert(myparent.index(myelement)+1,myelem)
        elif cfg[section]['mdkw'] in myiso:
            print "ISO is not supported yet..."
            sys.exit()
        myxslt.write('myfile.xsl',pretty_print=True)
        mytransform = ET.XSLT(myxslt)

        # Find files to process
        try:
            myfiles = os.listdir(indir)
        except OSError as e:
            print e
            sys.exit(1)

        # Process files
        if process_files(myflags, myfiles, indir, outdir, mycollections, mytransform):
            print "Something went wrong processing files"
            sys.exit(2)


if __name__ == '__main__':
    main(sys.argv[1:])
