#!/usr/bin/env python3
#
# Name:
# traverseSLF
#
# Purpose:
# Traverse OPeNDAP server without THREDDS catalogs and extract discovery
# metadata into MMD files for datasets having ACDD elements.
# 
# Author:
# Øystein Godøy, METNO/FOU, 2020-10-06, original version
#
import sys
import os
import argparse
from mmd_utils.nc_to_mmd import Nc_to_mmd
#from mmd_utils import nc_to_mmd
import lxml.etree as ET
from datetime import datetime
import pytz
import uuid
import validators
import urllib.request

# decode the list file, this is basically the address of each file to be processed
def traverse_listfile(mylist, dstdir):
    # Check if list file is local or on a URL
    if validators.url(mylist):
        # Read using urllib
        try:
            myrecords = urllib.request.urlopen(mylist)
        except Exception as e:
            print('Something failed reading remote file', e)
    else:
        # Read local file
        with open(mylist) as f:
            myrecords = f.readline()
        f.close()
    # While testing... FIXME
    for rec in myrecords:
        print(rec)
        outfile = os.path.basename(rec.strip())
        try:
            md = Nc_to_mmd(dstdir, outfile, rec.strip(), False, False, False)
        except Exception as e:
            print('Something failed setting up ACDD extraction', e)
            continue
        try:
            myxml = md.to_mmd()
        except Exception as e:
            print('Something failed when extracting MMD elements', e)
            continue

##    for rec in myrecords:
##        print('Processing:\n\t', rec)
##        #newdstdir = os.path.join(dstdir,mypath)
##        newdstdir = os.path.join(dstdir)
##        # Make more robust...
##        if not os.path.exists(newdstdir):
##            os.makedirs(newdstdir)
##        infile = os.path.basename(ds)
##        print('>>>>',infile)
##        outfile = outfile
##        try:
##            md = nc_to_mmd.Nc_to_mmd(dstdir, outfile, infile, False, False, False)
##        except Exception as e:
##            print('Something failed setting up ACDD extraction', e)
##            continue
##        try:
##            myxml = md.to_mmd()
##        except Exception as e:
##            print('Something failed when extracting MMD elements', e)
##            continue
##
##        if myxml is None:
##            continue
##
##        # Modify the MMD file if necessary
##        # myxml = modifyMMD(myxml)
##
##        # Create new file
##        myxml.write(os.path.join(newdstdir,outfile), pretty_print=True)

def modifyMMD(myxml):
    # Modify the XML generated with information from THREDDS
    #print('Parsing XML')
    #myxml = ET.parse(os.path.join(dstdir,outfile))
    ns_map = {'mmd': "http://www.met.no/schema/mmd",
                  'gml': "http://www.opengis.net/gml"}
    myroot = myxml.getroot()
    # Check and potentially modify identifier
    mynode = myxml.find("./mmd:metadata_identifier", myroot.nsmap)
    #print(mynode.text, ds.url.replace('catalog.xml?dataset=',''))
    # If ID is not a UUID, replace with a newly generated UUID
    if mynode is not None:
        try:
            uuidver = uuid.UUID(mynode.text).version
        except ValueError:
            print("\tNot containing an UUID, replacing identifier.")
            try:
                mynode.text = str(uuid.uuid5(uuid.NAMESPACE_URL,
                    ds.url.replace('catalog.xml?dataset=','')))
            except TypeError as e:
                print(e)
    else:
        try:
            mynode = ET.Element("{http://www.met.no/schema/mmd}metadata_identifier")
            mynode.text = str(uuid.uuid5(uuid.NAMESPACE_URL,
                ds.url.replace('catalog.xml?dataset=','')))
        except TypeError as e:
            print(e)
        try:
            myroot.insert(0, mynode)
        except Exception as e:
            print(e)

    # Add metadata_status
    mynode = ET.Element("{http://www.met.no/schema/mmd}metadata_status")
    mynode.text = 'Active'
    myroot.insert(4, mynode)

    # Add collection
    mynode = ET.Element("{http://www.met.no/schema/mmd}collection")
    mynode.text = 'ADC'
    myroot.insert(6,mynode)
    mynode = ET.Element("{http://www.met.no/schema/mmd}collection")
    mynode.text = 'NSDN'
    myroot.insert(7,mynode)

    # Add iso_topic_category
    # Most datasets belong to this, quick hack for now
    mynode = ET.Element("{http://www.met.no/schema/mmd}iso_topic_category")
    mynode.text = 'Not available'
    myroot.insert(8, mynode)

    # Check and potentially modify activity_type
    mynode = myxml.find("./mmd:activity_type",namespaces=myroot.nsmap)
    if mynode is not None:
        mynode.text = 'Not available'
    else:
        mynode = ET.Element("{http://www.met.no/schema/mmd}activity_type")
        mynode.text = 'Not available'
    myroot.insert(9, mynode)

    # Check and potentially modify operational_status
    mynode = myxml.find("./mmd:operational_status",namespaces=myroot.nsmap)
    if mynode is not None:
        mynode.text = 'Not available'
    else:
        mynode = ET.Element("{http://www.met.no/schema/mmd}operational_status")
        mynode.text = 'Not available'
    myroot.insert(9, mynode)

    # Add related_information
    related_information = ET.Element(
            "{http://www.met.no/schema/mmd}related_information")
    related_information_resource = ET.SubElement(related_information,
            '{http://www.met.no/schema/mmd}resource')
    related_information_resource.text = ds.url.replace('xml','html')
    related_information_type = ET.SubElement(related_information,
            '{http://www.met.no/schema/mmd}type')
    related_information_type.text = 'Dataset landing page'
    related_information_description = ET.SubElement(related_information,
            '{http://www.met.no/schema/mmd}description')
    related_information_description.text = 'Dataset landing page'
    myroot.insert(-1,related_information)

    # Add data_access (not done automatically)
    data_access = ET.Element(
            '{http://www.met.no/schema/mmd}data_access')
    data_access_resource = ET.SubElement(data_access,
            '{http://www.met.no/schema/mmd}resource')
    data_access_resource.text = ds.download_url()
    data_access_type = ET.SubElement(data_access,
            '{http://www.met.no/schema/mmd}type')
    data_access_type.text = 'HTTP'
    data_access_description = ET.SubElement(data_access,
            '{http://www.met.no/schema/mmd}description')
    data_access_description.text = 'Direct download of datafile'
    myroot.insert(-1,data_access)

    data_access = ET.Element(
            '{http://www.met.no/schema/mmd}data_access')
    data_access_resource = ET.SubElement(data_access,
            '{http://www.met.no/schema/mmd}resource')
    data_access_resource.text = ds.opendap_url()
    data_access_type = ET.SubElement(data_access,
            '{http://www.met.no/schema/mmd}type')
    data_access_type.text = 'OPeNDAP'
    data_access_description = ET.SubElement(data_access,
            '{http://www.met.no/schema/mmd}description')
    data_access_description.text = 'OPeNDAP access to dataset'
    myroot.insert(-1,data_access)

    data_access = ET.Element(
            '{http://www.met.no/schema/mmd}data_access')
    data_access_resource = ET.SubElement(data_access,
            '{http://www.met.no/schema/mmd}resource')
    data_access_resource.text = ds.wms_url()
    data_access_type = ET.SubElement(data_access,
            '{http://www.met.no/schema/mmd}type')
    data_access_type.text = 'OGC WMS'
    data_access_description = ET.SubElement(data_access,
            '{http://www.met.no/schema/mmd}description')
    data_access_description.text = 'OGC WMS GetCapabilities URL'
    myroot.insert(-1,data_access)

    # Reference should be removed
    # dataset citation has to be further improved...


if __name__ == '__main__':
    # Parse command line arguments
    parser = argparse.ArgumentParser(
            description='Traverse THREDDS catalogues and extract '+
            'discovery metadata to MMD where ACDD elements are present')
    parser.add_argument('starturl', type=str, 
            help='Local or remote file (URL) containing references to the OPeNDAP records')
    parser.add_argument('dstdir', type=str, 
            help='Directory where to put MMD files')
    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        sys.exit()
    
    try:
        traverse_listfile(args.starturl, args.dstdir)
    except Exception as e:
        print('Something went wrong:', e)
        sys.exit()
    sys.exit()
