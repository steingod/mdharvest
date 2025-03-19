#!/usr/bin/env python3
#
# Name:
# traverseListfile
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
#from mmd_utils.nc_to_mmd import Nc_to_mmd
from mdh_modules.nc_to_mmd import Nc_to_mmd
#from mmd_utils import nc_to_mmd
import lxml.etree as ET
from datetime import datetime
import vocab.ControlledVocabulary
import vocab.CFGCMD
import pytz
import uuid
import validators
import urllib.request
import re
import string

# decode the list file, this is basically the address of each file to be processed
def traverse_listfile(mylist, dstdir, parse_services=False):
    # Check if list file is local or on a URL
    myrecords = [] 
    if validators.url(mylist):
        # Read using urllib
        try:
            myrecords = urllib.request.urlopen(mylist)
        except Exception as e:
            print('Something failed reading remote file', e)
    else:
        # Read local file
        with open(mylist) as f:
            for line in f:
                myrecords.append(line.rstrip())
        f.close()
    mysuffix = '.das'
    for rec in myrecords:
        print('Processing dataset:\n\t',rec)
        if rec.endswith(mysuffix):
            newrec = os.path.splitext(rec)[0].replace(b'output/',b'')
        else:
            newrec = rec
        outfile = os.path.splitext(os.path.basename(newrec))[0]+'.xml'
        try:
            md = Nc_to_mmd(dstdir, outfile, str(newrec), vocab, parse_services, False, False)
        except Exception as e:
            print('Something failed setting up ACDD extraction', e)
            continue
        try:
            myxml = md.to_mmd()
        except Exception as e:
            print('Something failed when extracting MMD elements', e)
            continue

        if myxml is None:
            continue

        # Modify the MMD file if necessary
        myxml = modifyMMD(myxml, False, 'NSDN,SIOS,SIOSCD', False)

        # Create new file
        print('Creating ', outfile)
        myxml.write(os.path.join(dstdir,outfile), pretty_print=True)

def modifyMMD(myxml, checkId=False, collections=None, thredds=False):
    # Modify the XML generated with information from THREDDS
    #print('Parsing XML')
    #myxml = ET.parse(os.path.join(dstdir,outfile))
    ns_map = {'mmd': "http://www.met.no/schema/mmd",
                  'gml': "http://www.opengis.net/gml"}
    myroot = myxml.getroot()
    # Check and potentially modify identifier
    if checkId:
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
    mynode = myxml.find("./mmd:metadata_status", myroot.nsmap)
    if mynode is None:
        mynode = ET.Element("{http://www.met.no/schema/mmd}metadata_status")
        mynode.text = 'Active'
        myroot.insert(4, mynode)

    # Add collections, ADC to all and additional if needed
    # More checking needed
    mynode = myxml.find("./mmd:collection", myroot.nsmap)
    if mynode is None:
        mynode = ET.Element("{http://www.met.no/schema/mmd}collection")
        mynode.text = 'ADC'
        myroot.insert(3,mynode)
    if collections:
        for el in collections.split(','):
            mynode = ET.Element("{http://www.met.no/schema/mmd}collection")
            mynode.text = el.strip()
            myroot.insert(3,mynode)

    # Add iso_topic_category
    # Most datasets belong to this, quick hack for now
    mynode = ET.Element("{http://www.met.no/schema/mmd}iso_topic_category")
    mynode.text = 'Not available'
    myroot.insert(8, mynode)

    # Check and potentially modify activity_type
    mynode = myxml.find("./mmd:activity_type",namespaces=myroot.nsmap)
    if mynode is None:
        mynode = ET.Element("{http://www.met.no/schema/mmd}activity_type")
        mynode.text = 'Not available'
    myroot.insert(9, mynode)

    # Check and potentially modify operational_status
    mynode = myxml.find("./mmd:operational_status",namespaces=myroot.nsmap)
    if mynode is None:
        mynode = ET.Element("{http://www.met.no/schema/mmd}operational_status")
        mynode.text = 'Not available'
    myroot.insert(9, mynode)

    # Check and potentially modify license
#    mynode = myxml.find("./mmd:use_constraing",namespaces=myroot.nsmap)
#    if mynode is not None:
#        mynode.text = 'Not available'

    if thredds:
        metadata_identifier = myxml.find("./mmd:metadata_identifier", namespaces=myroot.nsmap)
        # Add related_information
        related_information = ET.Element(
                "{http://www.met.no/schema/mmd}related_information")
        related_information_type = ET.SubElement(related_information,
                '{http://www.met.no/schema/mmd}type')
        related_information_type.text = 'Dataset landing page'
        related_information_description = ET.SubElement(related_information,
                '{http://www.met.no/schema/mmd}description')
        related_information_description.text = 'Dataset landing page'
        related_information_resource = ET.SubElement(related_information,
                '{http://www.met.no/schema/mmd}resource')
        if metadata_identifier is not None and 'no.met.adc' in metadata_identifier:
            related_information_resource.text = 'https://adc.met.no/dataset/' + metadata_identifier.split('no.met.adc:')[-1]
        else:
            related_information_resource.text = ds.url.replace('.xml','.html')
        myroot.insert(-1,related_information)

        # also add related_information Data server landing page
        related_information = ET.Element(
                '{http://www.met.no/schema/mmd}related_information')
        related_information_type = ET.SubElement(related_information,
                '{http://www.met.no/schema/mmd}type')
        related_information_type.text = 'Data server landing page'
        related_information_description = ET.SubElement(related_information,
                '{http://www.met.no/schema/mmd}description')
        related_information_description.text = 'Access to THREDDS catalogue landing page'
        related_information_resource = ET.SubElement(related_information,
                '{http://www.met.no/schema/mmd}resource')
        related_information_resource.text = ds.url.replace('.xml','.html')
        myroot.insert(-1,related_information)

        # Add data_access (not done automatically)
        data_access = ET.Element(
                '{http://www.met.no/schema/mmd}data_access')
        data_access_type = ET.SubElement(data_access,
                '{http://www.met.no/schema/mmd}type')
        data_access_type.text = 'HTTP'
        data_access_description = ET.SubElement(data_access,
                '{http://www.met.no/schema/mmd}description')
        data_access_description.text = 'Direct download of datafile'
        data_access_resource = ET.SubElement(data_access,
                '{http://www.met.no/schema/mmd}resource')
        data_access_resource.text = ds.download_url()
        myroot.insert(-1,data_access)

        data_access = ET.Element(
                '{http://www.met.no/schema/mmd}data_access')
        data_access_type = ET.SubElement(data_access,
                '{http://www.met.no/schema/mmd}type')
        data_access_type.text = 'OPeNDAP'
        data_access_description = ET.SubElement(data_access,
                '{http://www.met.no/schema/mmd}description')
        data_access_description.text = 'OPeNDAP access to dataset'
        data_access_resource = ET.SubElement(data_access,
                '{http://www.met.no/schema/mmd}resource')
        data_access_resource.text = ds.opendap_url()
        myroot.insert(-1,data_access)

        data_access = ET.Element(
                '{http://www.met.no/schema/mmd}data_access')
        data_access_type = ET.SubElement(data_access,
                '{http://www.met.no/schema/mmd}type')
        data_access_type.text = 'OGC WMS'
        data_access_description = ET.SubElement(data_access,
                '{http://www.met.no/schema/mmd}description')
        data_access_description.text = 'OGC WMS GetCapabilities URL'
        data_access_resource = ET.SubElement(data_access,
                '{http://www.met.no/schema/mmd}resource')
        data_access_resource.text = ds.wms_url()
        myroot.insert(-1,data_access)

    return(myxml)


if __name__ == '__main__':
    # Parse command line arguments
    parser = argparse.ArgumentParser(
            description='Traverse THREDDS catalogues and extract '+
            'discovery metadata to MMD where ACDD elements are present')
    parser.add_argument('starturl', type=str, 
            help='Local or remote file (URL) containing references to the OPeNDAP records')
    parser.add_argument('dstdir', type=str, 
            help='Directory where to put MMD files')
    parser.add_argument('-p','--parse_services', action='store_true', help='Parse services from source (hack enabled for IGPAS data).')
    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        sys.exit()
    
    try:
        traverse_listfile(args.starturl, args.dstdir, args.parse_services)
    except Exception as e:
        print('Something went wrong:', e)
        sys.exit()
    sys.exit()
