#!/usr/bin/env python3
"""
PURPOSE:

AUTHOR:

UPDATED:

NOTES:
    https://osds.openlinksw.com/
    http://theautomatic.net/2019/01/19/scraping-data-from-javascript-webpage-python/

    Parsing Javaloaded pages doesn't work with requests and urllib.request, using requests_html instead
"""

import os
import sys
import argparse
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
import pandas as pd
import extruct
import requests
import lxml.etree as ET
import urllib.request as ul
from requests_html import HTMLSession
from w3lib.html import get_base_url
from urllib.parse import urlparse
import json
from bs4 import BeautifulSoup as bs
import json
import datetime as dt

def extract_metadata(url):
    """
    Extract metadata from webpage. Using requests_html since NSF ADC is using Javascript to render pages...
    """

    # Use requests_html
    print('>>>>>', url)
    s = HTMLSession()
    try:
        resp = s.get(url)
    except Exception as e:
        print(resp)
        return(None)

    # wait is before and sleep is after render is done
    resp.html.render(wait=15, sleep=3)
    print(resp.html.render(wait=15, sleep=3))

    # Extract information from HTML
    metadata = extruct.extract(resp.html.html, base_url=url)
    #metadata_box = extruct.extract("https://data.g-e-m.dk/Datasets?doi=10.17897/KBN7-WP73")

    #print(metadata)
    if len(metadata['json-ld']) == 0:
        raise TypeError('No valid SOSO JSON-LD available')

    # Beware, this may fail if invalid soso
    return metadata['json-ld'][0]

def ccadiapicall(url, dstdir):
    """
    Designed for CCADI and PDC traversing (same API)
    """
    mystatus = 200
    page = 0

    while mystatus == 200:
        myapi = url+"?page="+str(page)
        print('>>> my api call: ', myapi)
        mypage = requests.get(myapi)
        print('Response code: ', mypage.status_code)
        if mypage.status_code == 200:
            print('Processing page... ', page)
            mydoc = json.loads(mypage.text)
            print(type(mydoc))
            print(len(mydoc))
            #print(json.dumps(mydoc, indent=4))
            print(mydoc.keys())
            for el in mydoc['itemListElement']:
                print('=========')
                mmd = sosomd2mmd(el['item'])
                if mmd == None:
                    print('Record is not complete and is skipped...')
                    continue
                # Dump MMD file
                output_file = 'mytestfile.xml' # while testing...
                #output_file = sosomd['identifier']
                et = ET.ElementTree(mmd)
                et.write(output_file, pretty_print=True)
            page += 1
            if page > 0:
                sys.exit()

def traversesite(url, dstdir):
    """
    Traverse the sitemap and extract information
    Works on NSF ADC not on GEM yet as their sitemap is different
    NSF ADC embeds JSON-LD while GEM links. Rewriting URL's for GEM to avopid overhead for reading sitemap and JSON-LD linked files.
    """

    # Read sitemap
    mypage = requests.get(url)
    sitefile = mypage.content
    mysoup = bs(sitefile, features="lxml")
    news = [i.text for i in mysoup.find_all('loc')]

    for el in news:
        url2read = el.strip()
        # Rewrite GEM URLs to avoid reading linked files (no obvious and unique linking convention)
        if "https://data.g-e-m.dk/Datasets" in url2read:
            print("Rewriting GEM dataset links")
            tmp = url2read.replace("https://data.g-e-m.dk/Datasets","https://data.g-e-m.dk/umbraco/surface/gemsurface/DatasetSchemaOrg")
            url2read = tmp
        print('Extracting information from: [%s]' % url2read)
        try:
            sosomd = extract_metadata(url2read)
        except Exception as e:
            print('Error returned: ',e)
            continue
        #print(sosomd)
        mmd = sosomd2mmd(sosomd)
        if mmd == None:
            print('Record is not complete and is skipped...')
            continue
        # Dump MMD file
        output_file = 'mytestfile.xml' # while testing...
        #output_file = sosomd['identifier']
        et = ET.ElementTree(mmd)
        et.write(output_file, pretty_print=True)

    return

def sosomd2mmd(sosomd):

    print('Now in sosomd2mmd...')
    print(json.dumps(sosomd,indent=4))

    # Create XML file with namespaces
    ET.register_namespace('mmd',"http://www.met.no/schema/mmd")
    ns_map = {'mmd': "http://www.met.no/schema/mmd"}
             # 'gml': "http://www.opengis.net/gml"}
    
    myroot = ET.Element(ET.QName(ns_map['mmd'], 'mmd'), nsmap=ns_map)

    # identifier
    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'metadata_identifier'))
    # Only valid for GEM
    #myel.text = sosomd['identifier']['value']
    mykeys = sosomd.keys()
    print(mykeys)
    print(sosomd['url'])
    if 'identifier' in mykeys:
        if 'value' in sosomd['identifier'].keys():
            myel.text = sosomd['identifier']['value']
        else:
            myel.text = sosomd['identifier']
    elif 'url' in mykeys:
        if 'PDCSearch' in sosomd['url']:
            myel.text = "PDC-"+sosomd['url'].split("=",1)[1]
    else:
        print('Cannot retrieve identifier...')
        sys.exit()

    # metadata update
    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'last_metadata_update'))
    myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'update'))
    myel3 = ET.SubElement(myel2, ET.QName(ns_map['mmd'],'datetime'))
    if 'datePublished' in sosomd:
        myel3.text = sosomd['datePublished']
    else:
        print('datePublished not found in record, leaving empty...')
    ET.SubElement(myel2,ET.QName(ns_map['mmd'],'type')).text = 'Created'
    ET.SubElement(myel2,ET.QName(ns_map['mmd'],'note')).text = 'From original metadata record'
    if 'dateModified' in sosomd:
        myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'update'))
        ET.SubElement(myel2,
                ET.QName(ns_map['mmd'],'datetime')).text = sosomd['dateModified']
        ET.SubElement(myel2,ET.QName(ns_map['mmd'],'type')).text = 'Updated'
        ET.SubElement(myel2,ET.QName(ns_map['mmd'],'note')).text = 'From original metadata record'
    # metadata status
    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'metadata_status'))
    myel.text = 'Active'
    # title
    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'title'))
    myel.text = sosomd['name']
    myel.set('lang','en')
    # abstract
    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'abstract'))
    myel.text = sosomd['description']
    myel.set('lang','en')
    # temporal extent
    # need to reformat strings to match mmd
    if 'temporalCoverage' in sosomd:
        tempcov = sosomd['temporalCoverage'].split('/')
        myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'temporal_extent'))
        myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'start_date'))
        myel2.text = tempcov[0]
        if tempcov[1]:
            myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'end_date'))
            myel2.text = tempcov[1]
    else:
        print('No temporal specification for dataset, skipping...')
        return None
    # geographical extent - not working
    # for PDC this reverts lat/lon
    if 'box' in sosomd['spatialCoverage']['geo']:
        geobox = sosomd['spatialCoverage']['geo']['box'].split(' ')
        #print(geobox)
        #print(type(geobox))
        myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'geographical_extent'))
        myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'rectangle'))
        # Need to be made more robust
        if 'additionalProperty' in sosomd['spatialCoverage']:
            if 'CRS84' in sosomd['spatialCoverage']['additionalProperty'][0]['value']:
                myel.set('srsName','EPSG;4326')
            else:
                myel.set('srsName','EPSG;4326')
        myel3 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'north'))
        myel3.text = geobox[3]
        myel3 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'south'))
        myel3.text =  geobox[1]
        myel3 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'west'))
        myel3.text =  geobox[0].rstrip(',')
        myel3 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'east'))
        myel3.text =  geobox[2].rstrip(',')
    else:
        print('Only supporting bounding boxes for now')
    # keywords are separated by ','.
    # failing here now Øystein Godøy, METNO/FOU, 2023-11-02  for pdc
    #mykws = sosomd['keywords'].split(',')
    mykws = sosomd['keywords']
    for kw in mykws:
        if 'UNKNOWN' in kw.upper():
            continue
        if 'EARTH SCIENCE' in kw.upper():
            if 'myelgcmd' not in locals():
                myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'keywords'))
                myel.set('vocabulary','GCMDSK')
            myelgcmd = ET.SubElement(myel,ET.QName(ns_map['mmd'],'keyword'))
            myelgcmd.text = kw.strip()
        elif 'IN SITU/LABORATORY INSTRUMENTS' in kw.upper():
            continue # while testing
            if 'myplatform' not in locals():
                myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'keywords'))
                myel.set('vocabulary','GCMDSK')
            myelgcmd = ET.SubElement(myel,ET.QName(ns_map['mmd'],'keyword'))
            myelgcmd.text = kw.strip()
        else:
            if 'myelnone' not in locals():
                myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'keywords'))
                myel.set('vocabulary','none')
            myelgcmd = ET.SubElement(myel,ET.QName(ns_map['mmd'],'keyword'))
            myelgcmd.text = kw.strip()
    # related_information
    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'related_information'))
    myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'type'))
    myel2.text = "Dataset landing page"
    myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'resource'))
    myel2.text = sosomd['url']
    print(ET.tostring(myroot, pretty_print=True, encoding='unicode'))
    # personnel
    # sosomd['creator'] type og name
    # data centre
    # sosomd['publisher'] type og name
    # license
    # sosomd['isAccessibleForFree'] true or false check...

    return myroot

"""
Main program below - only for testing, to be integrated in harvester
"""
if __name__ == '__main__':
    """
    some relevant links for testing
    """

    # Parse command line arguments
    parser = argparse.ArgumentParser(
            description='Traverse a sitemap to retrieve schema.org '+
            'discovery metadata to MMD. Provide the sitemap as input.')
    parser.add_argument('starturl', type=str, 
            help='URL to sitemap')
    parser.add_argument('dstdir', type=str, 
            help='Directory where to put MMD files')
    parser.add_argument('-c', '--ccadi-api', dest='ccadiapi', action='store_true')
    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        sys.exit()
    
    print('>>> ', args.ccadiapi)
    if args.ccadiapi:
        try:
            ccadiapicall(args.starturl, args.dstdir)
        except Exception as e:
            print('Something went wrong:', e)
            sys.exit()
    else:
        try:
            traversesite(args.starturl, args.dstdir)
        except Exception as e:
            print('Something went wrong:', e)
            sys.exit()

    sys.exit()
