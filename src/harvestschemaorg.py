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
import gc

def extract_metadata(url, delayedloading):
    """
    Extract metadata from webpage. Using requests_html since NSF ADC is using Javascript to render pages...
    """

    # Use requests_html
    #print('>>>>>', url)
    s = HTMLSession()
    try:
        resp = s.get(url)
    except Exception as e:
        print(resp)
        return(None)

    # wait is before and sleep is after render is done
    # Make this configurable depending on source
    waitingtime = 0
    sleeptime = 0
    if delayedloading:
        print('Prepared for NSF ADC scraping with delayed loading of pages.')
        waitingtime = 15
        sleeptime = 3

    resp.html.render(wait=waitingtime, sleep=sleeptime)
    #print(resp.html.render(wait=15, sleep=3))

    # Extract information from HTML
    metadata = extruct.extract(resp.html.html, base_url=url)
    #metadata_box = extruct.extract("https://data.g-e-m.dk/Datasets?doi=10.17897/KBN7-WP73")
    del resp

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
                #output_file = 'mytestfile.xml' # while testing...
                output_file = sosomd['identifier']
                et = ET.ElementTree(mmd)
                et.write(output_file, pretty_print=True)
                del mmd
                del et
            del mypage
            page += 1
            if page > 0:
                sys.exit()

    return

def traversesite(url, dstdir, delayedloading):
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
            sosomd = extract_metadata(url2read, delayedloading)
        except Exception as e:
            print('Error returned: ',e)
            continue
        mmd = sosomd2mmd(sosomd)
        if mmd == None:
            print('Record is not complete and is skipped...')
            continue
        # Dump MMD file
        #output_file = 'mytestfile.xml' # while testing...
        if 'http' in sosomd['identifier']:
            tmpname = sosomd['identifier'].split('/')[-1]
        else:
            tmpname = sosomd['identifier']
        filename = tmpname.replace('.','-')+'.xml' 
        output_file = dstdir+'/'+filename
        print(output_file)
        et = ET.ElementTree(mmd)
        et.write(output_file, pretty_print=True)
        del sosomd
        del mmd
        del et
        gc.collect()

    return

def sosomd2mmd(sosomd):
    """
    Transforming the JSON-LD from schema.org (ESIP's) to MMD format.
    """

    print('Now in sosomd2mmd...')
    ##print(json.dumps(sosomd,indent=4))

    # Create XML file with namespaces
    ET.register_namespace('mmd',"http://www.met.no/schema/mmd")
    ns_map = {'mmd': "http://www.met.no/schema/mmd"}
             # 'gml': "http://www.opengis.net/gml"}
    
    myroot = ET.Element(ET.QName(ns_map['mmd'], 'mmd'), nsmap=ns_map)

    # Get all keys from JSON for further use
    mykeys = sosomd.keys()
    #print(mykeys)

    # Extract the identifier, assumed to always be present
    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'metadata_identifier'))
    if 'identifier' in mykeys:
        if isinstance(sosomd['identifier'], dict):
            if 'value' in sosomd['identifier'].keys():
                myel.text = sosomd['identifier']['value']
            else:
                print('Not handled yet')
                return None
        else:
            myel.text = sosomd['identifier']
    elif 'url' in mykeys:
        if 'PDCSearch' in sosomd['url']:
            myel.text = "PDC-"+sosomd['url'].split("=",1)[1]
    else:
        print('Cannot retrieve identifier...')
        sys.exit()

    # Get metadata update
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

    # Set metadata status
    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'metadata_status'))
    myel.text = 'Active'

    # Get title, assumed to always be present
    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'title'))
    myel.text = sosomd['name']
    myel.set('lang','en')

    # Get abstract, assumed to always be present
    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'abstract'))
    myel.text = sosomd['description']
    myel.set('lang','en')
    # Get temporal extent, if not present dataset is not handled
    # FIXME need to reformat strings to match mmd
    # Double check, could be that this is instantaneous dataset and not ongoing
    if 'temporalCoverage' in sosomd:
        if '/' in sosomd['temporalCoverage']:
            tempcov = sosomd['temporalCoverage'].split('/')
            myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'temporal_extent'))
            myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'start_date'))
            myel2.text = tempcov[0]
            if tempcov[1]:
                myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'end_date'))
                myel2.text = tempcov[1]
        else:
            myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'temporal_extent'))
            myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'start_date'))
            myel2.text = sosomd['temporalCoverage']
    else:
        print('No temporal specification for dataset, skipping record...')
        return None

    # Geographical extent 
    if 'spatialCoverage' in mykeys:
        geokeys = sosomd['spatialCoverage']['geo'].keys()
        # FIXME for PDC this reverts lat/lon
        if 'box' in sosomd['spatialCoverage']['geo']:
            geobox = sosomd['spatialCoverage']['geo']['box'].split(' ')
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
            # Handle point from PANGEA, could be more thatdoes like this...
            # FIXME check that no bounding boxes are presented this way
            if 'latitude' in geokeys and 'longitude' in geokeys:
                myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'geographical_extent'))
                myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'rectangle'))
                myel3 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'north'))
                myel3.text = str(sosomd['spatialCoverage']['geo']['latitude'])
                myel3 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'south'))
                myel3.text =  str(sosomd['spatialCoverage']['geo']['latitude'])
                myel3 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'west'))
                myel3.text =  str(sosomd['spatialCoverage']['geo']['longitude'])
                myel3 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'east'))
                myel3.text =  str(sosomd['spatialCoverage']['geo']['longitude'])
            else:
                print('Only supporting bounding boxes for now, skipping record')
                return(None)
    # keywords are separated by ','. Not all are using this...
    # failing here now Øystein Godøy, METNO/FOU, 2023-11-02  for pdc
    if 'keywords' in mykeys: 
        mykws = sosomd['keywords'].split(',')
        #mykws = sosomd['keywords']
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

    # Extract variable information
    # FIXME check ontology references later
    # Need to be expanded
    if 'variableMeasured' in mykeys:
        myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'keywords'))
        myel.set('vocabulary','None')
        for el in sosomd['variableMeasured']:
            myelkw = ET.SubElement(myel,ET.QName(ns_map['mmd'],'keyword'))
            myelkw.text = el['name']

    # related_information, assuming primarily landing pages are conveyed
    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'related_information'))
    myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'type'))
    myel2.text = "Dataset landing page"
    myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'resource'))
    myel2.text = sosomd['url']

    # Get personnel involved
    # FIXME not sure how to differentiate roles
    if 'creator' in mykeys:
        #print(sosomd['creator'])
        if isinstance(sosomd['creator'],list):
            for el in sosomd['creator']:
                #print(el)
                myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'personnel'))
                myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'role'))
                myel2.text = el['name']
                if 'email' in el.keys():
                    myel3 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'email'))
                    myel3.text = el['email']
                # sosomd['creator'] type og name
        else:
            myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'personnel'))
            myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'role'))
            myel2.text = sosomd['creator']['name']
            if 'email' in sosomd['creator'].keys():
                myel3 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'email'))
                myel3.text = sosomd['creator']['email']

    # Get data centre
    if 'publisher' in mykeys:
        myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'data_center'))
        myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'data_center_name'))
        myel21 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'short_name'))
        myel21.text = sosomd['publisher']['name']
        myel22 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'long_name'))
        myel22.text = sosomd['publisher']['name']
        myel3 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'data_center_url'))
        myel3.text = sosomd['publisher']['url']

    # Get license
    # FIXME identifier is only tested on PANGAEA so far...
    if 'license' in mykeys:
        myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'use_constraint'))
        myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'resource'))
        myel2.text = sosomd['license']
        myel3 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'identifier'))
        # sosomd['isAccessibleForFree'] true or false check...
        if '/by/' in sosomd['license']:
            print('Assuming a CC license...')
            myidentifier = 'CC-BY'
            myel3.text = myidentifier

    # Get data_access information
    if 'distribution' in mykeys:
        if isinstance(sosomd['distribution'], list):
            for el in sosomd['distribution']:
                if el['@type'] == "DataDownload":
                    if el['encodingFormat'] == "text/tab-separated-values":
                        myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'data_access'))
                        myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'type'))
                        myel2.text = 'HTTP'
                        myel3 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'resource'))
                        myel3.text = el['contentUrl']

        else:
            print('To be handled later...')

    # Get parent/child information
    if 'isPartOf' in mykeys:
        myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'related_dataset'))
        myel.text = sosomd['isPartOf']
        myel.set('relation_type','parent')

    #print(ET.tostring(myroot, pretty_print=True, encoding='unicode'))
    #sys.exit()
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
            'discovery metadata to MMD. Provide the sitemap as input.', epilog="NSF ADC is serving schem.org through Javascript pages that are slow loading. Thus for scraping NSF ADC option d is required.")
    parser.add_argument('starturl', type=str, 
            help='URL to sitemap')
    parser.add_argument('dstdir', type=str, 
            help='Directory where to put MMD files')
    parser.add_argument('-c', '--ccadi-api', dest='ccadiapi', action='store_true')
    parser.add_argument('-d', '--delayed-loading', dest='delayedloading', action='store_true')
    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        sys.exit()
    
    if args.ccadiapi:
        try:
            ccadiapicall(args.starturl, args.dstdir)
        except Exception as e:
            print('Something went wrong:', e)
            sys.exit()
    else:
        try:
            traversesite(args.starturl, args.dstdir, args.delayedloading)
        except Exception as e:
            print('Something went wrong:', e)
            sys.exit()

    sys.exit()
