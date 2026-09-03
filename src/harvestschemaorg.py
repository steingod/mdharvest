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
from datetime import datetime, date, timedelta, timezone
from dateutil.parser import parse
import pandas as pd
#import extruct
import vocab.ControlledVocabulary
import vocab.ResearchInfra
import requests
import time
import lxml.etree as ET
import urllib.request as ul
#from requests_html import HTMLSession
#from w3lib.html import get_base_url
from urllib.parse import urlparse
import json
from bs4 import BeautifulSoup as bs
import json
import datetime as dt
import gc
import re
from shapely.geometry import Polygon

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
        sleeptime = 10

    resp.html.render(wait=waitingtime, sleep=sleeptime)
    #print(resp.html.render(wait=15, sleep=3))

    # Extract information from HTML
    metadata = extruct.extract(resp.html.html, base_url=url)
    #metadata_box = extruct.extract("https://data.g-e-m.dk/Datasets?doi=10.17897/KBN7-WP73")
    del resp

    #print(metadata)
    if len(metadata['json-ld']) == 0 or 'Dataset' not in metadata['json-ld'][0]['@type']:
        raise TypeError('No valid SOSO JSON-LD available')

    s.close()
    del s
    # Beware, this may fail if invalid soso
    return metadata['json-ld'][0]

def check_directories(mydir):
    if not os.path.isdir(mydir):
        try:
            os.makedirs(mydir)
        except:
            print("Could not create output directory")
            return(2)
    return(0)

def pangaeaapicall(url):
    """
    Designed for PANGAEA. This is the fastest solution. All jsonld records are available at the url+?format=metadata_jsonld
    Response headers give the Retry-After information which should be followed in case of status 429 (Too many requests).
    """

    myapi = url+"?format=metadata_jsonld"
    print('>>> my api call: ', myapi)
    mypage = requests.get(myapi)
    print('Response code: ', mypage.status_code)
    if mypage.status_code == 200:
        print('Processing page... ')
        metadata = mypage.json()
    elif mypage.status_code == 429:
        #print('headers',mypage.headers)
        #print('wait')
        time.sleep(int(mypage.headers['Retry-After']))
        mypage = requests.get(myapi)
        print('Response code: ', mypage.status_code)
        metadata = mypage.json()
        #print(metadata)
    else:
        print('endpoint not responding')
        return(None)

    return metadata

def download_metadata_xml(url, dstdir):
    """
    Designed for INTERACT. Download DCAT xml into directory
    """
    try:
        filename = url.split('/')[-1]+'.xml'
        file_path = os.path.join(dstdir, filename)
        response = requests.get(url+'.xml')
        if response.status_code == 200:
            with open(file_path, 'wb') as file:
                file.write(response.content)
            print('Successfully downloaded ', filename)
        else:
            print('Could not download file ', filename)
    except requests.exceptions.RequestException as e:
        print('Error during download: ', e)

    return


def fieldsitesextract(url):
    """
    Designed for SITES data portal.

    """
    try:
        mypage = requests.get(url)
        if mypage.status_code == 200:
            sitefile = mypage.content
            mysoup = bs(sitefile, features="lxml")
            script = mysoup.find("script", type="application/ld+json")
            metadata = json.loads(script.string)
        elif mypage.status_code == 429:
            time.sleep(int(mypage.headers['Retry-After']))
            mypage = requests.get(url)
            sitefile = mypage.content
            mysoup = bs(sitefile, features="lxml")
            script = mysoup.find("script", type="application/ld+json")
            metadata = json.loads(script.string)
        else:
            print('Could not get', url)
    except:
        print('Something went wrong parsing: ', url)

    return metadata

def niozschemaextract(mybatch):
    """
    Designed for NIOZ. They embed all metadata as JSON in the main landing page https://npdc.nl/dataset.
    All datasets are found from the sitemap by adding .json to the dataset/<uuid>
    """
    #print(mybatch)
    for el in mybatch:
        print(el)
        if 'dataset' not in el:
            print('Skipping this url...')
            continue
        print('>>>> ',el)
        print(''.join([el,'.json']))
        mypage = requests.get(''.join([el,'.json']))
        if mypage.status_code != 200:
            print('Something went wrong accessing the file over internet...')
        print(mypage.json())

    return 

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

def traversesite(url, dstdir, delayedloading, lastmodday, collection):
    """
    Traverse the sitemap and extract information
    Works on NSF ADC not on GEM yet as their sitemap is different
    NSF ADC embeds JSON-LD while GEM links. Rewriting URL's for GEM to avopid overhead for reading sitemap and JSON-LD linked files.
    """
    today = date.today()
    if lastmodday:
        toharvest = today - timedelta(days=int(lastmodday))

    #validate provided collection
    collection_lookup = vocab.ControlledVocabulary.CollectionKeywords
    if collection in collection_lookup:
        collection = collection
    else:
        collection = None
    # Read sitemap or sitemapindex
    print('Reading sitemap or similar...')
    mypage = requests.get(url)
    sitefile = mypage.content
    mysoup = bs(sitefile, features="lxml")
    #check if this is a list of sitemaps
    sitemap_tags = mysoup.find_all("sitemap")
    if len(sitemap_tags) > 0:
        print('it is a list of sitemaps')
        list_sitemaps = [i.text for i in mysoup.find_all('loc')]
        news = []
        for sitemap in list_sitemaps:
            mypage = requests.get(sitemap)
            sitefile = mypage.content
            mysoup = bs(sitefile, features="lxml")
            if lastmodday:
                print("Parsing only records newer then: ", toharvest)
                news += [i.text for i in mysoup.find_all('loc') if datetime.strptime(i.find_next_sibling("lastmod").text, "%Y-%m-%d").date() > toharvest]
                #news += [i.text for i in mysoup.find_all('loc') if (lastmod := i.find_next_sibling("lastmod")) and datetime.strptime(lastmod.text, "%Y-%m-%d").date() > toharvest]
            else:
                news += [i.text for i in mysoup.find_all('loc')]
            print(len(news))
    elif 'https://api.g-e-m.dk/api/dataset/harvest' in url:
        print('This is the GEM API...')
        #for gem there is no loc, but directly the list of scripts. Does not support lastmod
        news = [ json.loads(x.string) for x in mysoup.find_all("script", type="application/ld+json")]
    else:
        if lastmodday:
            print("Parsing only records newer then: ", toharvest)
            news = [i.text for i in mysoup.find_all('loc') if datetime.strptime(i.find_next_sibling("lastmod").text, "%Y-%m-%d").date() > toharvest]
            #news = [i.text for i in mysoup.find_all('loc') if (lastmod := i.find_next_sibling("lastmod")) and datetime.strptime(lastmod.text, "%Y-%m-%d").date() > toharvest]
            #news = []
            #for i in mysoup.find_all('loc'):
            #    #print(i.find_next_sibling("lastmod"))
            #    #print(i.find_next_sibling("lastmod").text)
            #    #print(datetime.strptime(i.find_next_sibling("lastmod").text, "%Y-%m-%d"))
            #    #print(datetime.strptime(i.find_next_sibling("lastmod").text, "%Y-%m-%d").date())
            #    if datetime.strptime(i.find_next_sibling("lastmod").text, "%Y-%m-%d").date() > toharvest:
            #        news.append(i.text)
        else:
            news = [i.text for i in mysoup.find_all('loc')]

    del mypage
    del sitefile
    del mysoup

    batchsize = 50
    print('Number of items to be parsed:', len(news))
    for i in range(0, len(news), batchsize):
        batch = news[i:i+batchsize]
        print('Parsing batch of records:', i, i+batchsize)
        for el in batch:
            sosomd = None
            #for non gem we have the url from sitemap
            if isinstance(el,str):
                print(el)
                url2read = el.strip()
                # Rewrite GEM URLs to avoid reading linked files (no obvious and unique linking convention)
                #if "https://data.g-e-m.dk/Datasets" in url2read:
                #    print("Rewriting GEM dataset links")
                #    tmp = url2read.replace("https://data.g-e-m.dk/Datasets","https://data.g-e-m.dk/umbraco/surface/gemsurface/DatasetSchemaOrg")
                #    url2read = tmp
                print('Extracting information from: [%s]' % url2read)
                try:
                    if 'doi.pangaea.de' in url2read:
                        sosomd = pangaeaapicall(url2read)
                    elif 'npdc.nl' in url2read:
                        sosomd = niozschemaextract(batch)
                    elif 'meta.fieldsites.se' in url2read:
                        sosomd = fieldsitesextract(url2read)
                    elif 'dataportal.eu-interact.org/dataset' in url2read:
                        if '/resource/' not in url2read:
                            download_metadata_xml(url2read, dstdir)
                        else:
                            print("This is just a ckan distribution. Skipping")
                    else:
                        #sosomd = extract_metadata(url2read, delayedloading)
                        print('Skip for now')
                except Exception as e:
                    print('Error returned: ',e)
                    continue
            else:
                # for gem we can directly get the schema.org info in the form of dict
                sosomd = el
            if sosomd != None:
                mmd = sosomd2mmd(sosomd, collection)
            else:
                continue
            if mmd == None:
                print('Record is not complete and is skipped...')
                continue
            # Dump MMD file
            #output_file = 'mytestfile.xml' # while testing...
            #PANGAEA does not always have identifiers. If only url is present, data is under review. Record should be skipped.
            #print('sosomd')
            if 'identifier' in sosomd:
                if isinstance(sosomd['identifier'],str):
                    if 'http' in sosomd['identifier']:
                        # TODO fix when 'identifier': 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-arc/20033/7'
                        if 'eml/knb-lter-arc' in sosomd['identifier']:
                            # Harvesting  will not come here for now
                            tmpname = sosomd['identifier'].split('eml')[-1]
                        if 'hdl.handle.net/' in sosomd['identifier']:
                            tmpname = sosomd['identifier'].split('hdl.handle.net/')[-1]
                        else:
                            tmpname = sosomd['identifier'].split('/')[-1]
                    else:
                        tmpname = sosomd['identifier']
                else:
                    #cover soso identifier with PropertyValue, e.g. : "value": "doi:10.5066/F7VX0DMQ",
                    if 'value' in  sosomd['identifier']:
                        tmpname = sosomd['identifier']['value']
                    else:
                        print('identifier cannot be paresed')
                        continue
            elif 'url' in sosomd:
                if 'doi.pangaea.de/' not in sosomd['url']:
                   tmpname = sosomd['url'].split('/')[-1]
                else:
                    print('Pangaea record does not have identifier, only url and it is skipped...')
                    continue
            else:
                print('Record does not have identifier or url and is skipped...')
                continue
            if ':' in tmpname or '.' in tmpname or '/' in tmpname:
                filename = tmpname.replace(':','-').replace('.','-').replace('/','-')+'.xml'
            else:
                filename = tmpname+'.xml'
            print('filename',filename)
            #print(filename)
            output_file = dstdir+'/'+filename
            #print(output_file)
            et = ET.ElementTree(mmd)
            et.write(output_file, pretty_print=True)
            del sosomd
            del mmd
            del et
            del output_file
            del tmpname
            gc.collect()
        del batch
        gc.collect()

    return

def sosomd2mmd(sosomd, collection=None):
    """
    Transforming the JSON-LD from schema.org (ESIP's) to MMD format.
    """

    print('Now in sosomd2mmd...')
    ##print(json.dumps(sosomd,indent=4))

    # Create XML file with namespaces
    ET.register_namespace('mmd',"http://www.met.no/schema/mmd")
    # add gml to be able to save polygons
    ns_map = {'mmd': "http://www.met.no/schema/mmd",
              'gml': "http://www.opengis.net/gml"}

    myroot = ET.Element(ET.QName(ns_map['mmd'], 'mmd'), nsmap=ns_map)

    # Get all keys from JSON for further use
    mykeys = sosomd.keys()
    #print(mykeys)
    if 'Dataset' not in sosomd['@type']:
        print("Not a dataset")
        return None

    # Extract the identifier, assumed to always be present

    # Note on PANGAEA. Common representation of fully published identifiers:
    # "@id":"https://doi.org/10.1594/PANGAEA.846617",
    # "identifier":"https://doi.org/10.1594/PANGAEA.846617",
    # "url":"https://doi.pangaea.de/10.1594/PANGAEA.846617"
    # and for under review/waiting for publication only the url is present:
    # "url":"https://doi.pangaea.de/10.1594/PANGAEA.974493"
    # this url is also used as reference from isPartOf.

    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'metadata_identifier'))
    if 'identifier' in mykeys and sosomd['identifier'] is not None:
        #print(sosomd['identifier'], type(sosomd['identifier']))
        #it could be a schema:PropertyValue (recommended by SOSO). That would be a dict with: @id, @type, propertyID, value and url.
        if isinstance(sosomd['identifier'], dict):
            if 'value' in sosomd['identifier'].keys():
                myel.text = sosomd['identifier']['value']
            else:
                print('Not handled yet')
                return None
        elif isinstance(sosomd['identifier'], list):
            print('Identifier is a list. Not handled yet')
            return None
        else:
            #or it could be a simple string for text or url.
            #print(sosomd['identifier'], type(sosomd['identifier']))
            if 'doi.org/' in sosomd['identifier']:
                myel.text = 'doi:' + sosomd['identifier'].split('doi.org/')[-1]
            elif 'handle.net/' in sosomd['identifier']:
                myel.text = 'hdl:' + sosomd['identifier'].split('hdl.handle.net/')[-1]
            else:
                # TODO fix when 'identifier': 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-arc/20033/7'
                if 'http' in sosomd['identifier'] and 'eml/knb-lter-arc' in sosomd['identifier']:
                    print('identifier is a url. Skip for now')
                    #myel.text = sosomd['identifier'].split('eml')[-1].replace('/','-')
                    return None
                else:
                    myel.text = sosomd['identifier']
    elif 'url' in mykeys:
        #print('url',myel.text)
        if 'PDCSearch' in sosomd['url']:
            myel.text = "PDC-"+sosomd['url'].split("=",1)[1]
        #do not fetch doi from urls in pangaea. They might not be persistent
        elif 'doi.pangaea.de/' in sosomd['url']:
        #   myel.text = 'doi:' + sosomd['url'].split('doi.pangaea.de/')[-1]
            print("Pangaea dataset under review. Only url present. Skipping for now.")
            return None
        else:
            print('Cannot retrieve identifier...')
            return None
    else:
        print('Cannot retrieve identifier...')
        return None

    # Get title, assumed to always be present / NSF has some records without
    if 'name' in sosomd:
        myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'title'))
        myel.text = sosomd['name']
        myel.set('{http://www.w3.org/XML/1998/namespace}lang','en')
    else:
        print('No title available, skipping records')
        return(None)

    # Get abstract, assumed to always be present
    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'abstract'))
    if isinstance(sosomd['description'], dict):
        #for gem
        myel.text = sosomd['description']['@value']
    else:
        myel.text = sosomd['description']
    myel.set('{http://www.w3.org/XML/1998/namespace}lang','en')

    # Set metadata status
    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'metadata_status'))
    myel.text = 'Active'

    # Set dataset production status
    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'dataset_production_status'))
    myel.text = 'Not available'

    # Set collection - TODO remove after testing
    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'collection'))
    myel.text = 'ADC'
    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'collection'))
    myel.text = 'NSDN'

    # Get metadata update
    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'last_metadata_update'))
    myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'update'))
    myel3 = ET.SubElement(myel2, ET.QName(ns_map['mmd'],'datetime'))

    # TODO - handle datetime when datePublished is not provided. This is a mandatory field in mmd, not it will just be
    # empty, i.e. not compliant.
    customnote = False
    if 'datePublished' in sosomd:
        tmpdatetime = datetimeconvert(sosomd['datePublished'], True)
        if tmpdatetime:
            validdate = tmpdatetime['datetime']
            myel3.text = validdate
            customnote = tmpdatetime['note']
        else:
            print("Could not convert datePublished", sosomd['datePublished'])

    ET.SubElement(myel2,ET.QName(ns_map['mmd'],'type')).text = 'Created'
    if customnote is True:
        ET.SubElement(myel2,ET.QName(ns_map['mmd'],'note')).text = 'From original metadata record - Only year is known from source'
    else:
        ET.SubElement(myel2,ET.QName(ns_map['mmd'],'note')).text = 'From original metadata record'

    if 'dateModified' in sosomd:
        customnote = False
        tmpdatetime = datetimeconvert(sosomd['dateModified'], True)
        #print(tmpdatetime)
        if tmpdatetime:
            validdate = tmpdatetime['datetime']
            customnote = tmpdatetime['note']
            myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'update'))
            ET.SubElement(myel2, ET.QName(ns_map['mmd'],'datetime')).text = validdate
            #we do not know what has been modified. Set to Major modification
            ET.SubElement(myel2,ET.QName(ns_map['mmd'],'type')).text = 'Major modification'
            if customnote is True:
                ET.SubElement(myel2,ET.QName(ns_map['mmd'],'note')).text = 'From original metadata record - Only year is known from source'
            else:
                ET.SubElement(myel2,ET.QName(ns_map['mmd'],'note')).text = 'From original metadata record'
        else:
            print("Could not convert dateModified", sosomd['dateModified'])

    # Get temporal extent, if not present dataset is not handled
    # FIXME need to reformat strings to match mmd
    # Double check, could be that this is instantaneous dataset and not ongoing
    # gem is using " - " instead of "/" as separator and it can have empty string
    if 'temporalCoverage' in sosomd and sosomd['temporalCoverage']:
        myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'temporal_extent'))
        myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'start_date'))

        #make [start, end] list
        if '/' in sosomd['temporalCoverage']:
            tempcov = sosomd['temporalCoverage'].split('/')
        elif ' - ' in sosomd['temporalCoverage']:
            tempcov = sosomd['temporalCoverage'].split(' - ')
        else:
            #single date means start=end
            tempcov = [sosomd['temporalCoverage'], sosomd['temporalCoverage']]

        for i, t in enumerate(tempcov):
            if t != '..':
                #check valid datetime
                val = datetimeconvert(t, False)
                if val:
                    tempcov[i] = val['datetime']
                else:
                    print('Cannot parse datetime: skipping record')
                    return None

        if tempcov[0]:
            #print('start', tempcov[0])
            myel2.text = tempcov[0]
        else:
            print("Cannot parse temporal coverage, skipping")
        if tempcov[1] and tempcov[1] != '..':
            #print('end', tempcov[1])
            myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'end_date'))
            myel2.text = tempcov[1]

    else:
        print('No temporal specification for dataset, skipping record...')
        return(None)

    # Set iso_topic_category
    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'iso_topic_category'))
    myel.text = 'Not available'

    # keywords are separated by ','. Not all are using this...
    # failing here now Øystein Godøy, METNO/FOU, 2023-11-02  for pdc
    # This could also be a string ("keywords":"fish monitoring; Sylt Roads; Wadden Sea")
    # or SOSO as text:
    # "keywords": ["ocean acidification", "Dissolved Organic Carbon", "bacterioplankton respiration", "pCO2", "carbon dioxide", "oceans"]
    # or SOSO as DefinedTerm (recommend):
    # "keywords": [
    # {
    #  "@type": "DefinedTerm",
    #  "name": "OCEANS",
    #  "inDefinedTermSet": "https://gcmd.earthdata.nasa.gov/kms/concepts/concept_scheme/sciencekeywords",
    #  "url": "https://gcmd.earthdata.nasa.gov/kms/concept/91697b7d-8f2b-4954-850e-61d5f61c867d", (optional)
    #  "termCode": "91697b7d-8f2b-4954-850e-61d5f61c867d" (optional)
    # },...
    # It is also provided as empty list
    keywords = False
    rimapping = vocab.ResearchInfra.RI
    rilist = []
    # parsing of keywords can be used for adding collections. There is no garantee that parent and children have the
    # same match.
    polarincoll = False
    gcwcoll = False
    yoppcoll = False
    approject = False
    gcwpar = ["CRYOSPHERE", "TERRESTRIAL HYDROSPHERE &gt; SNOW/ICE", "OCEANS &gt; SEA ICE", "DEPTH, ICE/SNOW", "PERMAFROST"]
    if 'keywords' in mykeys:
        if isinstance(sosomd['keywords'],str):
            mykws = sosomd['keywords'].replace(';', ',').split(',')
            keywords = True
        elif isinstance(sosomd['keywords'],list) and len(sosomd['keywords']) > 0:
            mykws = sosomd['keywords']
            keywords = True
        else:
            print('Cannot parse keywords element')
        if keywords:
            for kw in mykws:
                if isinstance(kw,str):
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
                            myel.set('vocabulary','None')
                        myelnone = ET.SubElement(myel,ET.QName(ns_map['mmd'],'keyword'))
                        myelnone.text = kw.strip()
                    #Attempt Polarin RI mapping
                    for k,v in rimapping.items():
                        if kw.strip() in v['kw']:
                            ri = ET.Element(ET.QName(ns_map['mmd'],'related_information'))
                            ri2 = ET.SubElement(ri,ET.QName(ns_map['mmd'],'type'))
                            ri2.text = 'Observation facility'
                            ri3 = ET.SubElement(ri,ET.QName(ns_map['mmd'],'description'))
                            ri3.text = k
                            if v['polarin'] is True:
                                polarincoll = True
                            ri4 = ET.SubElement(ri,ET.QName(ns_map['mmd'],'resource'))
                            ri4.text = v['resource']
                            rilist.append(ri)
                    if kw.upper().strip() in gcwpar:
                        gcwcoll = True
                    if kw.strip() == 'Arctic PASSION':
                        approject = True
                    if kw.strip() == 'YOPP':
                        yoppcoll = True
                elif isinstance(kw,dict):
                    if kw['@type'] == 'DefinedTerm':
                        if 'sciencekeywords' in kw['inDefinedTermSet']:
                            if 'myelgcmd' not in locals():
                                myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'keywords'))
                                myel.set('vocabulary','GCMDSK')
                            myelgcmd = ET.SubElement(myel,ET.QName(ns_map['mmd'],'keyword'))
                            myelgcmd.text = kw['name']
                        if 'vocab.nerc.ac.uk/standard_name' in kw['inDefinedTermSet'] or 'vocab.nerc.ac.uk/collection/P07/' in kw['inDefinedTermSet']:
                            if 'myelcfstdn' not in locals():
                                myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'keywords'))
                                myel.set('vocabulary','CFSTDN')
                            myelcfstdn = ET.SubElement(myel,ET.QName(ns_map['mmd'],'keyword'))
                            myelcfstdn.text = kw['name']
                        if kw['name'].upper() in gcwpar:
                            gcwcoll = True
        else:
            print('Keywords found, but empty')



    # Extract variable information
    # FIXME check ontology references later
    # Need to be expanded
    # schema.org allows the value of variableMeasured to be a simple text string, but SOSO strongly recommends to use the schema:PropertyValue type
    varmeas = False
    if 'variableMeasured' in mykeys:
        if isinstance(sosomd['variableMeasured'],str):
            varmeas = True
        elif isinstance(sosomd['variableMeasured'],list) and len(sosomd['variableMeasured']) > 0:
            varmeas = True
        else:
            print('Cannot parse variableMeasured element')

        if varmeas:
            if isinstance(sosomd['variableMeasured'],list):
                if myroot.find("mmd:keywords/[@vocabulary = 'None']", myroot.nsmap) is not None:
                    myel = myroot.find("mmd:keywords/[@vocabulary = 'None']", myroot.nsmap)
                else:
                    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'keywords'))
                    myel.set('vocabulary','None')
                klist = []
                for el in sosomd['variableMeasured']:
                    if isinstance(el, dict):
                        if el['name'] not in klist:
                            klist.append(el['name'])
                        if el['name'].upper() in gcwpar:
                            gcwcoll = True
                        #it is possible to extract other vocabularies. Check for CF
                        #if 'subjectOf' in el:
                        #    if isinstance(el['subjectOf']['hasDefinedTerm'],list):
                        #        for i in el['subjectOf']['hasDefinedTerm']:
                        #            if isinstance(i,dict) and 'url' in i.keys() or 'url' in i:
                        #                if 'vocab.nerc.ac.uk/collection/P07/' in i['url']:
                        #                    print('standard name')
                        #    else:
                        #        if isinstance(el['subjectOf']['hasDefinedTerm'],dict) and 'url' in el['subjectOf']['hasDefinedTerm'].keys() or 'url' in el['subjectOf']['hasDefinedTerm']:
                        #            if 'vocab.nerc.ac.uk/collection/P07/' in el['subjectOf']['hasDefinedTerm']['url']:
                        #                print('standard name')
                    else:
                        if el not in klist:
                            klist.append(el)
                        if el.upper() in gcwpar:
                            gcwcoll = True
                for k in klist:
                    myelkw = ET.SubElement(myel,ET.QName(ns_map['mmd'],'keyword'))
                    myelkw.text = k
            else:
                if myroot.find("mmd:keywords/[@vocabulary = 'None']", myroot.nsmap) is not None:
                    myel = myroot.find("mmd:keywords/[@vocabulary = 'None']", myroot.nsmap)
                else:
                    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'keywords'))
                    myel.set('vocabulary','None')
                myelkw = ET.SubElement(myel,ET.QName(ns_map['mmd'],'keyword'))
                myelkw.text = sosomd['variableMeasured']['name']
                if sosomd['variableMeasured']['name'].upper() in gcwpar:
                    gcwcoll = True


    #test for PANGAEA to add keywords from title
    if ('keywords' not in mykeys or not keywords) and ('variableMeasured' not in mykeys or not varmeas):
        if 'Documentation of sediment core' in sosomd['name']:
            myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'keywords'))
            myel.set('vocabulary','GCMDSK')
            myelgcmd = ET.SubElement(myel,ET.QName(ns_map['mmd'],'keyword'))
            myelgcmd.text = 'EARTH SCIENCE > PALEOCLIMATE > OCEAN/LAKE RECORDS > SEDIMENT CORE'
        #extraction for gem could be attempted, but is it not consitent
        #elif 'alternateName' in sosomd.keys() and 'data.g-e-m.dk' in sosomd['url']:
        #    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'keywords'))
        #    myel.set('vocabulary','None')
        #    an = sosomd['alternateName'].split(' - ')[0]
        #    myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'keyword'))
        #    myel2.text = an
        else:
            #leave empty keywords - it is mandatory in mmd but still it will not be indexed if empty
            myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'keywords'))
            myel.set('vocabulary','None')
            ET.SubElement(myel,ET.QName(ns_map['mmd'],'keyword'))


    # Geographical extent
    #geoshape/box def: A box is the area enclosed by the rectangle formed by two points. The first point is the lower
    #corner, the second point is the upper corner. A box is expressed as two points separated by a space character.
    # south-west-north-east
    # simple dict: lat/lon, box or polygon
    # "spatialCoverage": {"geo":{"@type":"GeoCoordinates","latitude":67.80472222,"longitude":29.28222222}}
    # "spatialCoverage": {"geo":{"@type":"GeoShape","box":"-54.401117 -37.5679 -54.065333 -36.142567"}}
    # "spatialCoverage": {"geo":{"@type":"GeoShape","polygon": "68.345332 18.967481 68.348875 18.967481 68.348875 18.98384 68.345332 18.98384 68.345332 18.967481"}

    # list of dict:
    # "spatialCoverage": [{..,"geo": {"@type": "GeoCoordinates","latitude": 57.177427,"longitude": 14.80762}, "name": "Feresjön, floating platform"},
    #                     {..,"geo": {"@type": "GeoShape","polygon": "57.182066 ...}, "name": "Feresjön"}]
    # "spatialCoverage": [{..,"geo": {"@type": "GeoCoordinates", "latitude": 68.353921, "longitude": 18.789888}, "name": "Abisko Scientific Research Station, Mast 4.5m Phenocam 01"},
    #                     {..,"geo": {"@type": "GeoCoordinates", "latitude": 68.353729, "longitude": 18.816522}, "name": "Abisko Scientific Research Station"}]
    #
    # NOT COVER YET: used by seanoe with different coordinate order for box S E N W
    # dict with one-item list:
    # "spatialCoverage":{"@type":"Place","geo":[{"@type":"GeoShape","box":"-90 180 90 -180"}]}
    # dict with lists:
    # "spatialCoverage": {"geo": [{"@type":"GeoShape","box":"46.293976 -5.077748 46.293977 -5.077749"},
    #                             {"@type":"GeoShape","box":"46.191084 -4.412852 46.191262 -4.413131"},
    #                             {"@type":"GeoShape","box":"47.485362 -8.648895 47.485365 -8.648903"}]}
    # "spatialCoverage": {'geo': [{'@type': 'GeoCoordinates', 'latitude': 46.335244, 'longitude': -1.38658},
    #                             {'@type': 'GeoCoordinates', 'latitude': 46.058902, 'longitude': -1.16599},
    #                             {'@type': 'GeoCoordinates', 'latitude': 46.026908, 'longitude': -1.099562},
    #                             {'@type': 'GeoCoordinates', 'latitude': 45.953185, 'longitude': -1.218234},...
    if 'spatialCoverage' in mykeys and sosomd['spatialCoverage'] is not None:
        #simple dict
        if isinstance(sosomd['spatialCoverage'], dict):
            geo = sosomd['spatialCoverage'].get('geo')
            if geo:
                if isinstance(geo, list):
                    # we should get the max of all to create the including box probably.
                    # A list here is likely describing the same spatial location, but in different ways.
                    print('List of geo is not supported yet')
                    return None

                geographical_extent = {'north': None, 'south': None, 'east': None, 'west': None}
                pol_string = None
                # FIXME for PDC this reverts lat/lon
                if 'box' in geo:
                    #NSF/ADC has records with 'box': '-180, 45 180, 90' with "W,S E,N" (instead of "S W N E")
                    if ',' in geo['box']:
                        geobox = geo['box'].replace(',', '').split(' ')
                        tmp = [geobox[1], geobox[0], geobox[3], geobox[2]]
                        geobox = tmp
                    else:
                        geobox = geo['box'].split(' ')
                    north = float(geobox[2])
                    south = float(geobox[0])
                    west = float(geobox[1].rstrip(','))
                    east = float(geobox[3].rstrip(','))
                elif 'latitude' in geo and 'longitude' in geo:
                    north = geo['latitude']
                    south = geo['latitude']
                    west = geo['longitude']
                    east = geo['longitude']
                elif 'polygon' in geo and geo['polygon'] is not None:
                    pol_string = geo['polygon']
                    #create bbox from polygon
                    coords = list(map(float, pol_string.split()))
                    poly_coords = [(coords[i], coords[i+1]) for i in range(0, len(coords), 2)]
                    polygon = Polygon(poly_coords)
                    #bbox = (minx, miny, maxx, maxy)
                    bbox = polygon.bounds
                    north = bbox[3]
                    south = bbox[1]
                    west = bbox[0]
                    east = bbox[2]
                else:
                    print('Type of geoinfo not supported. Skipping record.')
                    return None

                geographical_extent = {'north': north, 'south': south, 'east': east, 'west': west}
                if all(value is not None for value in geographical_extent.values()):
                    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'geographic_extent'))
                    myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'rectangle'))
                    myel2.set('srsName','EPSG:4326')

                    myel3 = ET.SubElement(myel2,ET.QName(ns_map['mmd'],'north'))
                    myel3.text = str(geographical_extent['north'])
                    myel3 = ET.SubElement(myel2,ET.QName(ns_map['mmd'],'south'))
                    myel3.text = str(geographical_extent['south'])
                    myel3 = ET.SubElement(myel2,ET.QName(ns_map['mmd'],'west'))
                    myel3.text = str(geographical_extent['west'])
                    myel3 = ET.SubElement(myel2,ET.QName(ns_map['mmd'],'east'))
                    myel3.text = str(geographical_extent['east'])
                    #adding polygon
                    if pol_string:
                        pol = pol_string.split(" ")
                        myel2pol = ET.SubElement(myel,ET.QName(ns_map['mmd'],'polygon'))
                        myel3pol = ET.SubElement(myel2pol,ET.QName(ns_map['gml'],'Polygon'))
                        myel3pol.set('srsName','EPSG:4326')
                        myel3pol.set('id','polygon')
                        myel4pol = ET.SubElement(myel3pol,ET.QName(ns_map['gml'],'exterior'))
                        myel5pol = ET.SubElement(myel4pol,ET.QName(ns_map['gml'],'LinearRing'))
                        for item1, item2 in zip(pol[::2], pol[1::2]):
                            myel6pol = ET.SubElement(myel5pol,ET.QName(ns_map['gml'],'pos'))
                            myel6pol.text = str(item1)+ ' '+str(item2)
                else:
                    print('Could not extract geographical extent. Skipping record.')
                    return None

            else:
                print('No geoinformation. Skipping record.')
                return None

            #GEM is providing station names within the spatialCoverage
            # "spatialCoverage": {"name": "BioBasis Nuuk - Permanent plots",
            #                     "geo": {"@type": "GeoShape","box": "64.13 -51.38 64.13 -51.38","name": "BioBasis Nuuk - Permanent plots"}
            if 'name' in sosomd['spatialCoverage']:
                geoname = sosomd['spatialCoverage']['name']
                # we need to match the full name, otherwise we might get unwanted matches.
                tmpstation = geoname.split('-')
                for st in set(tmpstation):
                    st = st.strip()
                    for k,v in rimapping.items():
                        if st.strip() in v['kw']:
                            ri = ET.Element(ET.QName(ns_map['mmd'],'related_information'))
                            ri2 = ET.SubElement(ri,ET.QName(ns_map['mmd'],'type'))
                            ri2.text = 'Observation facility'
                            ri3 = ET.SubElement(ri,ET.QName(ns_map['mmd'],'description'))
                            ri3.text = k
                            if v['polarin'] is True:
                                polarincoll = True
                            ri4 = ET.SubElement(ri,ET.QName(ns_map['mmd'],'resource'))
                            ri4.text = v['resource']
                            rilist.append(ri)
        else:
            #it could be a list. But it is difficult to know what different elements represent.
            #the logic for sites seems to be that the first lat/lon is the sampling
            # several lat/lon
            # "spatialCoverage": [{..., "geo": {"@type": "GeoCoordinates","latitude": 56.941278,"longitude": 13.64125},"name": "Bolmen, mesocosm"},
            #                     {..., "geo": {"@type": "GeoCoordinates","latitude": 56.941278,"longitude": 13.64125},"name": "Bolmen AquaNet platform"}]
            # lat/lon and polygon
            # "spatialCoverage": [{.., "geo": {"@type": "GeoCoordinates","latitude": 68.331916,"longitude": 19.154204},"name": "Almbergasjön, limnic profile"},
            #                     {.., "geo": {"@type": "GeoShape", "polygon": "68.331658 19.149355 68.33219 ..."},"name": "Almbergasjön" }],
            # name can be None
            # "spatialCoverage": [{..., 'geo': {'@type': 'GeoCoordinates', 'latitude': 64.18201, 'longitude': 19.556576}, 'name': None},
            #                     {..., 'geo': {'@type': 'GeoShape', 'polygon': '64.166664 19.549721 64.17012 ..."},  'name': 'Degerö (C18)'}]
            #
            #Initialize elements.
            lat = []
            lon = []
            pol_string_list = []
            geographical_extent = {'north': None, 'south': None, 'east': None, 'west': None}
            geocoordinates = []
            geoshape = []
            for sc in sosomd['spatialCoverage']:
                if 'geo' in sc:
                    geo = sc['geo']
                    if geo:
                        if 'latitude' in geo and 'longitude' in geo:
                            geocoordinates.append(sc)
                        elif 'polygon' in geo or 'box' in geo:
                            geoshape.append(sc)
                        else:
                            print('Type of geoinfo not supported. Skipping record.')
                            return None
                    else:
                        print('No geoinformation. Skipping record.')
                        return None
                else:
                    print('No geoinformation. Skipping record.')
                    return None
                #fieldsites.se providing station names within the spatialCoverage
                if sc.get('name'):
                    geoname = sc['name']
                    tmpstation = geoname.split(',')
                    for st in set(tmpstation):
                        st = st.strip()
                        for k,v in rimapping.items():
                            if st.strip() in v['kw']:
                                ri = ET.Element(ET.QName(ns_map['mmd'],'related_information'))
                                ri2 = ET.SubElement(ri,ET.QName(ns_map['mmd'],'type'))
                                ri2.text = 'Observation facility'
                                ri3 = ET.SubElement(ri,ET.QName(ns_map['mmd'],'description'))
                                ri3.text = k
                                if v['polarin'] is True:
                                    polarincoll = True
                                ri4 = ET.SubElement(ri,ET.QName(ns_map['mmd'],'resource'))
                                ri4.text = v['resource']
                                rilist.append(ri)
            if geocoordinates:
                #If there is one lat/lon use that
                if len(geocoordinates) == 1:
                    lat.append(geocoordinates[0]['geo']['latitude'])
                    lon.append(geocoordinates[0]['geo']['longitude'])
                else:
                    for gc in geocoordinates:
                        name = gc.get('name')
                        if name and ',' in gc['name']:
                            lat.append(gc['geo']['latitude'])
                            lon.append(gc['geo']['longitude'])
                            break
                        else:
                            lat.append(gc['geo']['latitude'])
                            lon.append(gc['geo']['longitude'])
            elif geoshape:
                for gs in geoshape:
                    if isinstance(gs, list):
                        print('GEO: too many geoinfo')
                        continue
                    if 'box' in gs:
                        geobox = gs['geo']['box'].split(' ')
                        lat.append(geobox[0])
                        lat.append(geobox[2])
                        lon.append(geobox[1])
                        lon.append(geobox[3])
                    elif 'polygon' in gs['geo'] and gs['geo']['polygon'] is not None:
                        #pol = sc['geo']['polygon'].split(" ")
                        pol_string = gs['geo']['polygon']
                        pol_string_list.append(pol_string)
                        coords = list(map(float, pol_string.split()))
                        poly_coords = [(coords[i], coords[i+1]) for i in range(0, len(coords), 2)]
                        polygon = Polygon(poly_coords)
                        #bbox = (minx, miny, maxx, maxy)
                        bbox = polygon.bounds
                        lat.append(bbox[3])
                        lat.append(bbox[1])
                        lon.append(bbox[0])
                        lon.append(bbox[2])
                    else:
                        print('Type of geoshape not supported. Skipping record.')
                        return None
            else:
                print('Type of geoinfo not supported. Skipping record.')
                return None

            if lat and lon:
                north = max(lat)
                south = min(lat)
                west = min(lon)
                east = max(lon)
            else:
                print('Could not find geographical info. Skipping record.')
                return None

            geographical_extent = {'north': north, 'south': south, 'east': east, 'west': west}
            if all(value is not None for value in geographical_extent.values()):
                myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'geographic_extent'))
                myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'rectangle'))
                myel2.set('srsName','EPSG:4326')

                myel3 = ET.SubElement(myel2,ET.QName(ns_map['mmd'],'north'))
                myel3.text = str(geographical_extent['north'])
                myel3 = ET.SubElement(myel2,ET.QName(ns_map['mmd'],'south'))
                myel3.text = str(geographical_extent['south'])
                myel3 = ET.SubElement(myel2,ET.QName(ns_map['mmd'],'west'))
                myel3.text = str(geographical_extent['west'])
                myel3 = ET.SubElement(myel2,ET.QName(ns_map['mmd'],'east'))
                myel3.text = str(geographical_extent['east'])
                # if only one poligon add it.
                if len(pol_string_list) == 1:
                    pol = pol_string_list[0].split(" ")
                    myel2pol = ET.SubElement(myel,ET.QName(ns_map['mmd'],'polygon'))
                    myel3pol = ET.SubElement(myel2pol,ET.QName(ns_map['gml'],'Polygon'))
                    myel3pol.set('srsName','EPSG:4326')
                    myel3pol.set('id','polygon')
                    myel4pol = ET.SubElement(myel3pol,ET.QName(ns_map['gml'],'exterior'))
                    myel5pol = ET.SubElement(myel4pol,ET.QName(ns_map['gml'],'LinearRing'))
                    for item1, item2 in zip(pol[::2], pol[1::2]):
                        myel6pol = ET.SubElement(myel5pol,ET.QName(ns_map['gml'],'pos'))
                        myel6pol.text = str(item1)+ ' '+str(item2)
            else:
                print('Could not extract geographical extent. Skipping record.')
                return None

        #add SIOS collection
        siosbbox = [90.,40.,70.,-20.]
        thisbb = [north,east,south,west]
        sios = False
        if (thisbb[0] < siosbbox[0]) and (thisbb[1] < siosbbox[1]) and (thisbb[0] > siosbbox[2]) and (thisbb[1] > siosbbox[3])\
            or (thisbb[2] > siosbbox[2]) and (thisbb[1] < siosbbox[1]) and (thisbb[2] < siosbbox[0]) and (thisbb[1] > siosbbox[3])\
                or (thisbb[2] > siosbbox[2]) and (thisbb[3] > siosbbox[3]) and (thisbb[2] < siosbbox[0]) and (thisbb[3] < siosbbox[1])\
                    or (thisbb[0] < siosbbox[0]) and (thisbb[3] > siosbbox[3]) and (thisbb[0] > siosbbox[2]) and (thisbb[3] < siosbbox[1]):
            sios = True
        elif (thisbb[0] > siosbbox[0]) and (thisbb[1] > siosbbox[1]) and (thisbb[2] < siosbbox[2]) and (thisbb[3] < siosbbox[3]):
            sios = True
        elif (thisbb[0] > siosbbox[0]) and (thisbb[2] < siosbbox[0]) and (thisbb[1] > siosbbox[1]) and (thisbb[3] < siosbbox[3])\
            or (thisbb[0] > siosbbox[0]) and (thisbb[2] < siosbbox[2]) and (thisbb[1] > siosbbox[1]) and (thisbb[3] < siosbbox[1])\
                or (thisbb[0] > siosbbox[2]) and (thisbb[3] < siosbbox[3]) and (thisbb[1] > siosbbox[1]) and (thisbb[3] < siosbbox[3])\
                    or (thisbb[0] > siosbbox[0]) and (thisbb[2] < siosbbox[2]) and (thisbb[1] > siosbbox[3]) and (thisbb[3] < siosbbox[3]):
            sios = True
        else:
            sios = False
        if sios:
            mycoll = myroot.find("mmd:collection",myroot.nsmap)
            myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'collection'))
            myel.text = 'SIOS'
            mycoll.addnext(myel)

            #add NySMAC collection within for records within the box. Only if within SIOS to speedup.
            nysmacbbox = [79.11850,14.06356,78.72381,10.45540]
            thisbb = [north,east,south,west]
            nysmac = False
            if (thisbb[0] <= nysmacbbox[0]) and (thisbb[1] <= nysmacbbox[1]) and (thisbb[2] >= nysmacbbox[2]) and (thisbb[3] >= nysmacbbox[3]):
                nysmac = True
            if nysmac:
                mycoll = myroot.find("mmd:collection",myroot.nsmap)
                myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'collection'))
                myel.text = 'NySMAC'
                mycoll.addnext(myel)
    else:
        print('No spatial extent, skipping record')
        return None


    # related_information, assuming primarily landing pages are conveyed
    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'related_information'))
    myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'type'))
    myel2.text = "Dataset landing page"
    myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'description'))
    myel2.text = "Dataset landing page"
    myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'resource'))
    myel2.text = sosomd['url']

    # Get personnel involved
    # FIXME not sure how to differentiate roles
    if 'creator' in mykeys:
        if isinstance(sosomd['creator'],list):
            for el in sosomd['creator']:
                myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'personnel'))
                myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'role'))
                myel2.text = 'Investigator'
                myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'name'))
                myel2.text = el['name']
                orcid = None
                ror = None
                if 'identifier' in el.keys():
                    if 'orcid.org' in el['identifier']:
                        orcid = el['identifier'].strip()
                        myel2.set('uri', orcid)
                    if 'ror.org' in el['identifier']:
                        ror = el['identifier'].strip()
                        myel2.set('uri', ror)
                if 'email' in el.keys():
                    myel3 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'email'))
                    myel3.text = el['email']
                else:
                    myel3 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'email'))
                    myel3.text = ''
                if '@type' in el.keys():
                    if el['@type'] == 'Organization':
                        ET.SubElement(myel,ET.QName(ns_map['mmd'],'type')).text = 'Organisation'
                        myel4 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'organisation'))
                        if 'parentOrganization' in el.keys():
                            myel4.text = el['parentOrganization']
                        else:
                            myel4.text = el['name']
                        if ror is not None:
                            myel4.set('uri', ror)
                    else:
                        ET.SubElement(myel,ET.QName(ns_map['mmd'],'type')).text = 'Person'
                        if 'affiliation' in el.keys():
                            if isinstance(el['affiliation'],dict):
                                if '@type' in el['affiliation'] and el['affiliation']['@type'] == 'Organization':
                                    if 'name' in el['affiliation']:
                                        myel4 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'organisation'))
                                        myel4.text = el['affiliation']['name']
                                    if 'identifier' in el['affiliation'] and 'ror.org' in el['affiliation']['identifier']:
                                        ror = el['affiliation']['identifier']
                                        myel4.set('uri', ror)
                            elif isinstance(el['affiliation'],list):
                                #take the first. We do not have multiple org in mmd
                                if '@type' in el['affiliation'][0] and el['affiliation'][0]['@type'] == 'Organization':
                                    if 'name' in el['affiliation'][0]:
                                        myel4 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'organisation'))
                                        myel4.text = el['affiliation'][0]['name']
                                    if 'identifier' in el['affiliation'][0] and 'ror.org' in el['affiliation'][0]['identifier']:
                                        ror = el['affiliation'][0]['identifier']
                                        myel4.set('uri', ror)
                            else:
                                myel4 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'organisation'))
                                myel4.text = ''
                        else:
                            myel4 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'organisation'))
                            myel4.text = ''

                # sosomd['creator'] type og name
        else:
            myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'personnel'))
            myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'role'))
            myel2.text = 'Investigator'
            myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'name'))
            myel2.text = sosomd['creator']['name']
            orcid = None
            ror = None
            if 'identifier' in sosomd['creator'].keys():
                if 'orcid.org' in sosomd['creator']['identifier']:
                    orcid = sosomd['creator']['identifier'].strip()
                    myel2.set('uri', orcid)
                if 'ror.org' in sosomd['creator']['identifier']:
                    ror = sosomd['creator']['identifier'].strip()
                    myel2.set('uri', ror)
            if 'email' in sosomd['creator'].keys():
                myel3 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'email'))
                myel3.text = sosomd['creator']['email']
            else:
                myel3 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'email'))
                myel3.text = ''
            if '@type' in sosomd['creator'].keys():
                if sosomd['creator']['@type'] == 'Organization':
                    ET.SubElement(myel,ET.QName(ns_map['mmd'],'type')).text = 'Organisation'
                    myel4 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'organisation'))
                    if 'parentOrganization' in sosomd['creator'].keys():
                        myel4.text = sosomd['creator']['parentOrganization']
                    else:
                        myel4.text = sosomd['creator']['name']
                    if ror is not None:
                        myel4.set('uri', ror)
                else:
                    ET.SubElement(myel,ET.QName(ns_map['mmd'],'type')).text = 'Person'
                    if 'affiliation' in sosomd['creator'].keys():
                        if isinstance(sosomd['creator']['affiliation'],dict):
                            if '@type' in sosomd['creator']['affiliation'] and sosomd['creator']['affiliation']['@type'] == 'Organization':
                                if 'name' in sosomd['creator']['affiliation']:
                                    myel4 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'organisation'))
                                    myel4.text = sosomd['creator']['affiliation']['name']
                                if 'identifier' in sosomd['creator']['affiliation'] and 'ror.org' in sosomd['creator']['affiliation']['identifier']:
                                    ror = sosomd['creator']['affiliation']['identifier']
                                    myel4.set('uri', ror)
                        elif isinstance(sosomd['creator']['affiliation'],list):
                            #take the first. We do not have multiple org in mmd
                            if '@type' in sosomd['creator']['affiliation'][0] and sosomd['creator']['affiliation'][0]['@type'] == 'Organization':
                                if 'name' in sosomd['creator']['affiliation'][0]:
                                    myel4 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'organisation'))
                                    myel4.text = sosomd['creator']['affiliation'][0]['name']
                                if 'identifier' in sosomd['creator']['affiliation'][0] and 'ror.org' in sosomd['creator']['affiliation'][0]['identifier']:
                                    ror = sosomd['creator']['affiliation'][0]['identifier']
                                    myel4.set('uri', ror)
                        else:
                            myel4 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'organisation'))
                            myel4.text = ''
                    else:
                        myel4 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'organisation'))
                        myel4.text = ''

    #test contributors
    if 'contributor' in mykeys and sosomd['contributor'] is not None:
        contributors = sosomd['contributor']
        # always create a list
        if isinstance(contributors, dict):
            contributors = [contributors]
        for contributor in contributors:
            myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'personnel'))
            myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'role'))
            myel2.text = 'Technical contact'
            myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'name'))
            myel2.text = contributor['name']
            orcid = None
            ror = None
            if 'identifier' in contributor.keys():
                if 'orcid.org' in contributor['identifier']:
                    orcid = contributor['identifier'].strip()
                    myel2.set('uri', orcid)
                if 'ror.org' in contributor['identifier']:
                    ror = contributor['identifier'].strip()
                    myel2.set('uri', ror)
            if 'email' in contributor.keys():
                myel3 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'email'))
                myel3.text = contributor['email']
            else:
                myel3 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'email'))
                myel3.text = ''
            if '@type' in contributor.keys():
                if contributor['@type'] == 'Organization':
                    ET.SubElement(myel,ET.QName(ns_map['mmd'],'type')).text = 'Organisation'
                    myel4 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'organisation'))
                    if 'parentOrganization' in contributor.keys():
                        myel4.text = contributor['parentOrganization']
                    else:
                        myel4.text = contributor['name']
                    if ror is not None:
                        myel4.set('uri', ror)
                else:
                    ET.SubElement(myel,ET.QName(ns_map['mmd'],'type')).text = 'Person'
                    if 'affiliation' in contributor.keys():
                        if isinstance(contributor['affiliation'],dict):
                            if '@type' in contributor['affiliation'] and contributor['affiliation']['@type'] == 'Organization':
                                if 'name' in contributor['affiliation']:
                                    myel4 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'organisation'))
                                    myel4.text = contributor['affiliation']['name']
                                if 'identifier' in contributor['affiliation'] and 'ror.org' in contributor['affiliation']['identifier']:
                                    ror = contributor['affiliation']['identifier']
                                    myel4.set('uri', ror)
                    else:
                        myel4 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'organisation'))
                        myel4.text = ''

    #fieldsites.se providing station names as personnel
    if 'producer' in mykeys and sosomd['producer'] is not None:
        producers = sosomd['producer']
        # always create a list
        if isinstance(producers, dict):
            producers = [producers]
        for producer in producers:
            if 'name' in producer:
                tmpstation = producer['name'].strip()
                for k,v in rimapping.items():
                    if tmpstation.strip() in v['kw']:
                        ri = ET.Element(ET.QName(ns_map['mmd'],'related_information'))
                        ri2 = ET.SubElement(ri,ET.QName(ns_map['mmd'],'type'))
                        ri2.text = 'Observation facility'
                        ri3 = ET.SubElement(ri,ET.QName(ns_map['mmd'],'description'))
                        ri3.text = k
                        if v['polarin'] is True:
                            polarincoll = True
                        ri4 = ET.SubElement(ri,ET.QName(ns_map['mmd'],'resource'))
                        ri4.text = v['resource']
                        rilist.append(ri)

    #fieldsites.se providing station names as personnel
    if 'provider' in mykeys and sosomd['provider'] is not None:
        providers = sosomd['provider']
        # always create a list
        if isinstance(providers, dict):
            providers = [providers]
        for provider in providers:
            if 'name' in provider:
                tmpstation = provider['name'].strip()
                for k,v in rimapping.items():
                    if tmpstation.strip() in v['kw']:
                        ri = ET.Element(ET.QName(ns_map['mmd'],'related_information'))
                        ri2 = ET.SubElement(ri,ET.QName(ns_map['mmd'],'type'))
                        ri2.text = 'Observation facility'
                        ri3 = ET.SubElement(ri,ET.QName(ns_map['mmd'],'description'))
                        ri3.text = k
                        if v['polarin'] is True:
                            polarincoll = True
                        ri4 = ET.SubElement(ri,ET.QName(ns_map['mmd'],'resource'))
                        ri4.text = v['resource']
                        rilist.append(ri)

    # Get data centre
    if 'publisher' in mykeys:
        myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'data_center'))
        myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'data_center_name'))
        myel21 = ET.SubElement(myel2,ET.QName(ns_map['mmd'],'short_name'))
        myel21.text = sosomd['publisher']['name']
        myel22 = ET.SubElement(myel2,ET.QName(ns_map['mmd'],'long_name'))
        if 'disambiguatingDescription' in sosomd['publisher']:
            myel22.text = sosomd['publisher']['disambiguatingDescription']
        elif 'legalName' in sosomd['publisher']:
            myel22.text = sosomd['publisher']['legalName']
        else:
            myel22.text = sosomd['publisher']['name']
        myel3 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'data_center_url'))
        if 'url' in sosomd['publisher']:
            myel3.text = sosomd['publisher']['url']
        else:
            myel3.text = ''


    # Get project. Sometimes it is a list of dicts, othertimes it's a dict
    # In general is should be skipped as there is no confirmed match between project and funding.
    # This is used only for selected projects in order to add MMD collections
    # A separated dictionary could be created and stored in vocab folder to make this more
    # generic and easy to update.
    if 'funding' in mykeys:
        if isinstance(sosomd['funding'],list):
            for i in sosomd['funding']:
                if  i['@type'] == 'MonetaryGrant':
                    if 'identifier' in i:
                        if i['identifier'] == 'AFMOSAiC-1_00':
                            yoppcoll = True
                            myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'project'))
                            myel1 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'short_name'))
                            myel1.text = 'MOSAiC'
                            myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'long_name'))
                            myel2.text = 'Multidisciplinary drifting Observatory for the Study of Arctic Climate'
                        if i['identifier'] == '101003472':
                            approject = True
        else:
            if  sosomd['funding']['@type'] == 'MonetaryGrant' and 'identifier' in sosomd['funding']:
                if sosomd['funding']['identifier'] == 'AFMOSAiC-1_00':
                    yoppcoll = True
                    myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'project'))
                    myel1 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'short_name'))
                    myel1.text = 'MOSAiC'
                    myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'long_name'))
                    myel2.text = 'Multidisciplinary drifting Observatory for the Study of Arctic Climate'
                if sosomd['funding']['identifier'] == '101003472':
                    approject = True

    if approject:
        myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'project'))
        myel1 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'short_name'))
        myel1.text = 'Arctic PASSION'
        myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'long_name'))
        myel2.text = 'Pan-Arctic observing System of Systems: Implementing Observations for societal Needs'

    if yoppcoll:
        mycoll = myroot.find("mmd:collection",myroot.nsmap)
        myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'collection'))
        myel.text = 'YOPP'
        mycoll.addnext(myel)

    # Get license
    # FIXME identifier is only tested on PANGAEA so far...
    if 'license' in mykeys:
        license_lookup = vocab.ControlledVocabulary.UseConstraint
        mylicense = sosomd['license']
        for k, v in license_lookup.items():
            if mylicense in v['exactMatch']:
                licenseid = k
                if 'spdx' in mylicense:
                    licenseurl = mylicense
                else:
                    licenseurl = ''.join(i for i in license_lookup[licenseid]['exactMatch'] if 'spdx' in i)
                break
            else:
                mytext = mylicense
                licenseid = None
                licenseurl = None

        myel = ET.SubElement(myroot, ET.QName(ns_map['mmd'], 'use_constraint'))
        if licenseid:
            ET.SubElement(myel, ET.QName(ns_map['mmd'], 'identifier')).text = licenseid
            ET.SubElement(myel, ET.QName(ns_map['mmd'], 'resource')).text = licenseurl
        else:
            ET.SubElement(myel, ET.QName(ns_map['mmd'], 'license_text')).text = mytext

    #add access_constraint
    if 'conditionsOfAccess' in mykeys:
        if sosomd['conditionsOfAccess'] == 'unrestricted':
            myel = ET.SubElement(myroot, ET.QName(ns_map['mmd'], 'access_constraint'))
            myel.text = 'Open'

    #add language
    if 'inLanguage' in mykeys and sosomd['inLanguage'] is not None:
        language = sosomd['inLanguage']
        valid_lang = {'en': ['English', 'en', 'eng']}
        mmdlang = False
        if isinstance(language, str):
            if language in valid_lang['en']:
                mmdlang = True
        elif isinstance(language, dict):
            if 'name' in language:
                if language['name'] in valid_lang['en']:
                    mmdlang = True
        else:
            print("Could not parse language")

        if mmdlang:
            myel = ET.SubElement(myroot, ET.QName(ns_map['mmd'], 'dataset_language'))
            myel.text = "en"

    # Get data_access information
    if 'distribution' in mykeys:
        distributions = sosomd['distribution']
        storageinfo = []
        if isinstance(sosomd['distribution'], dict):
            distributions = [distributions]
        for el in distributions:
            #encodingFormat is not always present.
            valid_encoding = ['text/tab-separated-values', 'application/zip',
                              'text/csv, application/excel', 'text/csv', 'text/plain',
                              'application/x-netcdf']
            storaged = {}
            if 'contentUrl' in el.keys() and 'format=html' not in el['contentUrl']:
                myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'data_access'))
                myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'type'))
                myel2.text = 'HTTP'
                myel3 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'resource'))
                myel3.text = el['contentUrl']
                ef = ''
                size = ''
                if 'encodingFormat' in el.keys() and el['encodingFormat'] in valid_encoding:
                    ef = el['encodingFormat']
                    if 'sha256' in el.keys():
                        size = el['sha256']
                storaged = {'url': el['contentUrl'], 'encoding': ef, 'size': size}
                storageinfo.append(storaged)
        if len(storageinfo) == 1:
            if storageinfo[0]['encoding']:
                myel4 = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'storage_information'))
                myel5 = ET.SubElement(myel4,ET.QName(ns_map['mmd'],'file_format'))
                myel5.text = storageinfo[0]['encoding']
                if storageinfo[0]['size']:
                    myel6 = ET.SubElement(myel4,ET.QName(ns_map['mmd'],'checksum'))
                    myel6.set('type', 'sha256sum')
                    myel6.text = el['sha256']

    # Get parent/child information
    if 'isPartOf' in mykeys and sosomd['isPartOf'] is not None and 'meta.fieldsites.se/collections' not in sosomd['isPartOf']:
        parentavailable = True
        #parse from url. Skip for now, DOIs are not persistent for data under review
        #if 'doi.pangaea.de/' in sosomd['isPartOf']:
        #    myel.text = 'doi:' + sosomd['isPartOf'].split('doi.pangaea.de/')[-1]
        if 'doi.org/' in sosomd['isPartOf']:
            parentid = 'doi:' + sosomd['isPartOf'].split('doi.org/')[-1]
        else:
            if 'doi.pangaea.de/' in sosomd['isPartOf']:
                parentavailable = False
                print("Pangaea parent dataset under review. Cannot link to parent: DOI is not persistent. Keep standalone record")
            else:
                parentid = sosomd['isPartOf']

        if parentavailable:
            myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'related_dataset'))
            myel.set('relation_type','parent')
            myel.text = parentid

    #data citation DOI
    if 'identifier' in mykeys and sosomd['identifier'] is not None:
        if isinstance(sosomd['identifier'],str):
            if 'doi.org' in sosomd['identifier']:
                myel = ET.SubElement(myroot, ET.QName(ns_map['mmd'], 'dataset_citation'))
                myel2 = ET.SubElement(myel, ET.QName(ns_map['mmd'], 'doi'))
                myel2.text = sosomd['identifier']
            if 'hdl.handle.net' in sosomd['identifier']:
                myel = ET.SubElement(myroot, ET.QName(ns_map['mmd'], 'dataset_citation'))
                myel2 = ET.SubElement(myel, ET.QName(ns_map['mmd'], 'url'))
                myel2.text = sosomd['identifier']
        else:
            if 'url' in sosomd['identifier'] and 'doi.org' in sosomd['identifier']['url']:
                myel = ET.SubElement(myroot, ET.QName(ns_map['mmd'], 'dataset_citation'))
                myel2 = ET.SubElement(myel, ET.QName(ns_map['mmd'], 'doi'))
                myel2.text = sosomd['identifier']['url']

    #check if RI is available
    if len(rilist) > 0:
        #add Observation facilty
        lp = myroot.find("mmd:related_information/[mmd:type = 'Dataset landing page']",myroot.nsmap)
        added = []
        for rimapped in rilist:
            dtext = rimapped.find("mmd:description", myroot.nsmap).text
            if dtext not in added:
                added.append(dtext)
                lp.addnext(rimapped)

    if polarincoll:
        mycoll = myroot.find("mmd:collection",myroot.nsmap)
        myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'collection'))
        myel.text = 'POLARIN'
        mycoll.addnext(myel)

    if gcwcoll:
        mycoll = myroot.find("mmd:collection",myroot.nsmap)
        myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'collection'))
        myel.text = 'GCW'
        mycoll.addnext(myel)

    #add default collection provided
    if collection:
        mycolls = myroot.findall('mmd:collection', myroot.nsmap)
        collection_found = any(oldc.text == collection for oldc in mycolls)
        if not collection_found:
            mycoll = myroot.find('mmd:collection', myroot.nsmap)
            myel = ET.SubElement(myroot,ET.QName(ns_map['mmd'],'collection'))
            myel.text = collection
            mycoll.addnext(myel)

    #print(ET.tostring(myroot, pretty_print=True, encoding='unicode'))
    #sys.exit()
    del mykeys
    del myel
    return myroot

def datetimeconvert(temporalinput, pubtype):
    # try to get a consistent datetime element from datePublished, dateModified and temporalCoverage.
    # it supports reading of:
    # year    = '%Y' -> adding default 01-01T12:00:00Z
    # date    = '%Y-%m-%d'-> adding default T12:00:00Z
    # datetime  = '%Y-%m-%dT%H:%M:%S' -> adding Z
    # datetimem = '%Y-%m-%dT%H:%M:%S.%f'-> adding Z
    # datetimez  = '%Y-%m-%dT%H:%M:%SZ' -> keeping as is
    # datetimemz = '%Y-%m-%dT%H:%M:%S.%fZ' -> trimming milliseconds and add Z
    # datetimeaw = '%Y-%m-%dT%H:%M:%S.%Z' -> converting to UTC and add Z
    # and returns '%Y-%m-%dT%H:%M:%SZ'
    # additionally, for publication types it returns a customnote boolean
    try:
        dtdef = parse(temporalinput, default=datetime(1000, 1, 1, 12, 0, tzinfo=timezone.utc))
        dtutc = dtdef.astimezone(timezone.utc) # transforms to UTC if string has other zone offset
        dtutcs = dtutc.isoformat(timespec="seconds") # trimming to seconds
        dtutcsz = dtutcs.replace("+00:00", "Z") # replace UTC with Z
    except:
        print("Could not parse temporal input")
        return None

    if pubtype:
        if re.match(r'^\d{4}$', temporalinput):
            customnote = True
        else:
            customnote = False
        vdatetime = {'datetime': dtutcsz, 'note': customnote}
    else:
        vdatetime = {'datetime': dtutcsz}

    return vdatetime

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
    parser.add_argument('starturl', type=str, help='URL to sitemap')
    parser.add_argument('dstdir', type=str, help='Directory where to put MMD files')
    parser.add_argument('-c', '--ccadi-api', dest='ccadiapi', action='store_true')
    parser.add_argument('-d', '--delayed-loading', dest='delayedloading', action='store_true')
    parser.add_argument('-l', '--last-modified-day', dest="lastmodday", type=str, help="harvest only the modified in the last X day")
    parser.add_argument('-a', '--collection', dest="collection", type=str, help="collection to be added to all records of one endpoint")
    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        sys.exit()

    check_directories(args.dstdir)

    if args.ccadiapi:
        try:
            ccadiapicall(args.starturl, args.dstdir)
        except Exception as e:
            print('Something went wrong:', e)
            sys.exit()
    else:
        try:
            traversesite(args.starturl, args.dstdir, args.delayedloading, args.lastmodday, args.collection)
        except Exception as e:
            print('Something went wrong:', e)
            sys.exit()

    sys.exit()
