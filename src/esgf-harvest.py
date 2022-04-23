#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import os
import sys
import requests
import json
import lxml.etree as ET

def check_directories(mydir):
    if not os.path.isdir(mydir):
        try:
            os.makedirs(mydir)
        except:
            print("Could not create output directory")
            return(2)
    return(0)

def parserecords(destdir, item, myjson):
    records = 0
    for rec in myjson['response']['docs']:
        print('parsing id:',rec['id'])
        mydestdir = '/'.join([destdir,item,rec['experiment_id'][0],rec['member_id'][0],rec['table_id'][0],rec['variable_id'][0]])
        check_directories(mydestdir)
        myname = rec['instance_id'].replace('.','_')+'.xml'
        outputfile = '/'.join([mydestdir,myname])
        r = createMMD(rec)
        print(r['parentrec'])
        # Create file
        r['xml'].write(outputfile,pretty_print=True)
        # Check if parent record is created, create if not
        #print(r['experiment'])
        #print(outputfile)
        #print(outputfile.split(r['experiment']))
        parentfile = "".join([outputfile.split(r['experiment'])[0],r['experiment'],'.xml'])
        print(parentfile)
        if not os.path.exists(parentfile):
            print('A parent file has to be generated and record updated')
            modid = "".join([rec['id'].split(r['experiment'])[0],r['experiment']])
            print(modid)
            searchbase = "https://cera-www.dkrz.de/WDCC/ui/cerasearch/cerarest/exportcmip6?input="
            searchurl = "".join([searchbase,modid])
            #myparxml = createparent("".join([r['parentrec'],"&wt=XML"]))
            mypar = createparent(searchurl)
            # Create file
            mypar['xml'].write(parentfile,pretty_print=True)

        records += 1

    return(records)

def createparent(myurl):

    # Get the parent documents we are using tio create parent MMD records. These are missing temporal and spatial constraints, but still are the best starting point.
    print(myurl)
    try:
        r = requests.get(myurl)
    except Exception as e:
        print('Something failed when checking parent records', e)
        raise
    myjson = r.json()
    print(json.dumps(myjson, indent=4))

    # Create the MMD file based on the JSON retrieved
    ns_map = {'mmd': "http://www.met.no/schema/mmd"}
    root = ET.Element(ET.QName(ns_map['mmd'], 'mmd'), nsmap=ns_map)

    # Add id
    #ET.SubElement(root,ET.QName(ns_map['mmd'],'metadata_identifier')).text = myjson['identifier']['id']
    ET.SubElement(root,ET.QName(ns_map['mmd'],'metadata_identifier')).text = myjson['subjects'][0]['subject']
    #myid = myjson['identifier']['id']
    myid = myjson['subjects'][0]['subject']

    # Add title
    ET.SubElement(root,ET.QName(ns_map['mmd'],'title')).text = myjson['titles'][0]

    # Add abstract
    ET.SubElement(root,ET.QName(ns_map['mmd'],'abstract')).text = myjson['descriptions'][0]['text']

    # Add metadata_status
    ET.SubElement(root,ET.QName(ns_map['mmd'],'metadata_status')).text = 'Active'

    # Add operational_status
    ET.SubElement(root,ET.QName(ns_map['mmd'],'operational_status')).text = 'Scientific'

    # Add last_metadata_update
    myel = ET.SubElement(root,ET.QName(ns_map['mmd'],'last_metadata_update'))
    myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'update'))
    ET.SubElement(myel2,
        ET.QName(ns_map['mmd'],'datetime')).text = myjson['dates'][0]['date']
    ET.SubElement(myel2,ET.QName(ns_map['mmd'],'type')).text = 'Created'
    ET.SubElement(myel2,ET.QName(ns_map['mmd'],'note')).text = 'Automatically generated from ESGF metadata'

    # Add activity type
    ET.SubElement(root,ET.QName(ns_map['mmd'],'activity_type')).text = 'Numerical Simulation'

    # Add iso topic category
    ET.SubElement(root,ET.QName(ns_map['mmd'],'iso_topic_category')).text = 'climatologyMeteorologyAtmosphere'

    # Add license
    myel = ET.SubElement(root,ET.QName(ns_map['mmd'],'use_constraint'))
    ET.SubElement(myel,ET.QName(ns_map['mmd'],'resource')).text = 'https://pcmdi.llnl.gov/CMIP6/TermsOfUse/TermsOfUse6-1.html'
    ET.SubElement(myel,ET.QName(ns_map['mmd'],'identifier')).text = 'CMIP6: Terms of Use'

    # Add bounding box - FIXME
##    myel = ET.SubElement(root,ET.QName(ns_map['mmd'],'geographic_extent'))
##    myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'rectangle'))
##    if 'north_degrees' in myjson and 'east_degrees' in myjson and 'south_degrees' in myjson and 'west_degrees' in myjson:
##        ET.SubElement(myel2, ET.QName(ns_map['mmd'],'north')).text = str(myjson['north_degrees'])
##        ET.SubElement(myel2, ET.QName(ns_map['mmd'],'south')).text = str(myjson['south_degrees']) 
##        ET.SubElement(myel2, ET.QName(ns_map['mmd'],'east')).text = str(myjson['east_degrees']-180.) 
##        ET.SubElement(myel2, ET.QName(ns_map['mmd'],'west')).text = str(myjson['west_degrees']-180.) 

    # Add temporal duration - FIXME
##    myel = ET.SubElement(root,ET.QName(ns_map['mmd'],'temporal_extent'))
##    if 'start_date' in myjson and 'end_date' in myjson:
##        ET.SubElement(myel,ET.QName(ns_map['mmd'],'start_date')).text = myjson['datetime_start']
##        ET.SubElement(myel,ET.QName(ns_map['mmd'],'end_date')).text = myjson['datetime_stop']

    # Add keyword - FIXME
##    myel = ET.SubElement(root,ET.QName(ns_map['mmd'],'keywords'),vocabulary='cf')
##    for kw in myjson['cf_standard_name']:
##        ET.SubElement(myel,ET.QName(ns_map['mmd'],'keyword')).text = kw

    # Add data centre
    myel = ET.SubElement(root,ET.QName(ns_map['mmd'],'data_center'))
    myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'data_center_name'))
    ET.SubElement(myel2, ET.QName(ns_map['mmd'],'long_name')).text = 'Earth System Grid Federation' 
    ET.SubElement(myel2, ET.QName(ns_map['mmd'],'short_name')).text = 'ESGF'
    ET.SubElement(myel,ET.QName(ns_map['mmd'],'data_center_url')).text = 'https://esgf.llnl.gov/' 

    # Add related_url
    baseland = "https://cera-www.dkrz.de/WDCC/ui/cerasearch/cmip6?input="
    myel = ET.SubElement(root,ET.QName(ns_map['mmd'],'related_information'))
    ET.SubElement(myel,ET.QName(ns_map['mmd'],'type')).text = 'Dataset landing page'
    ET.SubElement(myel, ET.QName(ns_map['mmd'],'resource')).text = ''.join([baseland,myid])
    ET.SubElement(myel,ET.QName(ns_map['mmd'],'description')).text = '' 

    # Add personnel
    for item in myjson['creators']:
        myel = ET.SubElement(root,ET.QName(ns_map['mmd'],'personnel'))
        ET.SubElement(myel,ET.QName(ns_map['mmd'],'role')).text = 'Investigator' 
        ET.SubElement(myel,ET.QName(ns_map['mmd'],'name')).text = " ".join([item['givenName'],item['familyName']])
        ET.SubElement(myel,ET.QName(ns_map['mmd'],'email')).text = item['email'] 
        ET.SubElement(myel,ET.QName(ns_map['mmd'],'organisation')).text = item['affiliation'] 
    for item in myjson['contributors']:
        myel = ET.SubElement(root,ET.QName(ns_map['mmd'],'personnel'))
        ET.SubElement(myel,ET.QName(ns_map['mmd'],'role')).text = 'Technical contact' 
        ET.SubElement(myel,ET.QName(ns_map['mmd'],'name')).text = " ".join([item['givenName'],item['familyName']])
        ET.SubElement(myel,ET.QName(ns_map['mmd'],'email')).text = item['email'] 
        ET.SubElement(myel,ET.QName(ns_map['mmd'],'organisation')).text = item['affiliation'] 

    # Add project
    myel = ET.SubElement(root,ET.QName(ns_map['mmd'],'project'))
    ET.SubElement(myel,ET.QName(ns_map['mmd'],'short_name')).text = 'APPLICATE'
    ET.SubElement(myel,ET.QName(ns_map['mmd'],'long_name')).text = 'Advanced Prediction in Polar regions and beyond'

    # Add collection
    ET.SubElement(root,ET.QName(ns_map['mmd'],'collection')).text = 'YOPP'
    ET.SubElement(root,ET.QName(ns_map['mmd'],'collection')).text = 'APPL'
    ET.SubElement(root,ET.QName(ns_map['mmd'],'collection')).text = 'ADC'
    ET.SubElement(root,ET.QName(ns_map['mmd'],'collection')).text = 'NSDN'

    #print(ET.tostring(root, pretty_print=True))
    et = ET.ElementTree(root)
    return({'id': myid, 'xml': et})

def createMMD(myjson):
    # Add warnings if list elements are used...

    #print(json.dumps(myjson,indent=4))
    #sys.exit()
    # Create root element
    ns_map = {'mmd': "http://www.met.no/schema/mmd"}
    root = ET.Element(ET.QName(ns_map['mmd'], 'mmd'), nsmap=ns_map)

    # Add id
    ET.SubElement(root,ET.QName(ns_map['mmd'],'metadata_identifier')).text = myjson['instance_id']

    # Add title
    #ET.SubElement(root,ET.QName(ns_map['mmd'],'title')).text = myjson['experiment_title'][0]
    ET.SubElement(root,ET.QName(ns_map['mmd'],'title')).text = myjson['instance_id']

    # Add abstract
    #print(myjson['member_id'][0])
    myparid = myjson['experiment_id'][0]
    #print(myjson['further_info_url'][0])
    ET.SubElement(root,ET.QName(ns_map['mmd'],'abstract')).text = 'Data extracted from ESGF. This dataset belongs to the Polar Amplification Model Intercomparison Project (PAMIP) experiments and was conducted during the APPLICATE project. Further information on PAMIP is available at https://www.wcrp-climate.org/modelling-wgcm-mip-catalogue/cmip6-endorsed-mips-article/1303-modelling-cmip6-pamip. These data are part of the experiment '+myparid+' ensemble '+myjson['member_id'][0]+' and contains the variable '+myjson['cf_standard_name'][0]+'. Further information is available at '+myjson['further_info_url'][0]+'.'

    # Add last_metadata_update
    myel = ET.SubElement(root,ET.QName(ns_map['mmd'],'last_metadata_update'))
    myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'update'))
    ET.SubElement(myel2,
        ET.QName(ns_map['mmd'],'datetime')).text = myjson['_timestamp']
    ET.SubElement(myel2,ET.QName(ns_map['mmd'],'type')).text = 'Created'
    ET.SubElement(myel2,ET.QName(ns_map['mmd'],'note')).text = 'Automatically generated from ESGF metadata'

    # Add metadata_status
    ET.SubElement(root,ET.QName(ns_map['mmd'],'metadata_status')).text = 'Active'

    # Add activity type
    ET.SubElement(root,ET.QName(ns_map['mmd'],'activity_type')).text = 'Numerical Simulation'

    # Add iso topic category
    ET.SubElement(root,ET.QName(ns_map['mmd'],'iso_topic_category')).text = 'climatologyMeteorologyAtmosphere'

    # Add license
    myel = ET.SubElement(root,ET.QName(ns_map['mmd'],'use_constraint'))
    ET.SubElement(myel,ET.QName(ns_map['mmd'],'resource')).text = 'https://pcmdi.llnl.gov/CMIP6/TermsOfUse/TermsOfUse6-1.html'
    ET.SubElement(myel,ET.QName(ns_map['mmd'],'identifier')).text = 'CMIP6: Terms of Use'

    # Add bounding box
    myel = ET.SubElement(root,ET.QName(ns_map['mmd'],'geographic_extent'))
    myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'rectangle'))
    if 'north_degrees' in myjson and 'east_degrees' in myjson and 'south_degrees' in myjson and 'west_degrees' in myjson:
        ET.SubElement(myel2, ET.QName(ns_map['mmd'],'north')).text = str(myjson['north_degrees'])
        ET.SubElement(myel2, ET.QName(ns_map['mmd'],'south')).text = str(myjson['south_degrees']) 
        ET.SubElement(myel2, ET.QName(ns_map['mmd'],'east')).text = str(myjson['east_degrees']-180.) 
        ET.SubElement(myel2, ET.QName(ns_map['mmd'],'west')).text = str(myjson['west_degrees']-180.) 

    # Add temporal duration
    myel = ET.SubElement(root,ET.QName(ns_map['mmd'],'temporal_extent'))
    if 'start_date' in myjson and 'end_date' in myjson:
        ET.SubElement(myel,ET.QName(ns_map['mmd'],'start_date')).text = myjson['datetime_start']
        ET.SubElement(myel,ET.QName(ns_map['mmd'],'end_date')).text = myjson['datetime_stop']

    # Add keyword FIXME
    myel = ET.SubElement(root,ET.QName(ns_map['mmd'],'keywords'),vocabulary='cf')
    for kw in myjson['cf_standard_name']:
        ET.SubElement(myel,ET.QName(ns_map['mmd'],'keyword')).text = kw

    # Add data centre
    myel = ET.SubElement(root,ET.QName(ns_map['mmd'],'data_center'))
    myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'data_center_name'))
    ET.SubElement(myel2, ET.QName(ns_map['mmd'],'long_name')).text = 'Earth System Grid Federation' 
    ET.SubElement(myel2, ET.QName(ns_map['mmd'],'short_name')).text = 'ESGF'
    ET.SubElement(myel,ET.QName(ns_map['mmd'],'data_center_url')).text = 'https://esgf.llnl.gov/' 

    # Add related_url
    myel = ET.SubElement(root,ET.QName(ns_map['mmd'],'related_information'))
    ET.SubElement(myel,ET.QName(ns_map['mmd'],'type')).text = 'Dataset landing page'
    ET.SubElement(myel, ET.QName(ns_map['mmd'],'resource')).text = 'https://hdl.handle.net/'+myjson['pid'][0]
    ET.SubElement(myel,ET.QName(ns_map['mmd'],'description')).text = '' 

    # Add personnel
    myel = ET.SubElement(root,ET.QName(ns_map['mmd'],'personnel'))
    ET.SubElement(myel,ET.QName(ns_map['mmd'],'role')).text = 'Not available' 
    ET.SubElement(myel,ET.QName(ns_map['mmd'],'name')).text = 'Not available' 
    ET.SubElement(myel,ET.QName(ns_map['mmd'],'organisation')).text =  myjson['institution_id'][0]

    # Add project
    myel = ET.SubElement(root,ET.QName(ns_map['mmd'],'project'))
    ET.SubElement(myel,ET.QName(ns_map['mmd'],'short_name')).text = 'APPLICATE'
    ET.SubElement(myel,ET.QName(ns_map['mmd'],'long_name')).text = 'Advanced Prediction in Polar regions and beyond'

    # Add collection
    ET.SubElement(root,ET.QName(ns_map['mmd'],'collection')).text = 'YOPP'
    ET.SubElement(root,ET.QName(ns_map['mmd'],'collection')).text = 'APPL'
    ET.SubElement(root,ET.QName(ns_map['mmd'],'collection')).text = 'ADC'
    ET.SubElement(root,ET.QName(ns_map['mmd'],'collection')).text = 'NSDN'

    # Add related_dataset
    #print('>>>>', "".join([myjson['instance_id'].split(myparid)[0],myparid]))
    myel = ET.SubElement(root,ET.QName(ns_map['mmd'],'related_dataset'), relation_type='parent').text = "".join([myjson['instance_id'].split(myparid)[0],myparid])

    #print(ET.tostring(root, pretty_print=True))
    et = ET.ElementTree(root)

    # Check content for parent record
    if 'xlink' in myjson:
        mytmpstr = myjson['xlink']
        for item in mytmpstr:
            if 'citation' in item:
                myparent = "".join([item.split(myparid)[0],myparid])

    return({'xml':et, 'parentrec': myparent,'experiment': myparid})

# Harvest ESGF metadata using the RESTful interface of ESGF. This software is
# not intended for routine operation, but specific downloads. Initially
# developed for APPLICATE and YOPP.

# Elements to query include project and source_id. project should be CMIP6,
# activity_drs, PAMIP6, source_id should include AWI-CM-1-1-MR, NorESM???, ???.
# DRS in this context is Data Reference Syntax. details are provided in
# https://esgf.github.io/esg-search/Climate_Model_Metadata.html

# Remember to use replica=False&latest=True
# format=application%2Fsolr%2Bjson
# limit=10000 in order to speed up data harvest

# Move to configuration file at some point

#baseurl = "https://esgf-node.llnl.gov/esg-search/search?"
baseurl = "http://esgf-data.dkrz.de/esg-search/search?"
projects = ['CMIP6']
activity = ['PAMIP']
sources = ['AWI-CM-1-1-MR','NorESM2-LM','CNRM-CM6-1','HadGEM3-GC31-MM']
mylimit = 5000
destdir = './mmd'

# Check that output directory exists
check_directories(destdir)

searchbase = baseurl+"replica=False&latest=True&format=application%2Fsolr%2Bjson&limit="+str(mylimit)+'&project='+projects[0]+'&activity_drs='+activity[0]

for item in sources:
    print('Collecting information from:', item)
    check_directories('/'.join([destdir,item]))
    records = 0
    page = 0
    numrec = 0
    recproc = 0
    numret = 0
    numfound = 0
    myrequest = '&source_id='+item
    searchrequest = searchbase+myrequest
    print('>>>', searchrequest)
    r = requests.get(searchrequest)
    myjson = r.json()
    numfound = myjson['response']['numFound']
    numrec = parserecords(destdir, item, myjson)
    recproc += numrec
    print('Started on record:', myjson['response']['start'])
    while recproc < numfound:
        myrequest = '&source_id='+item+'&offset='+str(recproc)
        searchrequest = searchbase+myrequest
        print('>>>', searchrequest)
        r = requests.get(searchrequest)
        myjson = r.json()
        #print(myjson['responseHeader'])
        numret = myjson['responseHeader']['params']['rows']
        print('Started on record:', myjson['response']['start'])
        numrec = parserecords(destdir, item, myjson)
        recproc += numrec
