#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
PURPOSE:
    This is an reimplmentation of the Perl script filter-dif-records used
    to subset the harvested discovery metadata records. It relates only to
    MMD files and can add collection elements to existing files or write
    files to a new repository. Filtering is done on the basis of
    geographical location and/or parameter content as defined by GCMD DIF
    keywords.

AUTHOR:
    Øystein Godøy, METNO/FOU, 2018-03-27 

UPDATED:
    Øystein Godøy, METNO/FOU, 2018-12-12:
        Added more filtering of time specifications.
    Øystein Godøy, METNO/FOU, 2018-12-11
        Added filtering for NORMAP
        Added check of time strings (temporary fix as SolR ingestion is
        wrong)
    Øystein Godøy, METNO/FOU, 2018-12-08 
        Changed name, plus updated
    Øystein Godøy, METNO/FOU, 2018-06-23 
        Added parameter match
    Øystein Godøy, METNO/FOU, 2021-02-18 
        Cleaning of code, logging implemented and errors corrected.

NOTES:
    - Using the same configuration as harvest and transformation
    - Once working for bounding box, create functions for specific
      purposes...
    - Only valid in the Northern hemisphere as of now.
    - Add option to only process new files
    - Add option to add collection from project names (e.g. NERSC Normap)

"""

import sys
import os
import argparse
import lxml.etree as ET
import codecs
import re
import yaml
import datetime
from dateutil.parser import parse
import logging
from logging.handlers import TimedRotatingFileHandler
from mdh_modules.harvest_metadata import initialise_logger

def parse_arguments():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("-c","--config",dest="cfgfile", help="Configuration file containing endpoints to harvest", required=True)
    parser.add_argument("-l","--logfile",dest="logfile", help="Log file", required=True)
    parser.add_argument("-o","--collection",dest="collection", help="Collection tag to use", required=False)
    parser.add_argument("-p","--parameters",dest="parameters", help="Comma separated list of parameters to filter on.", required=False)
    parser.add_argument("-b","--bounding",dest="bounding", help="Comma separated bounding box (N,E,S,W) to filter on.", required=False)
    parser.add_argument("-a","--aen",help="Checks project affiliation and adds Nansen Legacy collection", action='store_true')
    parser.add_argument("-y","--cryoclim",help="Checks project affiliation and adds CryoClim (CC) collection", action='store_true')
    parser.add_argument("-g","--gcw",help="Checks parameters (GCMD) for cryosphere and adds GCW collection", action='store_true')
    parser.add_argument("-s","--sios",help="Checks bounding box and adds SIOS collection.", action='store_true')
    parser.add_argument("-i","--infranor",help="Checks project affiliation and adds InfraNOR collection.", action='store_true')
    parser.add_argument("-n","--nmap",help="Checks project affiliation and adds NMAP collection.", action='store_true')
    parser.add_argument("-m","--nysmac",help="Checks bounding box for NySMAC.", action='store_true')
    parser.add_argument("-t","--tone",help="Checks bounding box for TONE.", action='store_true')
    parser.add_argument('-r','--sources',dest='sources',help='Comma separated list of sources (in config) to harvest',required=False)

    # Options a, g, i, n and s cannot be used simultaneously

    args = parser.parse_args()

    if args.cfgfile is None:
        parser.print_help()
        parser.exit()

    return args

class LocalCheckMMD():
    def __init__(self, logname, section, mmd_file, bounding, parameters, mycollection,
            project):
        self.logger = logging.getLogger('.'.join([logname,'LocalCheckMMD']))
        self.logger.info('Creating an instance of LocalCheckMMD')
        self.section = section
        self.mmd_file = mmd_file
        self.bbox = bounding
        self.params = parameters
        self.coll = mycollection
        self.project = project

    def check_project(self,elements,root):
        projmatch = False

        if isinstance(self.project,list):
            for proj in self.project:
                self.logger('Project %s',proj)
                if any(proj in mystring.text for mystring in elements):
                    projmatch = True
        else:
            for el in elements:
                if self.project in el.text:
                    projmatch = True

        if projmatch:
            return True
        else:
            return False


    def check_params(self,elements,root):

        parmatch = False

        for par in self.params:
            for mystring in elements:
                if par in mystring.text.upper():
                    parmatch = True

        if parmatch:
            return True
        else:
            return False

    def check_bounding_box(self,elements,root):
        print(">>>>>> Now in checking bounding box....")
        #print("####",elements)
        if len(elements) > 1:
            self.logger.warning("Found more than one element, not handling this now...")
            return False
        print(ET.tostring(elements[0],pretty_print=True))
        # Decode bounding box from XML
        thisbb = []
        for el in elements:
            if el.find('mmd:north',namespaces=root.nsmap).text is None:
                self.logger.warn('mmd:north is empty')
                return(False)
            else:
                myvalue = el.find('mmd:north',namespaces=root.nsmap).text.strip()
                if len(myvalue)>0:
                    northernmost = float(myvalue)
                    thisbb.append(northernmost)
            if el.find('mmd:east',namespaces=root.nsmap).text is None:
                self.logger.warn('mmd:east is empty')
                return(False)
            else:
                myvalue = el.find('mmd:east',namespaces=root.nsmap).text.strip()
                if len(myvalue)>0:
                    easternmost = float(myvalue)
                    thisbb.append(easternmost)
            if el.find('mmd:south',namespaces=root.nsmap).text is None:
                self.logger.warn('mmd:south is empty')
                return(False)
            else:
                myvalue = el.find('mmd:south',namespaces=root.nsmap).text.strip()
                if len(myvalue)>0:
                    southernmost = float(myvalue)
                    thisbb.append(southernmost)
            if el.find('mmd:west',namespaces=root.nsmap).text is None:
                self.logger.warn('mmd:west is empty')
                return(False)
            else:
                myvalue = el.find('mmd:west',namespaces=root.nsmap).text.strip()
                if len(myvalue)>0:
                    westernmost = float(myvalue)
                    thisbb.append(westernmost)

        #### As the data to be flagged should only intersect the bounding box in question, the original code is altered as it demands that the boundary box
        #### should be entirely within the referance boundary box in question.
        # Check bounding box
        # Lists are ordered N, E, S, W

        ### Original - all data within the reference bounding box
        '''
        self.logger.info("This bounding box: %s",thisbb)
        self.logger.info("Reference bounding box: %s",self.bbox)
        if len(thisbb)<4:
            return(False)
        latmatch = False
        lonmatch = False
        if (thisbb[0] < self.bbox[0]) and (thisbb[2] > self.bbox[2]):
            latmatch = True
            self.logger.info("Latitude match")
        if (thisbb[1] < self.bbox[1]) and (thisbb[3] > self.bbox[3]):
            lonmatch = True
            self.logger.info("Longitude match")

        if (latmatch and lonmatch):
            return True
        else:
            return False
        #'''

        # Check bounding box
        # Lists are ordered N, E, S, W

        self.logger.info("This bounding box: %s",thisbb)
        self.logger.info("Reference bounding box: %s",self.bbox)
        if len(thisbb)<4:
            return(False)
        intercept = False

        ###### For bbox to intersect the reference bbox at least one of the following three cases must be true: #########

        ### At least one corner of thisbb (boundary_box) within self.bbox (reference). Only demand one corner must be within, two or four corners within is thereby also covered.
        # Within the opposing sides first, then within the sides of self.bbox which bb might cut across.
        # North-East corner of bb within reference box: # (N < N_max and E < E_max) and (N > S_min and E > W_min)
        # South-East corner of bb within reference box: # (S > S_min and E < E_max) and (S < N_max and E > W_min)
        # South-West corner of bb within reference box: # (S > S_min and W > W_min) and (S < N_max and W < E_max)
        # North-West corner of bb within reference box: # (N < N_max and W > W_min) and (N > S_min and W < E_max)

        if (thisbb[0] < self.bbox[0]) and (thisbb[1] < self.bbox[1]) and (thisbb[0] > self.bbox[2]) and (thisbb[1] > self.bbox[3])\
            or (thisbb[2] > self.bbox[2]) and (thisbb[1] < self.bbox[1]) and (thisbb[2] < self.bbox[0]) and (thisbb[1] > self.bbox[3])\
                or (thisbb[2] > self.bbox[2]) and (thisbb[3] > self.bbox[3]) and (thisbb[2] < self.bbox[0]) and (thisbb[3] < self.bbox[1])\
                    or (thisbb[0] < self.bbox[0]) and (thisbb[3] > self.bbox[3]) and (thisbb[0] > self.bbox[2]) and (thisbb[3] < self.bbox[1]):
            intercept = True
            self.logger.info("Interception occurs")


        ### thisbb (boundary_box) covers the entire self.bbox (reference).
        # North side of bb: # N > N_max
        # East side of bb:  # E > E_max
        # South side of bb: # S < S_min
        # West side of bb:  # W < W_min

        elif (thisbb[0] > self.bbox[0]) and (thisbb[1] > self.bbox[1]) and (thisbb[2] < self.bbox[2]) and (thisbb[3] < self.bbox[3]):
            intercept = True
            self.logger.info("Interception occurs")


        ### thisbb (boundary_box) covers an entire side of self.bbox (reference bbox).
        # North side of self.bbox covered: # (N > N_max and S < N_max) and (E > E_max and W < W_min)
        # East side of self.bbox covered:  # (N > N_max and S < S_min) and (E > E_max and W < E_max)
        # South side of self.bbox covered: # (N > S_min and S < S_min) and (E > E_max and W < W_min)
        # West side of self.bbox covered:  # (N > N_max and S < S_min) and (E > W_min and W < W_min)

        elif (thisbb[0] > self.bbox[0]) and (thisbb[2] < self.bbox[0]) and (thisbb[1] > self.bbox[1]) and (thisbb[3] < self.bbox[3])\
            or (thisbb[0] > self.bbox[0]) and (thisbb[2] < self.bbox[2]) and (thisbb[1] > self.bbox[1]) and (thisbb[3] < self.bbox[1])\
                or (thisbb[0] > self.bbox[2]) and (thisbb[3] < self.bbox[3]) and (thisbb[1] > self.bbox[1]) and (thisbb[3] < self.bbox[3])\
                    or (thisbb[0] > self.bbox[0]) and (thisbb[2] < self.bbox[2]) and (thisbb[1] > self.bbox[3]) and (thisbb[3] < self.bbox[3]):
            intercept = True
            self.logger.info("Interception occurs")

        ### Altered
        if intercept:
            return True
        else:
            return False
            
    def check_mmd(self):
        mymatch = False
        mmd_file = self.mmd_file
        tmpcoll = []
        tmpcoll.append(self.coll)
        tree = ET.ElementTree(file=mmd_file)
        root = tree.getroot()
        mynsmap = {'mmd':'http://www.met.no/schema/mmd'}
        #print ET.tostring(root)

        setInactive = False
        cnvDateTime = False
        # Set all NPI records hosted by IMR for NMDC to inactive
        if self.section == "IMR":
            try:
                myvalue = tree.find("mmd:data_center/mmd:data_center_name/mmd:long_name", namespaces=mynsmap)
                if myvalue != None:
                    if myvalue.text in ["Norwegian Polar Institute"]:
                        setInactive = True
            except Exception as e:
                self.logger.warning("%s didn't contain information on the data centre.", self.mmd_file)
        if not setInactive:
            # Check for empty or incomplete bounding box
            # Add check for multiple bounding box
            elements = tree.findall("mmd:geographic_extent",
                    namespaces=mynsmap)
            if len(elements) != 1:
                self.logger.warning("Error in bounding box (too few or too many)...")
                setInactive = True
            bboxfields = ['mmd:north','mmd:east','mmd:south','mmd:west']
            if tree.find('mmd:geographic_extent/mmd:rectangle',namespaces=root.nsmap) is not None:
                for myel in elements:
                    for myfield in bboxfields:
                        if myel.find('mmd:rectangle/'+myfield,
                                namespaces=root.nsmap) is not None:
                            myvalue = myel.find('mmd:rectangle/'+myfield,
                                    namespaces=root.nsmap).text
                            if myvalue == None or myvalue.isspace():
                                setInactive = True
            else:
                self.logger.warning("No bounding box found in data.")
                setInactive = True
            # Check for multiple temporal periods
            # This should be removed as this is supported by the MMD
            # specification, but not supproted by the SolR ingestion.
            elements = tree.findall("mmd:temporal_extent",
                    namespaces=mynsmap)
            if len(elements) != 1:
                self.logger.warning("Too few or too many temporal extent elements...")
                setInactive = True
            # Check DateTime strings (to be removed later)
            if not setInactive:
                for item in elements[0].iterdescendants():
                    #print type(item), item.tag, item.text
                    if item.text == None:
                        setInactive = True
                        continue
                    if re.match('\d{4}-\d{2}-\d{2}Z', item.text):
                        #item.text = parse(item.text).date().strftime("%Y-%m-%d")
                        item.text = item.text[:-1]
                        cnvDateTime = True
                    elif re.match('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', item.text):
                        item.text = parse(item.text).date().strftime("%Y-%m-%d")
                        cnvDateTime = True

        if setInactive:
            myelement = tree.find("mmd:metadata_status",
                    namespaces=mynsmap)
            myelement.text = "Inactive"

        ''' # Original formulation - need to separate elements so that they make sense in functions
        # Check parameters,bounding box and project
        if self.params and not setInactive:
            elements = tree.findall("mmd:keywords[@vocabulary='gcmd']/mmd:keyword", namespaces=mynsmap)
        if self.bbox and not setInactive:
            elements = tree.findall('mmd:geographic_extent/mmd:rectangle', namespaces=mynsmap)
            print(elements)
        if self.project and not setInactive:
            elements = tree.findall('mmd:project/mmd:short_name', namespaces=mynsmap)
            print(elements)
            for el in elements:
                if el.text == None:
                    setInactive = True
        # Check information found
        # some check of elements content...
        #'''

        # Altered formulation to be able to check these three at the same time
        # Check parameters,bounding box and project
        if self.params and not setInactive:
            params_elements = tree.findall("mmd:keywords[@vocabulary='gcmd']/mmd:keyword", namespaces=mynsmap)
        if self.bbox and not setInactive:
            bbox_elements = tree.findall('mmd:geographic_extent/mmd:rectangle', namespaces=mynsmap)
            print(bbox_elements)
        if self.project and not setInactive:
            project_elements = tree.findall('mmd:project/mmd:short_name', namespaces=mynsmap)
            print(project_elements)
            for el in project_elements:
                if el.text == None:
                    setInactive = True
        # Check information found
        # some check of elements content...


        # Decide on test
        if self.bbox and not setInactive:
            if self.check_bounding_box(bbox_elements,root):
                mymatch = True
        if self.params and not setInactive:
            if self.check_params(params_elements,root):
                mymatch = True
        if self.project and not setInactive:
            if self.check_project(project_elements,root):
                mymatch = True
        if (mymatch == False and 
            setInactive == False and cnvDateTime == False):
            return mymatch

        # Check if the collection is already added, and add if not
        if mymatch:
            for item in tmpcoll:
                myel = '//mmd:collection[text()="'+item+'"]'
                myelement = tree.xpath(myel, namespaces=mynsmap)
                if myelement:
                    self.logger.warning("Already belongs to %s",item)
                    #tmpcoll.remove(item)
                else:
                    # Add new collections
                    myelement = tree.find('mmd:collection', namespaces=mynsmap)
                    if myelement is not None:
                        mycollection = myelement.getparent()
                        mycollection.insert(mycollection.index(myelement),
                                ET.XML("<mmd:collection xmlns:mmd='http://www.met.no/schema/mmd'>"+item+"</mmd:collection>"""))
        #tree = ET.ElementTree(mycollection)
        ET.indent(root, space="  ")
        tree.write(mmd_file, pretty_print=True)

        return mymatch

def main(argv):
    # This is the main method

    # Parse command line arguments
    try:
        args = parse_arguments()
    except:
        raise SystemExit('Command line arguments didn\'t parse correctly.')

    if args.sources:
        mysources = args.sources.split(',')

    # Set up logging
    mylog = initialise_logger(args.logfile, 'filter_mmd_records')
    mylog.info('\n==========\nConfiguration of logging is finished.')

    # Read config file
    #mylog.info("Reading configuration from: %s", args.cfgfile)
    with open(args.cfgfile, 'r') as ymlfile:
        cfg = yaml.full_load(ymlfile)

    # Define parameters to find
    # Not working yet...
    if args.parameters:
        parameters = args.parameters.split(',')
    else:
        parameters = None

    # If filtering for GCW, parameters and collection are added automatic
    bounding = project = None
    if args.gcw:
        parameters = ["CRYOSPHERE",
                "TERRESTRIAL HYDROSPHERE &gt; SNOW/ICE",
                "OCEANS &gt; SEA ICE"]
        collection = "GCW"
    elif args.aen:
        project = "Nansen Legacy"
        collection = "AeN"
    elif args.sios:
        bounding = [90.,40.,70.,-20.]
        collection = "SIOS"
    elif args.infranor:
        project = "SIOS INFRANOR"
        collection = "SIOSIN"
    elif args.nmap:
        project = "NORMAP"
        collection = "NMAP"
    elif args.cryoclim:
        project = "CryoClim"
        collection = "CC"
    elif args.nysmac:
        #bounding = [79.11850,10.45540,78.72381,14.06356]   # ORIGINAL
        bounding = [79.11850,14.06356,78.72381,10.45540]    # Reformulated as the order seem to be [N, E, S, W]
        collection ="NySMAC"
    elif args.tone:
        #bounding = [90.,50.,50.,45.]   # ADDED 
        project = "TONe"                # Filled in
        collection ="TONE"

    # Define collections to add
    if args.collection:
        collection = args.collection.split(',')
    elif ((not args.sios) and (not args.gcw) and (not args.cryoclim) and (not args.nmap) and (not args.aen) and (not args.infranor) and (not args.tone) and (not args.nysmac)):
        collection = None

    # Read config file
    mylog.info("Reading configuration from: %s", args.cfgfile)
    with open(args.cfgfile, 'r') as ymlfile:
        cfg = yaml.full_load(ymlfile)

    # Define bounding box
    # Provided as comma separated list (N,E,S,W)
    if args.bounding:
        bbox = args.bounding.split(",")
        bounding = [float(i) for i in bbox]

    # Each section is a data centre to handle
    for section in cfg:
        if args.sources:
            if section not in mysources:
                continue
        mylog.info("Filtering records from: %s", section)

        # Find files to process
        try:
            myfiles = os.listdir(cfg[section]['mmd'])
        except os.error:
            mylog.error('Couldn\'t find files to process: %s', os.error)
            sys.exit(1)

        # Process files, dump valid filenames to file
        i=1
        s = "/"
        for myfile in myfiles:
            if myfile.endswith(".xml"):
                mylog.info('Processing file %d, %s', i, myfile)
                i += 1
                file2check = LocalCheckMMD('filter_mmd_records', section,s.join((cfg[section]['mmd'],myfile)), bounding, parameters, collection, project)
                if file2check.check_mmd():
                    mylog.info("Success")
                else:
                    mylog.info("Failure")
    mylog.info('Processing finished.')


if __name__ == '__main__':
    main(sys.argv[1:])
