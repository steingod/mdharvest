#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
PURPOSE:
Harvest metadata from a number of data centres contributing to GCW and
other activities, prepare metadata for ingestion.

Following a request from GCW Steering Group, the number of ingested
records is monitored. Distinguish between active and deleted records.

AUTHOR:
    Øystein Godøy, METNO/FOU, 2014-01-05 
        Original Perl variant.

UPDATED:
    Øystein Godøy, METNO/FOU, 2018-05-10 
        Modified to Python
    Øystein Godøy, METNO/FOU, 2021-02-18 
        Added logging and corrected some bugs. Improved error handling and selective harvesting.

NOTES:
    - NA

"""

import sys
import os
import argparse
import yaml
from mdh_modules.harvest_metadata import *
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

def parse_arguments():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("-c","--config",dest="cfgfile", help="Configuration file containing endpoints to harvest", required=True)
    parser.add_argument("-l","--logfile",dest="logfile", help="Log file", required=True)
    parser.add_argument("-f","--from",dest="fromTime", help="DateTime to  harvest fromday in the form YYYY-MM-DD", required=False)
    parser.add_argument('-s','--sources',dest='sources',help='Comma separated list of sources (in config) to harvest',required=False)

    args = parser.parse_args()

    if args.fromTime:
        try:
            datetime.strptime(args.fromTime,'%Y-%m-%d')
        except ValueError:
            raise ValueError

    if args.cfgfile is None:
        parser.print_help()
        parser.exit()

    return args

###########################################################
def main(argv):
    # Parse command line arguments
    try:
        args = parse_arguments()
    except:
        raise SystemExit('Command line arguments didn\'t parse correctly.')

    if args.sources:
        mysources = args.sources.split(',')

    # Set up logging
    print(args.logfile)
    mylog = initialise_logger(args.logfile,'run-harvest')
    mylog.info('\n==========\nConfiguration of logging is finished.')

    # Read config file
    mylog.info("Reading configuration from: %s", args.cfgfile)
    with open(args.cfgfile, 'r') as ymlfile:
        cfg = yaml.full_load(ymlfile)

    # Check that all relevant directories exists...
    if check_directories(cfg):
        warnings.warn("Something went wrong creating directories")
        sys.exit(2)

    # Each section is a data centre to harvest
    for section in cfg:
        if args.sources:
            if section not in mysources:
                continue
        mylog.info('\n\n====\nChecking: '+section)
        if cfg[section]['protocol'] == 'OAI-PMH':
            if cfg[section]['set']:
                if args.fromTime:
                    request = "?verb=ListRecords"\
                            "&metadataPrefix="+cfg[section]['mdkw']+\
                            "&set="+cfg[section]['set']+\
                            "&from="+args.fromTime
                else:
                    request = "?verb=ListRecords"\
                            "&metadataPrefix="+cfg[section]['mdkw']+\
                            "&set="+cfg[section]['set'] 
            else:   
                if args.fromTime:
                    request = "?verb=ListRecords"\
                            "&metadataPrefix="+cfg[section]['mdkw']+\
                            "&from="+args.fromTime
                else: 
                    request = "?verb=ListRecords"\
                            "&metadataPrefix="+cfg[section]['mdkw']
        elif cfg[section]['protocol'] == 'OGC-CSW':
            if section == "EUMETSAT-CSW":
                request ="?SERVICE=CSW&VERSION=2.0.2"\
                        "&request=GetRecords" \
                        "&resultType=results"\
                        "&outputSchema=http://www.isotc211.org/2005/gmd"\
                        "&elementSetName=full"
            elif section == "WGMS":
                request ="?SERVICE=CSW&VERSION=2.0.2"\
                        "&request=GetRecords" \
                        "&constraintLanguage=CQL_TEXT" \
                        "&typeNames=csw:Record"\
                        "&resultType=results"\
                        "&outputSchema=http://www.isotc211.org/2005/gmd" \
                        "&elementSetName=full"
            else:
                request ="?SERVICE=CSW&VERSION=2.0.2"\
                        "&request=GetRecords" \
                        "&constraintLanguage=CQL_TEXT" \
                        "&typeNames=csw:Record"\
                        "&resultType=results"\
                        "&outputSchema=http://www.isotc211.org/2005/gmd" \
                        "&elementSetName=full"
        else:
            mylog.warn("The chosen protocol is not supported yet")
            continue
        numRec = 0
        mh = MetadataHarvester('run-harvest', cfg[section]['source'],
                request,cfg[section]['raw'],cfg[section]['mmd'],
                cfg[section]['protocol'],
                cfg[section]['mdkw'])
        try: 
            numRec = mh.harvest()
        except Exception as e:
            mylog.warning("Something went wrong on harvest from "+section)
            mylog.warning("Exception message: " + str(e))
        mylog.info("Number of records harvested "+section+': '+str(numRec))

    sys.exit(0)

if __name__ == '__main__':
    main(sys.argv[1:])

