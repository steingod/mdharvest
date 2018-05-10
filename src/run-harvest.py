#!/usr/bin/python
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

COMMENTS:

"""

import sys
import os
import getopt
import yaml
from harvest_metadata import MetadataHarvester

def usage():
    print sys.argv[0]+" [options] input"
    print "\t-h|--help: dump this information"
    print "\t-c|--config: specify the configuration file to use"
    print "\t-l|--logfile: specify the logfile to use"
    sys.exit(2)

def check_directories(cfg):
    for section in cfg:
        for name in ['dest1','dest2']:
            #print section, name
            ##print cfg[section][name]
            if not os.path.isdir(cfg[section][name]):
               try:
                   os.makedirs(cfg[section][name])
               except:
                   print "Could not create output directory"
                   return(2)
    return(0)


###########################################################
def main(argv):
    # Parse command line arguments
    try:
        opts, args = getopt.getopt(argv,"hc:l:",
                ["help","config","logfile"])
    except getopt.GetoptError:
        usage()

    cflg = lflg = False
    for opt, arg in opts:
        if opt == ("-h","--help"):
            usage()
        elif opt in ("-c","--config"):
            cfgfile = arg
            cflg =True
        elif opt in ("-l","--logfile"):
            logfile = arg
            lflg =True

    if not cflg:
        usage()
    elif not lflg:
        usage()

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
        if cfg[section]['protocol'] == 'OAI-PMH':
            if cfg[section]['set']:
                request = "?verb=ListRecords"\
                        "&metadataPrefix="+cfg[section]['mdkw']+\
                        "&set="+cfg[section]['set'] 
            else:   
                request = "?verb=ListRecords"\
                        "&metadataPrefix="+cfg[section]['mdkw']
        elif cfg[section]['protocol'] == 'OGC-CSW':
            request ="?SERVICE=CSW&VERSION=2.0.2"\
                    "&request=GetRecords&constraintLanguage=CQL_TEXT" \
                    "&typeNames=csw:Record"\
                    "&resultType=results"\
                    "&outputSchema=http://www.isotc211.org/2005/gmd&elementSetName=full"
        else:
            print "Protocol not supported yet"
        print request
        mh = MetadataHarvester(cfg[section]['source'],
                request,cfg[section]['dest1'],cfg[section]['protocol'])
        try: 
            mh.harvest()
        except Exception, e:
            print "Something went wrong on harvest from", section
            print str(e)

    sys.exit(0)

if __name__ == '__main__':
    main(sys.argv[1:])

