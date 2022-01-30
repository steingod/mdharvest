#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
PURPOSE:
    Check if raw files haven't been updated in a recent full harvest. Since the harvest is running a full harvest on a regular basis, no raw files that are still active should be older than the most recent full harvest. This helps ensure no stale datasets are shown for information harvested using PGC CSW and other mechanism that do not have information on deleted datasets like OAI-PMH have. Files that are checked are in the raw area, files to be modified (set inactive) are in the mmd artea for each data centre respectively.

AUTHOR:
    Øystein Godøy, METNO/FOU, 2019-04-23 

UPDATED:
    Øystein Godøy, METNO/FOU, 2022-01-30 
        Further refined.

NOTES:
    - NA

"""

import sys
import os
import argparse
import yaml
from harvest_metadata import setInactive,initialise_logger
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
import time

def parse_arguments():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("-c","--config",dest="cfgfile", help="Configuration file containing endpoints to harvest", required=True)
    parser.add_argument("-l","--logfile",dest="logfile", help="Log file", required=True)
    parser.add_argument("-f","--from",dest="fromTime", help="DateTime to check agains, in the form YYYY-MM-DD, older files are set inactive", required=False)
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

def loop_directory(mylog, dir2c, dir2m):
    mylog.info('Checking files')
    # Set default outdated time, should be one week
    defoutdtime = 60*60*24*7
    for fn in os.listdir(dir2c):
        if fn.endswith('.xml'):
            lastmtime = os.path.getmtime('/'.join([dir2c,fn]))
            # Check against minimum check
            # TODO: Make configureable and add check from command line
            if lastmtime < (time.time()-defoutdtime):
                mmdid = fn.rstrip('.xml')
                mylog.info("File %s will be set inactive", fn)
                setInactive(dir2m, mmdid, mylog)

    return

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
    mylog = initialise_logger(args.logfile,'check4outdated')
    mylog.info('\n==========\nConfiguration of logging is finished.')

    # Read config file
    mylog.info("Reading configuration from: %s", args.cfgfile)
    with open(args.cfgfile, 'r') as ymlfile:
        cfg = yaml.full_load(ymlfile)

    # Each section is a data centre to harvest
    for section in cfg:
        if args.sources:
            if section not in mysources:
                continue
        mylog.info('\n')
        mylog.info('====')
        mylog.info('Checking: %s for old files',section)
        mylog.info('Looping harvested files in: %s', cfg[section]['raw'])
        loop_directory(mylog, cfg[section]['raw'],cfg[section]['mmd'])

if __name__ == '__main__':
    main(sys.argv[1:])
