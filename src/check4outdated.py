#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
PURPOSE:
    Clean raw/mmd directories for harvested/transformed files.
    This script should be run programmatically after a full harvest so that:
    - mmd files corresponding to raw files older than 1st of the current month are set Inactive
    - mmd files that do not have corresponding raw files are set to Inactive
    - raw files that are older than 1st of the month are deleted

    If the -f (time to check against) is not provided the 1st of the current month is used.

    The script is discriminating between OAI-PMH and OGC-GCW protocol:
    - if OAI-PMH is used, it checks that files from 1st of the month are present, assuming then that a full harvest has been done.
      If so, it cleans the directories, otherwise it aborts. If the date is provided with -f this check is not done (assuming we proactively
      want to delete files until the date provided).
    - if OGC-CSW is used, it will clean files older then the provided date, or defaut 1st of the month date.

    Removing stale raw files ensures that they are not transformed in mmd, and therefore the Inactive mmd files are not overwritten.

    A dry run can be done to check which files would be deleted.

AUTHOR:
    Øystein Godøy, METNO/FOU, 2019-04-23

UPDATED:
    Øystein Godøy, METNO/FOU, 2022-01-30
        Further refined.

    Lara Ferrighi, METNO/FOU, 2026-08-28
        Extend functionality and include protocols and removing of files. Adding dry run

NOTES:
    - NA

"""
import sys
import os
import argparse
import yaml
from mdh_modules.harvest_metadata import setInactive, initialise_logger
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from datetime import datetime, timedelta


def parse_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--config", dest="cfgfile", help="Configuration file containing endpoints to harvest", required=True)
    parser.add_argument("-l", "--logfile", dest="logfile", help="Log file", required=True)
    parser.add_argument("-f", "--from", dest="fromTime", help="DateTime to check against, in the form YYYY-MM-DD, older files are set Inactive", required=False)
    parser.add_argument("-s", "--sources", dest="sources", help="Comma-separated list of sources (in config) to harvest", required=False)
    parser.add_argument("-d", "--dry-run", action="store_true", help="Perform a dry run (no files will be changed)")

    args = parser.parse_args()

    if args.fromTime:
        try:
            datetime.strptime(args.fromTime, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Invalid date format. Use YYYY-MM-DD.")

    if args.cfgfile is None:
        parser.print_help()
        parser.exit()

    return args


def check_files_from_first_of_month(mylog, dir2c):
    """
    Checks that there are files in the raw folder from the exact date of the 1st of the current month.
    This would likely mean that for OAI-PMH a full reharvest is done.
    """
    mylog.info("Checking for files from the exact date of the 1st of the month in: %s", dir2c)

    # Get the timestamp range for the 1st of the current month
    now = datetime.now()
    first_of_month_start = datetime(now.year, now.month, 1).timestamp()
    first_of_month_end = (datetime(now.year, now.month, 1) + timedelta(days=1)).timestamp() - 1

    # Find all files modified on the 1st of the month
    files_from_first = [
        fn for fn in os.listdir(dir2c)
        if fn.endswith('.xml') and first_of_month_start <= os.path.getmtime(os.path.join(dir2c, fn)) <= first_of_month_end
    ]

    # Check if any files were found
    if not files_from_first:
        mylog.error("No files from the 1st of the month found in %s. A full harvest has probably not been done. Aborting cleanup.", dir2c)
        raise RuntimeError(f"No files from the exact date 1st of the month found in {dir2c}. Cleanup aborted.")

    mylog.info("Found %d file(s) from the 1st of the month in %s. Proceeding with cleanup.", len(files_from_first), dir2c)


def loop_directory(mylog, dir2c, dir2m, olderthan, dry_run=False):
    """
    Loop through the raw folder, set Inactive mmd files for outdated raw files, and delete outdated raw files.
    """
    mylog.info("Checking files in %s", dir2c)

    # Keep track of all raw files for comparison with mmd files
    raw_files = set(Path(fn).stem for fn in os.listdir(dir2c) if fn.endswith('.xml'))

    # Process raw files
    for fn in os.listdir(dir2c):
        if fn.endswith('.xml'):
            raw_file_path = os.path.join(dir2c, fn)
            lastmtime = os.path.getmtime(raw_file_path)

            if lastmtime < olderthan:
                mmdid = Path(fn).stem
                if dry_run:
                    mylog.info("[DRY RUN] mmd file %s would be set as Inactive and raw deleted.", fn)
                else:
                    mylog.info("File %s is outdated and will be processed", fn)

                    # Set corresponding mmd file to Inactive
                    setInactive(dir2m, mmdid, mylog)
                    mylog.info("Corresponding mmd file for %s has been set to Inactive", fn)

                    # Delete the outdated raw file, otherwise the inactive mmd will be overwritten by xmltransform.
                    try:
                        os.remove(raw_file_path)
                        mylog.info("Deleted outdated raw file: %s", raw_file_path)
                    except Exception as e:
                        mylog.error("Error deleting file %s: %s", raw_file_path, str(e))
            else:
                mylog.debug("File %s is still valid", fn)

    # Check mmd files for corresponding raw files.
    # Due to old cleaning only in the raw folders, mmd files might still be present and never set to inactive before.
    mylog.info("Checking mmd folder %s for orphaned files.", dir2m)
    for fn in os.listdir(dir2m):
        if fn.endswith('.xml'):
            mmdid = Path(fn).stem
            if mmdid not in raw_files:
                if dry_run:
                    mylog.info("[DRY RUN] mmd file %s would be set to Inactive.", fn)
                else:
                    mylog.info("No corresponding raw file for mmd file %s. Setting it to Inactive.", fn)
                    setInactive(dir2m, mmdid, mylog)

    return


def main(argv):
    # Parse command line arguments
    try:
        args = parse_arguments()
    except Exception as e:
        raise SystemExit(f"Error parsing command line arguments: {e}")

    if args.sources:
        mysources = args.sources.split(',')

    if args.fromTime:
        # Use the provided `-f` value
        olderthan = datetime.strptime(args.fromTime, '%Y-%m-%d').timestamp()
        print(f"Using -f value: {args.fromTime} ({olderthan})")
        check_for_first_of_month = False  # Skip the check if -f is provided
    else:
        # Default to the 1st of the current month at midnight
        first_of_month = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        olderthan = first_of_month.timestamp()
        print(f"Defaulting -f to the 1st of the current month: {first_of_month.strftime('%Y-%m-%d')} ({olderthan})")
        check_for_first_of_month = True  # Perform the check only if -f is not provided

    # Set up logging
    mylog = initialise_logger(args.logfile, 'check4outdated')
    mylog.info("\n==========\nConfiguration of logging is finished.")

    # Read config file
    mylog.info("Reading configuration from: %s", args.cfgfile)
    try:
        with open(args.cfgfile, 'r') as ymlfile:
            cfg = yaml.full_load(ymlfile)
    except Exception as e:
        mylog.error("Failed to read configuration file: %s", str(e))
        sys.exit(1)

    # Process each source section in the config
    for section in cfg:
        if args.sources and section not in mysources:
            continue

        mylog.info('\n')
        mylog.info('====')
        mylog.info('Checking: %s for old files', section)
        raw_dir = cfg[section].get('raw')
        mmd_dir = cfg[section].get('mmd')
        protocol = cfg[section].get('protocol')

        if not raw_dir or not mmd_dir:
            mylog.warning("Source %s is missing raw or mmd directory in configuration.", section)
            continue

        # Check for files from the 1st of the month only for OAI-PMH protocol
        if protocol == "OAI-PMH" and check_for_first_of_month:
            mylog.info("Protocol for %s is OAI-PMH. Checking for files from the 1st of the month.", section)
            try:
                check_files_from_first_of_month(mylog, raw_dir)
            except RuntimeError as e:
                mylog.error(str(e))
                continue

        # Proceed with cleanup
        mylog.info("Looping harvested files in: %s", raw_dir)
        loop_directory(mylog, raw_dir, mmd_dir, olderthan, dry_run=args.dry_run)


if __name__ == '__main__':
    main(sys.argv[1:])
