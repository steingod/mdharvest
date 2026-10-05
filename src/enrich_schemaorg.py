#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

'''
This script processes XML files generated through schemaorg harvesting, thus missing the raw files,
to enrich them with additional metadata based on a configuration file and an enrichment YAML file.
It supports a dry-run mode to simulate the changes without modifying the files.

It is based on filter_mmd_records.py, but developed for the time being separately, as the harvesting
procedure is not homogeneous. It add collections and observation facilities to XML files.

In the future this script can be merged with filter_mmd_records.py

Command-Line Options:
---------------------
-c, --config <path>       Path to the YAML configuration file specifying directories to process (recursively)
-l, --logfile <path>      Path to the log file where processing logs will be written.
-e, --enrich <path>       Path to the enrichment YAML file containing metadata to add to the XML files.
-r, --resources <list>    Comma-separated list of resources to process (e.g., PANGAEA,GEM). Optional.
--dry-run                 Simulate the process without modifying any files. Optional.

Example Usage:
--------------
1. Normal Mode (Modify Files):
   python enrich_script.py -c schemaorg-harvester.yml -l enrich.log -e enrich-schemaorg.yml

2. Dry-Run Mode (No Modifications):
   python enrich_script.py -c schemaorg-harvester.yml -l enrich.log -e enrich-schemaorg.yml --dry-run

The configuration file is needed to point to the mmd folder(s).
The enrichment file is providing info for enriching. The id is used to figure our what xml file should be enhanced.
The enrichment file would look like:
PANGAEA:
- id: doi:10.1594/PANGAEA.925357
  collection: SIOSAP
- id: doi:10.1594/PANGAEA.846617
  collection: SESS2018
- id: doi:10.1594/PANGAEA.971787
  collection: POLARIN
  obsfacility: 'Kevo Research Station'
- id: doi:10.1594/PANGAEA.111222
  collection: GCW

'''

import sys
import os
import argparse
import lxml.etree as ET
import yaml
import logging
import vocab.ResearchInfra
from logging.handlers import TimedRotatingFileHandler
from mdh_modules.harvest_metadata import initialise_logger


def parse_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--config", dest="cfgfile", help="Configuration file containing MMD folders to enrich", required=True)
    parser.add_argument("-l", "--logfile", dest="logfile", help="Log file", required=True)
    parser.add_argument('-e', '--enrich', dest='enrich', help='Enrich MMD based on YAML file', required=False)
    parser.add_argument('-r', '--resources', dest='resources', help='Comma-separated list of resources to process (e.g., PANGAEA,GEM)', required=False)
    parser.add_argument('-d', '--dry-run', action='store_true', help="Simulate the process without modifying any files")

    args = parser.parse_args()

    if args.cfgfile is None:
        parser.print_help()
        parser.exit()

    return args

def index_directory(base_path):
    """
    Index all XML files in the directory and return a mapping of transformed filenames to their full paths.
    """
    file_index = {}
    for root, _, files in os.walk(base_path):
        for file in files:
            if file.endswith(".xml"):
                file_index[file] = os.path.join(root, file)
    return file_index


def find_file_in_index(file_index, file_id):
    """
    Find a file in the pre-built index using the transformed file_id.
    This is ad-hoc, based on the filename creation used in the harvester.
    """
    # Remove "doi:10.1594/" prefix if it exists
    if file_id.startswith("doi:10.1594/"):
        file_id = file_id[12:]  # Strip "doi:10.1594/"

    # Replace dots and slashes with dashes
    transformed_file_id = file_id.replace(".", "-").replace("/", "-") + ".xml"

    return file_index.get(transformed_file_id)


class LocalCheckMMD():
    def __init__(self, logname, section, enrich, vocab):
        self.logger = logging.getLogger('.'.join([logname, 'LocalCheckMMD']))
        self.logger.info('Creating an instance of LocalCheckMMD')
        self.section = section
        self.enrich = enrich
        self.vocab = vocab

    def check_for_enrichment(self, selected, root):
        """
        Check and apply enrichment to an XML tree root.
        Adds 'collection' and 'obsfacility' (observation facility) if provided in the enrichment.
        Observation facilities are only added if they exist in the rimapping (self.vocab).
        """
        enrichmatch = False
        mynsmap = {'mmd': 'http://www.met.no/schema/mmd'}

        # Add collection
        if 'collection' in selected.keys():
            collection_to_add = selected['collection']
            existing_collections = [
                node.text for node in root.findall("./mmd:collection", namespaces=mynsmap)
            ]
            if collection_to_add in existing_collections:
                self.logger.info(f"Collection '{collection_to_add}' is already present in the file.")
            else:
                self.logger.info(f"Adding collection: {collection_to_add}")
                last_collection = root.findall("./mmd:collection", namespaces=mynsmap)[-1] if existing_collections else None
                new_node = ET.Element("{http://www.met.no/schema/mmd}collection")
                new_node.text = collection_to_add
                # the schemaorg harvester is always adding collections by default. At least ADC and NSDN
                if last_collection is not None:
                    last_collection.addnext(new_node)
                enrichmatch = True

        # Add observation facility
        if 'obsfacility' in selected.keys():
            new_obsfacilities = [facility.strip() for facility in selected['obsfacility'].split(',')]
            if new_obsfacilities:
                # Find existing observation facilities in the XML
                existing_facilities = [
                    ri.text for ri in root.findall("./mmd:related_information[mmd:type = 'Observation facility']/mmd:description", namespaces=mynsmap)
                ]
                # Determine which facilities need to be added
                facilities_to_add = list(set(new_obsfacilities) - set(existing_facilities))
                rimapping = self.vocab  # Use the vocabulary mapping for additional information
                for facility in new_obsfacilities:
                    if facility in existing_facilities:
                        self.logger.info(f"Observation facility '{facility}' is already present in the file.")
                    elif facility in rimapping:
                        self.logger.info(f"Adding observation facility: {facility}")
                        # Create the related information structure for the observation facility
                        related_info = ET.Element("{http://www.met.no/schema/mmd}related_information")
                        type_element = ET.SubElement(related_info, '{http://www.met.no/schema/mmd}type')
                        type_element.text = 'Observation facility'
                        description_element = ET.SubElement(related_info, '{http://www.met.no/schema/mmd}description')
                        description_element.text = facility
                        resource_element = ET.SubElement(related_info, '{http://www.met.no/schema/mmd}resource')
                        resource_element.text = rimapping[facility]['resource']
                        root.append(related_info)
                        enrichmatch = True
                    else:
                        self.logger.warning(f"Facility '{facility}' not found in mapping. Skipping.")

        return enrichmatch

    def process_file(self, file_path, enrichments, dry_run=False):
        """Process a single file and apply enrichment if applicable."""
        tree = ET.parse(file_path)
        root = tree.getroot()

        # Get the identifier from the XML file
        identifier_node = root.find('mmd:metadata_identifier', namespaces=root.nsmap)
        if identifier_node is None:
            self.logger.warning("No <mmd:metadata_identifier> found in %s", file_path)
            return "SKIPPED"

        identifier = identifier_node.text

        for enrichment in enrichments:
            if identifier == enrichment['id']:
                self.logger.info("Enriching file: %s with collection: %s", file_path, enrichment.get('collection'))
                if self.check_for_enrichment(enrichment, root):
                    if dry_run:
                        # In dry-run mode, log what would happen instead of saving
                        self.logger.info("[DRY-RUN] Changes would be applied to file: %s", file_path)
                    else:
                        # Write the updated tree back to the file
                        ET.indent(tree, space="  ")
                        tree.write(file_path, pretty_print=True, encoding='UTF-8', xml_declaration=True)
                    return "PROCESSED"
                else:
                    # File is already enriched
                    self.logger.info("File is already enriched: %s", file_path)
                    return "ALREADY_PROCESSED"

        return "SKIPPED"


def main(argv):
    args = parse_arguments()

    # Read the enrichment file
    if args.enrich:
        with open(args.enrich, 'r') as ymlfile:
            fullenrichment = yaml.full_load(ymlfile)
    else:
        fullenrichment = None

    # Set up logging
    mylog = initialise_logger(args.logfile, 'enrich_schemaorg_mmd_records')
    mylog.info('\n==========\nConfiguration of logging is finished.')

    # Log whether the script is in dry-run mode. This is mainly for testing.
    if args.dry_run:
        mylog.info("Running in DRY-RUN mode. No files will be modified.")

    # Read configuration file
    mylog.info("Reading configuration from: %s", args.cfgfile)
    with open(args.cfgfile, 'r') as ymlfile:
        config = yaml.full_load(ymlfile)

    # Use the vocabulary from the imported module
    rimapping = vocab.ResearchInfra.RI  # Import the vocabulary directly

    # Filter resources based on the `-r` argument
    if args.resources:
        requested_resources = args.resources.split(',')
        requested_resources = [res.strip() for res in requested_resources]

        # Check on the input sources
        invalid_resources = [res for res in requested_resources if res not in config]
        for invalid_res in invalid_resources:
            mylog.warning("Resource %s not found in the configuration file.", invalid_res)

        config = {key: value for key, value in config.items() if key in requested_resources}

    if not config:
        mylog.error("No valid resources to process. Exiting.")
        return

    for section, paths in config.items():
        if fullenrichment is not None and section in fullenrichment.keys():
            enrichments = fullenrichment[section]
            expected_updates = len(enrichments)
        else:
            enrichments = None
            expected_updates = 0
            mylog.info("No dedicated enrichment for: %s", section)

        mylog.info("Working in directory: %s", paths['mmd'])
        file_index = index_directory(paths['mmd'])

        mylog.info("Processing section: %s", section)
        processed_count = 0
        already_processed_count = 0
        skipped_count = 0

        if enrichments:
            for enrichment in enrichments:
                file_id = enrichment['id']  # No transformation here because it's handled in find_file_in_index
                file_path = find_file_in_index(file_index, file_id)

                if file_path:
                    mylog.info("Found file: %s", file_path)
                    checker = LocalCheckMMD(
                        logname='enrich_schemaorg_mmd_records',
                        section=section,
                        enrich=enrichments,
                        vocab=rimapping
                    )
                    status = checker.process_file(file_path, enrichments, dry_run=args.dry_run)
                    if status == "PROCESSED":
                        processed_count += 1
                    elif status == "ALREADY_PROCESSED":
                        already_processed_count += 1
                    elif status == "SKIPPED":
                        skipped_count += 1
                else:
                    mylog.warning("File not found for ID: %s in section: %s", file_id, section)
                    skipped_count += 1

        mylog.info("Expected updates for %s: %d, Newly Processed: %d, Already Updated: %d, Skipped: %d",
                   section, expected_updates, processed_count, already_processed_count, skipped_count)

    mylog.info('Processing finished.')

if __name__ == '__main__':
    main(sys.argv[1:])
