#!/usr/bin/python3
# This script is used in the context of ad-hoc enrichment of harvested metadata
# The script:
# - parses a csv file with a specific format containing: an observation facility name (from a controlled list),
#   the original identifier of the record, a space separated list of collections (from a controlled list) and
#   the metadata endpoint where the record is served
# - creates a yml file, grouped by sources
#
# The yml file can then be the input for the filtering routine
#
#
#
import sys
import os
import argparse
import csv
import yaml
import vocab.ResearchInfra
import vocab.ControlledVocabulary
from collections import defaultdict
from urllib.parse import urlparse

def parse_arguments():
    parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
            description='creates yaml enrichment file from csv')
    parser.add_argument("-c","--config",dest="cfgfile", help="Configuration file containing endpoints to map", required=True)
    parser.add_argument("-i", "--input", required=True, help='The input file')
    parser.add_argument("-o", "--output", required=True, help='The output file')

    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        parser.exit()

    return args

def csv_to_yaml(csv_file, yaml_file, mapping):

    #simplify headers
    new_headers = {
    "machine-readable link to the metadata catalogue": "url_repository",
    "station/vessel/platform" : "obsfacility"
    }

    parsed_list = []
    dict_to_yaml = {}

    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        updated_fieldnames = [new_headers.get(field, field) for field in reader.fieldnames]
        for row in reader:
            updated_row = {new_headers.get(key, key): value for key, value in row.items()}
            stripped_row = {key: value.strip() for key, value in updated_row.items()}
            parsed_list.append(stripped_row)

    rimapping = vocab.ResearchInfra.RI
    collectionmapping = vocab.ControlledVocabulary.CollectionKeywords
    grouped_by_type = defaultdict(list)
    # create structure with source (mapping to url of the endpoint) as group, and elements per id with collection and obsfacility, e.g.:
    # SOURCE:
    # - id: uuid
    #   collection: SESS2019,, POLARIN
    #   obsfacility: Name of the obs facility
    for entry in parsed_list:
        provided_url = urlparse(entry['url_repository'])
        base_provided_url = f"{provided_url.scheme}://{provided_url.netloc}"

        if base_provided_url in mapping.keys() and entry['id']:
            # make sure that info is compliant with MMD vocabulary
            obsname = None
            if entry.get('obsfacility'):
                for key, value in rimapping.items():
                    if entry['obsfacility'] == key or entry['obsfacility'] in value['kw']:
                        obsname = key
            collection = []
            if entry.get('tag'):
                # assume collection tags space separated in csv.
                tmplist = entry['tag'].split(" ")
                for l in tmplist:
                    if l in collectionmapping:
                        collection.append(l)
            if collection and obsname:
                grouped_by_type[mapping[base_provided_url]].append({'id': entry['id'], 'collection': ", ".join(collection), 'obsfacility': obsname})
            elif collection:
                grouped_by_type[mapping[base_provided_url]].append({'id': entry['id'], 'collection': ", ".join(collection)})
            elif obsname:
                if rimapping[obsname]['polarin'] is True:
                    grouped_by_type[mapping[base_provided_url]].append({'id': entry['id'], 'collection': 'POLARIN', 'obsfacility': obsname})
                else:
                    grouped_by_type[mapping[base_provided_url]].append({'id': entry['id'], 'obsfacility': obsname})

    final_dict = dict(grouped_by_type)
    # write to yaml
    with open(yaml_file, 'w') as outfile:
        yaml.dump(final_dict, outfile, sort_keys=False, indent=2) #

# Usage

if __name__ == '__main__':

    try:
        args = parse_arguments()
    except Exception as e:
        print(e)
        sys.exit()

    mapping_resources = {}

    with open(args.cfgfile) as ymlfile:
        cfg = yaml.full_load(ymlfile)

        for section in cfg:
            if 'source' in cfg[section]:
                #create an easy mapping between base url machine readable endpoing and section name
                parsed_url = urlparse(cfg[section]['source'])
                source = f"{parsed_url.scheme}://{parsed_url.netloc}"
                mapping_resources[source] = section

    if mapping_resources:
        try:
            csv_to_yaml(args.input, args.output, mapping_resources)
        except Exception as e:
            print(e)
            sys.exit()
    else:
       print('Configuration mapping could not be created')

