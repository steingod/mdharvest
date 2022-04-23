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

def retrieve_identifier(myfile):

    try:
        mytree = ET.parse(myfile)
    except Exception as e:
        print('Couldn\'t open and parse XML file', e)
        raise
    myroot = mytree.getroot()
    #print(myroot.nsmap)
    myid = myroot.find('mmd:metadata_identifier',namespaces=myroot.nsmap).text
    return(myid)

def add_parent(myfile, parentid):
    try:
        mytree = ET.parse(myfile)
    except Exception as e:
        print('Couldn\'t open and parse XML file', e)
        return(None)
    myroot = mytree.getroot()

    # Add related_dataset
    mynode = ET.Element("{http://www.met.no/schema/mmd}related_dataset",
            relation_type='parent')
    mynode.text = parentid
    myroot.insert(-1, mynode)

    # Dump results to file
    try:
        mytree.write(myfile, pretty_print=True)
    except Exception as e:
        print('Couldn\'t create new file with identifier')
        raise

    return

def main():

    srcdir = '/disk1/data/adc/mmd4adc-gitlab/XML/applicate/BSC/EC-Earth3.3/November_startdates'
    mylist = os.scandir(srcdir)
    mydirs = list()
    myfiles = list()
    for item in mylist:
        if os.path.isdir(item):
            mydirs.append(item.name)
        if os.path.isfile(item):
            if item.name.endswith('xml'):
                myfiles.append(item.name)
    # Loop directories, check base xml for parent id
    i = 0
    for item in mydirs:
        print('Processing directory: ', item)
        parentfile = list(filter(lambda x: item in x, myfiles))[0]
        print('Collecting parent id from: ', parentfile)
        try:
            myid = retrieve_identifier('/'.join([srcdir, parentfile]))
        except Exception as e:
            print('Couldn\'t parse file')
            raise
        print('Parent identifier to insert: ', myid)

        # Traverse folders and check parent identifiers
        for root, dirs, files in os.walk('/'.join([srcdir, item])):
            #path = root.split(os.sep)
            #print((len(path) - 1) * '-', os.path.basename(root))
            for myfile in files:
                if not myfile.endswith('xml'):
                    continue
                print('\t', myfile, ' - ', myid)
                try:
                    add_parent('/'.join([root, myfile]), myid)
                except Exception as e:
                    print('Something failed processing ', '/'.join([root,myfile]))
                    raise
                i += 1
    print('Total number of files processed: ', i)

if __name__ == '__main__':
    main()

