"""
PURPOSE:
    Reading title and the time of the last metadata update of the metadata
    file, this software generates a UUID for the dataset.

AUTHOR:
    Øystein Godøy, METNO/FOU, 2016-06-08

UPDATED:
    Øystein Godøy, METNO/FOU, 2018-09-26 
        Added support for addition of metadata_identifier
        create if missing, change if present
    Øystein Godøy, METNO/FOU, 2018-04-14
        Moved from solrindexing to mdharvest.
    Mortenwh
        Refactored to have a method and an executable

"""
import sys
import os
import uuid
import datetime
import xml.etree.ElementTree as ET

def create_MET_uuid(infile, overwrite=False):
   # Parse the XML file
   tree = ET.parse(infile)
   ET.register_namespace('mmd','http://www.met.no/schema/mmd')
   ET.register_namespace('gml','http://www.opengid.net/gml')
   root = tree.getroot()
   try:
       mytitle = tree.find('{http://www.met.no/schema/mmd}title').text
   except:
       print ("title is missing from metadata file")
       sys.exit(2)
   try:
       mylastupdate = tree.find('{http://www.met.no/schema/mmd}last_metadata_update').text
   except:
       print ("last_metadata_update is missing from metadata file")
       sys.exit(2)

   # Get the time of creation for the metadatafile
   #dstime = os.path.getctime(infile)

   # Prepare creation of UUID
   filename = "https://arcticdata.met.no/ds/"+os.path.basename(infile)+"-"
   #filename += datetime.datetime.utcfromtimestamp(dstime).strftime("%Y%m%dT%H%M%S")
   filename += mylastupdate

   # Create UUID
   myidentifier = uuid.uuid5(uuid.NAMESPACE_URL,filename)
   if overwrite == False:
       print (myidentifier)
       sys.exit(0)

   # Overwrite metadata_identifier in input file
   try:
       myexistingid = root.find('{http://www.met.no/schema/mmd}metadata_identifier')
       myexistingid.text = str(myidentifier)
   except:
       print ("No metadata identifier was found in this file, creating this...")
       myid = ET.Element('mmd:metadata_identifier')
       myid.text = str(myidentifier)
       root.insert(0, myid)

   tree.write(infile,
           xml_declaration=True,encoding='UTF-8',
           method="xml")
