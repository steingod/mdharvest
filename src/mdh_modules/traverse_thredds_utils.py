"""
PURPOSE:
    Utility functions used by traverse_thredds in the parent folder.

AUTHOR:
    Øystein Godøy, METNO/FOU, 2024-09-09 

UPDATED:
    Øystein Godøy, METNO/FOU, 2024-09-09 
        Moved to module

NOTES:

"""

import os
import threddsclient
from mdh_modules.nc_to_mmd import Nc_to_mmd
import vocab.ControlledVocabulary
import vocab.CFGCMD
import lxml.etree as ET
import uuid
import re
#from datetime import datetime
#import pytz

def sanitize_filename(filename):
    """
    Replace non-characters in filename
    """
    if filename.endswith(".nc"):
        name = filename.split('.nc')[0]
    else:
        name = filename

    sanitized_filename = re.sub(r'[^a-zA-z0-9]','_', name)

    return sanitized_filename

def traverse_thredds(mystart, dstdir, mydepth, mylog):
    mylog.info('Traversing: %s to depth %d', mystart, mydepth)
    #mystart = 'https://thredds.met.no/thredds/arcticdata/arcticdata.xml'
    #print(mystart)
    ns_map = {'mmd': "http://www.met.no/schema/mmd",
                  'gml': "http://www.opengis.net/gml"}
    if '?' in mystart:
        mydir = mystart.split('?')[0].replace('catalog.html','')
    else:
        mydir = mystart.replace('catalog.html','')
    #print('>>>', mystart)
    for ds in threddsclient.crawl(mystart, depth=mydepth):
        mylog.info('Processing:\n\t%s', ds.name)
        #print('\tLanding page:',ds.url,sep='\n\t\t')
        #print('\tDownload URL:', ds.download_url(),sep='\n\t\t')
        #print('\tOPeNDAP URL:', ds.opendap_url(),sep='\n\t\t')
        #print('\tOGC WMS URL:', ds.wms_url(),sep='\n\t\t')
        #mypath = ds.url.replace(mystart.replace('catalog.html',''),'').split('?')[0]
        if 'catalog.xml' in ds.url:
            mypath = (ds.url.split('?')[0].replace('catalog.xml','')).replace(mydir,'')
        else:
            mypath = (ds.url.split('?')[-1])
        mypath = mypath.replace('dataset=','')
        mypath = re.sub('https?://*.*.*/catalog/','',mypath)
        newdstdir = os.path.join(dstdir,mypath)

        #Special handling for specific URL
        if 'thredds.niva.no' in mystart:
            #Extract common sub-sub path segments
            handle = (ds.url.split('subcatalogs/')[-1])
            common_path = (handle.split('.')[0])
            newdstdir = os.path.join(dstdir, common_path)
        else:
            newdstdir = os.path.join(dstdir, mypath)

        #print('>>>',newdstdir)
        # Make more robust...
        if not os.path.exists(newdstdir):
            os.makedirs(newdstdir)
        infile = ds.opendap_url()
        sanitized_name = sanitize_filename(ds.name)
        outfile = os.path.splitext(sanitized_name)[0]+'.xml'
        #print('>>>', infile)
        if not infile.lower().endswith(('.nc','.ncml')):
            mylog.info('No NCML or NetCDF file, skipping parsing...')
            continue
        #print('>>>', outfile)
        #print('>>>',ds)
        #print('>>>',ds.url)
        try:
            md = Nc_to_mmd(dstdir, outfile, infile, vocab, False, False, False)
        except Exception as e:
            mylog.warning('Something failed setting up ACDD extraction', e)
            continue
        #print('####',md.output_name)
        #print('####',md.netcdf_product)
        try:
            myxml = md.to_mmd()
        except Exception as e:
            mylog.warning('Something failed when extracting MMD elements: %s', e)
            continue

        if myxml is None:
            continue
        # Modify the XML generated with information from THREDDS
        #print('Parsing XML')
        #myxml = ET.parse(os.path.join(dstdir,outfile))
        myroot = myxml.getroot()
        # Check and potentially modify identifier
        mynode = myxml.find("./mmd:metadata_identifier", myroot.nsmap)
        #print(mynode.text, ds.url.replace('catalog.xml?dataset=',''))
        # If ID is not a UUID, replace with a newly generated UUID
        # Check if UUID is prefixed by namespace
        # Everything below is focused at the ADC operation, so no.met.adc is the preferred prefix
        if mynode is not None:
            try:
                uuidver = uuid.UUID(mynode.text).version
            except ValueError:
                mylog.warning("\tNot containing a straight UUID, checks further.")
                # Checking if containing UUID with namespace
                if any((c in set('.:')) for c in mynode.text):
                    if ':' in mynode.text:
                        mystr = mynode.text.split(':')[-1]
                        try:
                            uuidver = uuid.UUID(mystr).version
                        except ValueError:
                            mylog.warning('Does not recognise {} as a valid identifier'.format(mystr))
                            # Check if a DOI is used and use this elsewhere
                            if 'doi.org' in mystr:
                                print('\tThis is a doi, putting it in data_citation')
                                mycit = myxml.find("./mmd:data_citation", myroot.nsmap)
                                if mycit:
                                    print("\tThe dataset_citation field is already populated, bailing out.")
                                else:
                                    mynewnode = ET.Element("{http://www.met.no/schema/mmd}dataset_citation")
                                    mydoi = ET.SubElement(mynewnode, "{http://www.met.no/schema/mmd}doi")
                                    mydoi.text = mynode.text
                                    myroot.insert(10,mynewnode)
                            # Not sure if this section is failsafe, lacking an else?
                            # Generate new identifier
                            try:
                                mynode.text = str(uuid.uuid5(uuid.NAMESPACE_URL,
                                    ds.url.replace('catalog.xml?dataset=','')))
                            except TypeError as e:
                                print(e)
                        print('\tContains identifier with probable namespace prefixed UUID')
                    elif '.' in mynode.text:
                        # FIXME
                        # Data from GEUS is handled specifically for now, accepting their identifiers even if not uuids
                        if 'dk.geus' in mynode.text:
                            print('Keeping GEUS identifiers, not sure this work...')
                        else:
                            # This is normally caused by filenames being used as identifiers. These have less probability of being unique and are replaced by UUIDs, with MET namespaces if hosted by MET.
                            mystr = mynode.text.split('.')[-1]
                            if "met.no" in ds.url:
                                idnamespace = "no.met.adc:"
                            elif "nersc.no" in ds.url:
                                idnamespace = "no.nersc:"
                            else:
                                idnamespace = ""
                            try:
                                mynode.text = idnamespace+str(uuid.uuid5(uuid.NAMESPACE_URL,
                                    ds.url.replace('catalog.xml?dataset=','')))
                            except TypeError as e:
                                print(e)
                else:
                    # FIXME not entirely sure when we end up here
                    # This is often caused by filenames without suffix being used as identifiers. Same as above, these are likely to be compromised and are replaced by UUIDs.
                    if "met.no" in ds.url:
                        idnamespace = "no.met.adc:"
                    elif "nersc.no" in ds.url:
                        idnamespace = "no.nersc:"
                    else:
                        idnamespace = ""
                    try:
                        mynode.text = idnamespace+str(uuid.uuid5(uuid.NAMESPACE_URL,
                            ds.url.replace('catalog.xml?dataset=','')))
                    except TypeError as e:
                        print(e)
        else:
            try:
                mynode = ET.Element("{http://www.met.no/schema/mmd}metadata_identifier")
                mynode.text = 'no.met.adc:'+str(uuid.uuid5(uuid.NAMESPACE_URL,
                    ds.url.replace('catalog.xml?dataset=','')))
            except TypeError as e:
                print(e)
            try:
                myroot.insert(0, mynode)
            except Exception as e:
                print(e)

        # Add metadata_status
        # Already done, removing
        #mynode = ET.Element("{http://www.met.no/schema/mmd}metadata_status")
        #mynode.text = 'Active'
        #myroot.insert(4, mynode)

        # Add and update last_metdata_update
        # Removed for now since covered by nc_to_mmd
##        mynode = ET.Element("{http://www.met.no/schema/mmd}last_metadata_update")
##        mychild = ET.SubElement(mynode,"{http://www.met.no/schema/mmd}update")
##        mygchild1 = ET.SubElement(mychild,"{http://www.met.no/schema/mmd}datetime")
##        mygchild1.text = datetime.now(tz=pytz.UTC).strftime('%Y-%m-%dT%H:%M:%S%Z')
##        mygchild2 = ET.SubElement(mychild,"{http://www.met.no/schema/mmd}type")
##        mygchild2.text = 'Created'
##        mygchild3 = ET.SubElement(mychild,"{http://www.met.no/schema/mmd}note")
##        mygchild3.text = 'Created automatically from traversing THREDDS server'
##        myroot.insert(5,mynode)

        # Add collection after ds production status
        dsstatus = myxml.find("./mmd:dataset_production_status",namespaces=myroot.nsmap)
        tags = []
        for collection in myxml.findall("./mmd:collection",namespaces=myroot.nsmap):
            tags.append(collection.text)
        if 'NSDN' not in tags:
            mynodensdn = ET.Element("{http://www.met.no/schema/mmd}collection")
            mynodensdn.text = 'NSDN'
            dsstatus.addnext(mynodensdn)
        if 'ADC' not in tags:
            mynodeadc = ET.Element("{http://www.met.no/schema/mmd}collection")
            mynodeadc.text = 'ADC'
            dsstatus.addnext(mynodeadc)


        # Check and potentially modify activity_type
        mynode = myxml.find("./mmd:activity_type",namespaces=myroot.nsmap)
        if mynode is None:
            mynode = ET.Element("{http://www.met.no/schema/mmd}activity_type")
            mynode.text = 'Not available'
            myroot.insert(-1, mynode)

        # Check and potentially modify operational_status
        mynode = myxml.find("./mmd:operational_status",namespaces=myroot.nsmap)
        if mynode is None:
            mynode = ET.Element("{http://www.met.no/schema/mmd}operational_status")
            mynode.text = 'Not available'
            myroot.insert(-1, mynode)

        # Add related_information
        related_information = ET.Element(
                "{http://www.met.no/schema/mmd}related_information")
        related_information_type = ET.SubElement(related_information,
                '{http://www.met.no/schema/mmd}type')
        related_information_type.text = 'Dataset landing page'
        related_information_description = ET.SubElement(related_information,
                '{http://www.met.no/schema/mmd}description')
        related_information_description.text = 'Dataset landing page'
        related_information_resource = ET.SubElement(related_information,
                '{http://www.met.no/schema/mmd}resource')
        related_information_resource.text = ds.url.replace('.xml','.html')
        myroot.insert(-1,related_information)

        # Add data_access (not done automatically)
        if ds.download_url():
            data_access = ET.Element(
                    '{http://www.met.no/schema/mmd}data_access')
            data_access_type = ET.SubElement(data_access,
                    '{http://www.met.no/schema/mmd}type')
            data_access_type.text = 'HTTP'
            data_access_description = ET.SubElement(data_access,
                    '{http://www.met.no/schema/mmd}description')
            data_access_description.text = 'Direct download of datafile'
            data_access_resource = ET.SubElement(data_access,
                    '{http://www.met.no/schema/mmd}resource')
            data_access_resource.text = ds.download_url()
            myroot.insert(-1,data_access)

        if ds.opendap_url():
            data_access = ET.Element(
                    '{http://www.met.no/schema/mmd}data_access')
            data_access_type = ET.SubElement(data_access,
                    '{http://www.met.no/schema/mmd}type')
            data_access_type.text = 'OPeNDAP'
            data_access_description = ET.SubElement(data_access,
                    '{http://www.met.no/schema/mmd}description')
            data_access_description.text = 'OPeNDAP access to dataset'
            data_access_resource = ET.SubElement(data_access,
                    '{http://www.met.no/schema/mmd}resource')
            data_access_resource.text = ds.opendap_url()
            myroot.insert(-1,data_access)

        if ds.wms_url():
            data_access = ET.Element(
                    '{http://www.met.no/schema/mmd}data_access')
            data_access_type = ET.SubElement(data_access,
                    '{http://www.met.no/schema/mmd}type')
            data_access_type.text = 'OGC WMS'
            data_access_description = ET.SubElement(data_access,
                    '{http://www.met.no/schema/mmd}description')
            data_access_description.text = 'OGC WMS GetCapabilities URL'
            data_access_resource = ET.SubElement(data_access,
                    '{http://www.met.no/schema/mmd}resource')
            data_access_resource.text = ds.wms_url()
            myroot.insert(-1,data_access)

        # Reference should be removed
        # dataset citation has to be further improved...

        # Create new file
        myxml.write(os.path.join(newdstdir,outfile), pretty_print=True)
