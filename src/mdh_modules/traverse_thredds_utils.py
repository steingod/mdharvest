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
import sys
import threddsclient
from mdh_modules.nc_to_mmd import Nc_to_mmd
import vocab.ControlledVocabulary
import vocab.CFGCMD
import vocab.KEYWORDS
import lxml.etree as ET
import uuid
import re
from datetime import datetime
from dateutil import parser
import pytz

def sanitize_filename(filename):
    """
    Replace non-characters in filename
    """
    if filename.endswith(".nc"):
        name = filename.split('.nc')[0]
    elif filename.endswith(".nc4"):
        name = filename.split('.nc4')[0]
    else:
        name = filename

    sanitized_filename = re.sub(r'[^a-zA-z0-9]','_', name)

    return sanitized_filename

def clean_hyrax_urls(myurl):
    """
    Clean up dataset landing page URLs for HYRAX servers
    """
    #tmpurl = re.sub(r'catalog.xml\?dataset=/opendap/hyrax/', '', myurl)
    tmplist = myurl.split('?')
    tmpurl = re.sub(r'/catalog.xml', '', tmplist[0])
    for x in re.sub(r'dataset=', '', tmplist[1]).split('/'):
        if x not in tmplist[0]:
            tmpurl = '/'.join([tmpurl,x]) 

    return tmpurl

def traverse_thredds(mystart, dstdir, mydepth, mylog, force_mmd=None):
    """
    Actual traversing of THREDDS catalogues for generation of MMD files.
    """

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
    myparentid = None
    epochroot = datetime(1970,1,1)
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
        # Works well for THREDDS, but not HYRAX
        mypath = re.sub(r'https?://(\S*\.){1,2}\.\S*/catalog/','',mypath)
        # Handling HYRAX, will not do anything if not a HYRAX server
        mypath = re.sub(r'https?://(\S*\.){2}','',mypath)
        mypath = re.sub(r'\S{2}/opendap/','',mypath)
        #newdstdir = os.path.join(dstdir,mypath) # not needed I think

        # Special handling for specific provider (NIVA)
        # FIXME check if needed onwards
        if 'thredds.niva.no' in mystart:
            #Extract common sub-sub path segments
            handle = (ds.url.split('subcatalogs/')[-1])
            common_path = (handle.split('.')[0])
            newdstdir = os.path.join(dstdir, common_path)
        else:
            newdstdir = os.path.join(dstdir, mypath)

        # Check if destination directory exist, create if not
        # FIXME Make more robust...
        if not os.path.exists(newdstdir):
            os.makedirs(newdstdir)
        # Check input filename
        infile = ds.opendap_url()
        if not infile:
            mylog.info("%s is not a proper filename, skipping parsing...", infile)
            continue
        if not infile.lower().endswith(('.nc','.nc4','.ncml')):
            mylog.info('No NCML or NetCDF file, skipping parsing...')
            continue
        # Create output filename
        sanitized_name = sanitize_filename(ds.name)
        outfile = os.path.splitext(sanitized_name)[0]+'.xml'
        # Check if this file already exist 
        # Need to update MMD if source (NetCDF or NCML) has been updated since the last run
        if not force_mmd:
            if os.path.isfile('/'.join([newdstdir,outfile])):
                # Check if modified is available, if not create MMD anyway
                if ds.modified != None:
                    # Check if the source file (NetCDF or NCML) has been updated after the MMD was generated
                    tmptime = parser.parse(ds.modified)
                    dsmodtime = tmptime.timestamp()
                    mmdmodtime = os.path.getmtime('/'.join([newdstdir,outfile]))
                    mylog.info("dst %d - %d", mmdmodtime, dsmodtime)
                    if dsmodtime > mmdmodtime:
                        mylog.info("%s is updated after the last MMD generation, updating MMD", infile)
                    else:
                        mylog.info("%s exists and nothing new has happened, skipping now", outfile)
                        continue

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
        # Assumes that the NCML identifier is valid for all files until the next ncml is found
        if infile.lower().endswith('.ncml'):
            myparentid = md.identifier
        print('########################################################')
        print('####',myparentid)
        # Modify the XML generated with information from THREDDS
        #print('Parsing XML')
        #myxml = ET.parse(os.path.join(dstdir,outfile))
        myroot = myxml.getroot()
        # Add parent record if record is deemed child
        if myparentid:
            myrelatedds = myxml.find("./mmd:metadata_identifier", myroot.nsmap)
            if infile.lower().endswith(('.nc','.nc4')) and not myrelatedds:
                print('assumes file to be child')
                myreldata = ET.Element("{http://www.met.no/schema/mmd}related_dataset")
                myreldata.set('relation_type','parent')
                myreldata.text = myparentid
                myroot.append(myreldata)

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
        # Covered by nc_to_mmd

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

        # Add related_information landing page. All records with no.met.adc prefix in the
        # metadata indetifier will have landing pages of type https://adc.met.no/dataset/id
        metadata_identifier = myxml.find("./mmd:metadata_identifier", myroot.nsmap).text
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
        if 'no.met.adc' in metadata_identifier:
            related_information_resource.text = 'https://adc.met.no/dataset/' + metadata_identifier.split('no.met.adc:')[-1]
        else:
            related_information_resource.text = ds.url.replace('.xml','.html')
        myroot.insert(-1,related_information)

        # also add related_information Data server landing page
        related_information = ET.Element(
                '{http://www.met.no/schema/mmd}related_information')
        related_information_type = ET.SubElement(related_information,
                '{http://www.met.no/schema/mmd}type')
        related_information_type.text = 'Data server landing page'
        related_information_description = ET.SubElement(related_information,
                '{http://www.met.no/schema/mmd}description')
        related_information_description.text = 'Access to the data server landing page'
        related_information_resource = ET.SubElement(related_information,
                '{http://www.met.no/schema/mmd}resource')
        if "thredds" in ds.url:
            # Works in THREDDS servers
            related_information_resource.text = ds.url.replace('.xml','.html')
        elif "opendap" in ds.url:
            # Works on HYRAX servers
            tmpurl = clean_hyrax_urls(ds.url)
            related_information_resource.text = '.'.join([tmpurl,'html'])
        myroot.insert(-1,related_information)

        # Add data_access (not done automatically)
        if ds.download_url() and not infile.lower().endswith('.ncml'):
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

def check_mmd_tree(tdsdir, mylog, dry_run):
    """
    Traverses the tree with generated MMD files and checks that each child has a parent. It can be used in folders containing MMD files generated by other processes as well. It is assumed that the parent files are located at the top level in each folder, instances can be at the same level or sorted into sub folders.
    """

    # Strings that are found in parents
    parentfiles = [
            'parent',
            'aggregated',
            'main',
            ]

    # Traverse the file tree
    i = 0
    datasetfound = False
    datasetroot = tdsdir
    for root, dirs, files in os.walk(tdsdir):
        mylog.info('Processing folder: %s', root)

        # First check if there are parent files in the folder
        if not datasetfound or datasetroot not in root:
            myparentfile = identify_parent(parentfiles, files, mylog)
            if myparentfile == None:
                mylog.info('No parent found in this folder, continues to next top level folder.')
                datasetfound = False
                continue
            # Get the identifier of the parent
            # Should add a check on error in parent files...
            parentid = get_parent_id('/'.join([root,myparentfile]), mylog)
            datasetfound = True
            datasetroot = root

        # Continuing to check all files in this and subsequent folders for reference to parent
        mylog.info('Identifier to be added: %s', parentid)
        # Checking all files at each level
        mylog.info('Checking files in: %s', root)
        if files is not None:
            for f in sorted(files):
                if f == myparentfile:
                    mylog.info('Parent file, continues...')
                    continue
                try: 
                    check_mmd4parent('/'.join([root,f]), parentid, mylog, dry_run)
                except Exception as e:
                    mylog.warn('Something went wrong in check_mmd4parent')
                    continue

def identify_parent(skey, filelist, mylog):
    '''
    Identify the parent file in each folder structure.
    '''
    mylog.info('Searching for parent file')

    parentexist = False
    myparent = None

    for f in filelist:
        for k in skey:
            if k in f:
                mylog.info('Found a parent: %s', f)
                myparent = f
                parentexist = True
                break
        if parentexist:
            break

    return(myparent)

def get_parent_id(myfile, mylog):
    '''
    Return the id, or return None if none id is found.
    '''
    mylog.info('Retrieve the identifier from the parent file: %s', myfile)

    myid = None

    # Parse the XML
    try:
        myxml = ET.parse(myfile)
    except Exception as e:
        mylog.warn('Could not properly parse: %s', myfile)
    myroot = myxml.getroot()
    try: 
        myid = myroot.find('mmd:metadata_identifier', namespaces=myroot.nsmap).text
    except Exception as e:
        mylog.info('No id provided: %s', e)

    return(myid)

def check_mmd4parent(myfile, myparent, mylog, dry_run):
    """
    Check the content of MMD file for presence of parent identifier
    """
    mylog.info('Checking %s', myfile)

    myid = None

    try:
        myxml = ET.parse(myfile)
    except Exception as e:
        mylog.warn('Could not properly parse: %s', myfile)
        myxml.warn(e)
    myroot = myxml.getroot()
    if myroot.find("mmd:related_dataset[@relation_type='parent']", namespaces=myroot.nsmap) is not None:
        # Need to add a check that correct parent is used...
        myid = myroot.find("mmd:related_dataset[@relation_type='parent']", namespaces=myroot.nsmap).text
        if myid != myparent:
            mylog.warn('Inconsistent relationship found.')
            return
        else:
            mylog.info('Nothing to do, parent/child relations are established.')
            return
    else:
        mylog.info('No reference to parent found in this document.')
        try:
            mynewxml = add_parent2mmd(myroot, myparent, mylog)
        except Exception as e:
            mylog.warn('Could not add identifier to document.')
    if not dry_run:
        try:
            #myxml.write('-'.join([myfile,'new']))
            myxml.write(myfile)
        except Exception as e:
            mylog.error('Could not write new content to: %s', myfile)
    else:
        mylog.info('This is a dry run, no editing of files done.')

def add_parent2mmd(myxml, myparent, mylog):
    mylog.info('Adding parent identifier to file')

    # Create new element
    try:
        mynewel = ET.SubElement(myxml, "{http://www.met.no/schema/mmd}related_dataset")
    except Exception as e:
        mylog.error('Something did not work adding element: %s', e)
    mynewel.text = myparent
    mynewel.set('relation_type','parent')
