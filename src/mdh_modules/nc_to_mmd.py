""" 
Script for parsing metadata content of NetCDF files and create a MET Norway
Metadata format specification document (MMD) based on the discovery metadata .

Will work on CF and ACDD compliant files.

Author(s):     Trygve Halsne, Øystein Godøy
Created:       2019-11-25 (YYYY-mm-dd)
Modifications: 2021-11-30 (major rewrite)
Copyright:     (c) Norwegian Meteorological Institute, 2019

"""
from pathlib import Path
from netCDF4 import Dataset
import lxml.etree as ET
import datetime as dt
from dateutil.parser import parse
import sys
import os
import re
import validators
import urllib.request as ul
import json

class Nc_to_mmd(object):

    def __init__(self, output_path, output_name, netcdf_product,
            parse_services=False, parse_wmslayers=False, print_file=False):
        """
        Class for creating an MMD XML file based on the discovery metadata provided in the global attributes of NetCDF
        files that are compliant with the CF-conventions and ACDD.

        Args:
            output_path (str): Output path for mmd.
            output_name (str): Output name for mmd.
            netcdf_product (str: nc file or OPeNDAP url): input NetCDF file.

        """
        super(Nc_to_mmd, self).__init__()
        if output_path.endswith('/'):
            self.output_path = output_path
        else:
            self.output_path = output_path+'/'
        self.output_name = output_name
        self.netcdf_product = netcdf_product
        self.parse_services = parse_services
        self.parse_wmslayers = parse_wmslayers
        self.print_file = print_file

    def to_mmd(self):
        """
        Method for parsing content of NetCDF file, mapping discovery
        metadata to MMD, and writes MMD to disk.
        """

        try:
            ncin = Dataset(self.netcdf_product)
        except Exception as e:
            print('Couldn\'t open file:', self.netcdf_product)
            print('Error: ',e)
            return

        global_attributes = ncin.ncattrs()
        all_netcdf_variables = [var for var in ncin.variables]

        # Create XML file with namespaces
        ns_map = {'mmd': "http://www.met.no/schema/mmd",
                  'xml': "http://www.w3.org/XML/1998/namespace"}
                 # 'gml': "http://www.opengis.net/gml"}
        root = ET.Element(ET.QName(ns_map['mmd'], 'mmd'), nsmap=ns_map)
        #root = ET.Element('mmd', nsmap=ns_map)

        # Write MMD elements from global attributes in NetCDF following a KISS approach starting with the required MMD element and looking for the ACDD element to parse. 

        # Extract metadata identifier. This relies on the ACDD attributes id and naming_authority
        if 'id' in global_attributes:
            self.add_identifier(root, ns_map, ncin, global_attributes)

        # Create metadata status. Default is active. Done above, rewrite... 
        self.add_metadata_status(root, ns_map)

        # Extract last metadata update. Multiple elements to process. Check both date_created and date_metadata_modified
        if 'date_created' in global_attributes:
            self.add_last_metadata_update(root, ns_map, ncin, global_attributes)
        else:
            myel = ET.SubElement(root,ET.QName(ns_map['mmd'],'last_metadata_update'))
            myel2 = ET.SubElement(myel,ET.QName(ns_map['mmd'],'update'))
            ET.SubElement(myel2,
                    ET.QName(ns_map['mmd'],'datetime')).text = dt.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
            ET.SubElement(myel2,ET.QName(ns_map['mmd'],'type')).text = 'Created'
            ET.SubElement(myel2,ET.QName(ns_map['mmd'],'note')).text = 'Automatically generated from ACDD elements'

        # Extract collection?? Not supported by ACDD, add afterwards using filter records from harvester. To be reconsidered in the future.

        # Extract title
        if 'title' in global_attributes:
            self.add_title(root, ns_map, ncin)

        # Extract abstract
        if 'summary' in global_attributes:
            self.add_abstract(root, ns_map, ncin)

        # Extract geographic extent
        if 'geospatial_lat_max' in global_attributes and 'geospatial_lat_min' in global_attributes and 'geospatial_lon_max' in global_attributes and 'geospatial_lon_min' in global_attributes:
            self.add_geographic_extent(root, ns_map, ncin, global_attributes)

        # Extract temporal extent
        if 'time_coverage_start' in global_attributes and 'time_coverage_end' in global_attributes:
            self.add_temporal_extent(root, ns_map, ncin)

        # Extract personnel. Need to extract multiple fields.
        self.add_personnel(root, ns_map, ncin, global_attributes)

        # Extract data centre. Need to extract multiple fields.
        if 'publisher_name' in global_attributes:
            self.add_data_centre(root, ns_map, ncin, global_attributes)

        # Extract use constraint
        if 'license' in global_attributes:
            self.add_use_constraint(root, ns_map, ncin)

        # Extract keywords, need to parse both keywords and keywords_vocabulary
        if 'keywords' in global_attributes:
            self.add_keywords(root,ns_map,ncin, global_attributes)

        # Extract project
        if 'project' in global_attributes:
            self.add_project(root, ns_map, ncin)

        # Extract platform
        if 'platform' in global_attributes:
            self.add_platform(root, ns_map, ncin, global_attributes)
            
        # Add related_information. There is currently no relevant ACDD element to process here.

        # Add related_dataset. There is currently no relevant ACDD element to process here.

        # Extract dataset citation FIXME
        if 'references' in global_attributes:
            self.add_dataset_citation(root, ns_map, ncin)

        # Extract data access??
        # Do not add here, handle this in traversing THREDDS catalogs

        # Extract related information?? This is not directly supported in ACDD.

        # Extract activity type. Relying on the ACDD element source or local extension
        if 'source' in global_attributes:
            self.add_activity_type(root, ns_map, ncin, global_attributes)

        # Extract WIGOS links
        if 'wigosId' in global_attributes:
            self.add_wigos_related_info(root, ns_map, ncin, global_attributes)

        # Extract ISO topic category

        # Set dataset production status?? Not supported by ACDD

        # Set operational status. This need specific care and checking for compliance
        if 'processing_level' in global_attributes or 'operational_status' in global_attributes:
            self.add_operational_status(root, ns_map, ncin, global_attributes)

        # Set dataset_production_status. This is not supported by ACDD and is left empty for now.

        # Check if services should be parsed
        if self.parse_services:
            self.add_web_services(root, ns_map)

        #print(ET.tostring(root, pretty_print=True).decode())
        #sys.exit()

        if not self.output_name.endswith('.xml'):
            output_file = str(self.output_path + self.output_name) + '.xml'
        else:
            output_file = str(self.output_path + self.output_name)

        et = ET.ElementTree(root)
        #et = ET.ElementTree(ET.fromstring(ET.tostring(root, pretty_print=True).decode("utf-8")))

        # Printing to file is optional
        if self.print_file:
            et.write(output_file, pretty_print=True)
        else:
            return(et)
        return

    # Relying on ACDD elements id and naming_authority
    # Implementation of naming_authority is pending FIXME
    def add_identifier(self, myxmltree, mynsmap, ncin, myattrs):
        myid = getattr(ncin, 'id')
        if 'naming_authority' in myattrs:
            mynamaut = getattr(ncin, 'naming_authority')
        else:
            mynamaut = 'None'
        myel = ET.SubElement(myxmltree,ET.QName(mynsmap['mmd'],'metadata_identifier'))
        myel.text = myid

    def add_metadata_status(self, myxmltree, mynsmap):
        ET.SubElement(myxmltree,ET.QName(mynsmap['mmd'],'metadata_status')).text = 'Active'

    # Check both date_created and date_metadata_modified
    # FIXME add multiple updates
    def add_last_metadata_update(self, myxmltree, mynsmap, ncin, myattrs):
        tmpdatetime = getattr(ncin, 'date_created')
        # First try to convert whatever is found
        mydatetime = parse(tmpdatetime)
        # Hack that will correct some files while waiting for updated versions
        if mydatetime is None:
            tmpdatetime = tmpdatetime.replace(':Z','Z')
            tmpdate = re.search(r'\d{4}-\d{2}-\d{2}',tmpdatetime)
            if not re.search(r'T\d{2}:\d{2}:\d{2}',tmpdatetime):
                tmptime="T12:00:00Z"
                tmpdatetime = tmpdate.group()+tmptime
        mydatetime = parse(tmpdatetime)
        # Prepare the output
        if 'date_metadata_modified' in myattrs:
            myupdate = getattr(ncin, 'date_metadata_modified')
        else:
            myupdate = None
        myel = ET.SubElement(myxmltree,ET.QName(mynsmap['mmd'],'last_metadata_update'))
        myel2 = ET.SubElement(myel,ET.QName(mynsmap['mmd'],'update'))
        myel3 = ET.SubElement(myel2,ET.QName(mynsmap['mmd'],'datetime'))
        myel3.text = mydatetime.strftime("%Y-%m-%dT%H:%M:%SZ") # FIXME, check datetime format
        myel3 = ET.SubElement(myel2,ET.QName(mynsmap['mmd'],'type'))
        myel3.text = 'Created'
        myel3 = ET.SubElement(myel2,ET.QName(mynsmap['mmd'],'note'))
        myel3.text = 'MMD record created from the NetCDF file'

    # Assuming english as default language
    def add_title(self, myxmltree, mynsmap, ncin):
        mytitle = getattr(ncin, 'title')
        myel = ET.SubElement(myxmltree,ET.QName(mynsmap['mmd'],'title'))
        myel.text = mytitle
        myel.set('{http://www.w3.org/XML/1998/namespace}lang','en')

    # Assuming english as default language
    def add_abstract(self, myxmltree, mynsmap, ncin):
        myabstract = getattr(ncin, 'summary')
        myel = ET.SubElement(myxmltree,ET.QName(mynsmap['mmd'],'abstract'))
        myel.text = myabstract
        myel.set('{http://www.w3.org/XML/1998/namespace}lang','en')

    # Add check and rewrite of longitudes.
    def add_geographic_extent(self, myxmltree, mynsmap, ncin, myattrs):
        mylist = ['geospatial_lat_max','geospatial_lat_min','geospatial_lon_max','geospatial_lon_min']
        # Check if all 4 corners of bounding box is provided
        for el in mylist:
            if el not in myattrs:
                print(el, ' is not found in the global attributes, bailing out')
                return
        # All 4 corners of the bounding box exist, proceeding
        myel = ET.SubElement(myxmltree,ET.QName(mynsmap['mmd'],'geographic_extent'))
        myel2 = ET.SubElement(myel,ET.QName(mynsmap['mmd'],'rectangle'))
        myel2.set('srsName','EPSG:4326')
        for el in mylist:
            mycont = getattr(ncin, el)
            if 'lat_max' in el:
                myel31 = ET.SubElement(myel2,ET.QName(mynsmap['mmd'],'north'))
                myel31.text = str(mycont)
            elif 'lat_min' in el:
                myel32 = ET.SubElement(myel2,ET.QName(mynsmap['mmd'],'south'))
                myel32.text = str(mycont)
            elif 'lon_max' in el:
                myel33 = ET.SubElement(myel2,ET.QName(mynsmap['mmd'],'east'))
                myel33.text = str(mycont)
            elif 'lon_min' in el:
                myel34 = ET.SubElement(myel2,ET.QName(mynsmap['mmd'],'west'))
                myel34.text = str(mycont)

    # Add temporal
    def add_temporal_extent(self, myxmltree, mynsmap, ncin):
        mystarttime = parse(getattr(ncin,'time_coverage_start'))
        myendtime = parse(getattr(ncin,'time_coverage_end'))
        myel = ET.SubElement(myxmltree,ET.QName(mynsmap['mmd'],'temporal_extent'))
        ET.SubElement(myel, ET.QName(mynsmap['mmd'],'start_date')).text = mystarttime.strftime('%Y-%m-%dT%H:%M:%SZ')
        ET.SubElement(myel, ET.QName(mynsmap['mmd'],'end_date')).text = myendtime.strftime('%Y-%m-%dT%H:%M:%SZ')

    # Add personnel, quite complex...
    # Creator is related to Principal investigator, contributor to technical contact and metadata author (no way to differentiate) and pubisher to data centre hosting data
    # FIXME add splitting of elements...
    def add_personnel(self, myxmltree, mynsmap, ncin, myattrs):
        # Not used yet...
        #myels2check = ['creator_name', 'creator_email', 'creator_url', 'creator_type', 'creator_institution',
        #        'contributor_name', 'contributor_role',
        #        'publisher_name', 'publisher_email', 'publisher_url', 'publisher_type', 'publisher_institution',]
        if 'creator_name' in myattrs:
            # Handle PI information

            # Check if element contains a list, if so make sure the same list order is used for matching elements. IGPAS is treated separately
            tmpvar = getattr(ncin, 'creator_name')
            creator_name = []
            if 'Institute of Geophysics, Polish Academy of Sciences' in tmpvar:
                creator_name.append(tmpvar)
            elif "Jean Rabault, using data from Takehiko Nose" in tmpvar:
                creator_name.append(tmpvar)
            else:
                creator_name = tmpvar.split(',')
            if 'creator_email' in myattrs:
                creator_email = getattr(ncin, 'creator_email').split(',')
            if 'creator_institution' in myattrs:
                tmpvar = getattr(ncin, 'creator_institution')
                if "Norwegian Meteorological Institute (MET), using data from the University of Tokyo" in tmpvar:
                    creator_institution = "Norwegian Meteorological Institute"
                else:
                    creator_institution = getattr(ncin, 'creator_institution').split(',')
            nmlen = len(creator_name)
            if 'creator_email' in myattrs:
                emlen = len(creator_email)
            if 'creator_institution' in myattrs:
                inlen = len(creator_institution)
            # Check if in vars()
            if ('creator_email' in vars() and 'creator_institution' in vars()):
                if len(creator_name) != len(creator_email) or len(creator_name) != len(creator_institution):
                    print('Inconsistency in personnel (creator) elements, not adding some of these')
            elif ('creator_email' in vars()):
                if len(creator_name) != len(creator_email):
                    print('Inconsistency in personnel (creator) elements, not adding some of these')
            # Create the XML
            i = 0
            for el in creator_name:
                myel = ET.SubElement(myxmltree, ET.QName(mynsmap['mmd'], 'personnel'))
                ET.SubElement(myel, ET.QName(mynsmap['mmd'], 'name')).text = el.strip()
                ET.SubElement(myel, ET.QName(mynsmap['mmd'], 'role')).text = 'Investigator'
                if 'creator_email' in myattrs:
                    if i > emlen-1:
                        ET.SubElement(myel, ET.QName(mynsmap['mmd'], 'email')).text = creator_email[emlen-1].strip()
                    else:
                        ET.SubElement(myel, ET.QName(mynsmap['mmd'], 'email')).text = creator_email[i].strip()
                if 'creator_institution' in myattrs:
                    if i > inlen-1:
                        ET.SubElement(myel, ET.QName(mynsmap['mmd'], 'organisation')).text = creator_institution[inlen-1].strip()
                    else:
                        ET.SubElement(myel, ET.QName(mynsmap['mmd'], 'organisation')).text = creator_institution[i].strip()
                i+=1
        if 'contributor_name' in myattrs:
            # Handle technical contact
            # Check if element contains a list, if so make sure the same list order is used for macthing elements
            contributor_name = getattr(ncin, 'contributor_name').split(',')
            if 'contributor_email' in myattrs:
                contributor_email = getattr(ncin, 'contributor_email').split(',')
            if 'contributor_institution' in myattrs:
                contributor_institution = getattr(ncin, 'contributor_institution').split(',')
            # Check if in vars()
            if ('contributor_email' in vars() and 'contributor_institution' in vars()):
                if len(contributor_name) != len(contributor_email) or len(contributor_name) != len(contributor_institution):
                    print('Inconsistency in personnel (contributor) elements, not adding some of these')
            elif ('contributor_email' in vars()):
                if len(contributor_name) != len(contributor_email) != len(contributor_institution):
                    print('Inconsistency in personnel (contributor) elements, not adding some of these')
            # Create the XML
            i = 0
            for el in contributor_name:
                myel = ET.SubElement(myxmltree, ET.QName(mynsmap['mmd'], 'personnel'))
                ET.SubElement(myel, ET.QName(mynsmap['mmd'], 'name')).text = el.strip()
                ET.SubElement(myel, ET.QName(mynsmap['mmd'], 'role')).text = 'Technical contact'
                if 'contributor_email' in myattrs:
                    ET.SubElement(myel, ET.QName(mynsmap['mmd'], 'email')).text = contributor_email[i].strip()
                if 'contributor_institution' in myattrs:
                    ET.SubElement(myel, ET.QName(mynsmap['mmd'], 'organisation')).text = contributor_institution[i].strip()
                i+=1
        if 'publisher_name' in myattrs:
            # Handle data center personnel
            # Check if element contains a list, if so make sure the same list order is used for macthing elements
            nmlen = inlen = emlen = 0
            publisher_name = getattr(ncin, 'publisher_name').split(',')
            nmlen = len(publisher_name)
            if 'publisher_email' in myattrs:
                publisher_email = getattr(ncin, 'publisher_email').split(',')
                emlen = len(publisher_email)
            if 'publisher_institution' in myattrs:
                publisher_institution = getattr(ncin, 'publisher_institution').split(',')
                inlen = len(publisher_institution)
            # Check if in vars()
            if ('publisher_email' in vars() and 'publisher_institution' in vars()):
                if len(publisher_name) != len(publisher_email) or len(publisher_name) != len(publisher_institution):
                    print('Inconsistency in personnel (publisher) elements, not adding some of these')
            elif ('publisher_email' in vars()):
                if len(publisher_name) != len(publisher_email):
                    print('Inconsistency in personnel (publisher) elements, not adding some of these')
            # Create the XML
            i = 0
            for el in publisher_name:
                myel = ET.SubElement(myxmltree, ET.QName(mynsmap['mmd'], 'personnel'))
                ET.SubElement(myel, ET.QName(mynsmap['mmd'], 'name')).text = el.strip()
                ET.SubElement(myel, ET.QName(mynsmap['mmd'], 'role')).text = 'Data center contact'
                if 'publisher_email' in myattrs:
                    if i > emlen-1:
                        ET.SubElement(myel, ET.QName(mynsmap['mmd'], 'email')).text = publisher_email[emlen-1].strip()
                    else:
                        ET.SubElement(myel, ET.QName(mynsmap['mmd'], 'email')).text = publisher_email[i].strip()
                if 'publisher_institution' in myattrs:
                    if i > inlen-1:
                        ET.SubElement(myel, ET.QName(mynsmap['mmd'], 'organisation')).text = publisher_institution[inlen-1].strip()
                    else:
                        ET.SubElement(myel, ET.QName(mynsmap['mmd'], 'organisation')).text = publisher_institution[i].strip()
                i+=1

    # Add data centre
    def add_data_centre(self, myxmltree, mynsmap, ncin, myattrs):
        myel = ET.SubElement(myxmltree, ET.QName(mynsmap['mmd'], 'data_center'))
        if 'publisher_institution' in myattrs:
            myel2 = ET.SubElement(myel, ET.QName(mynsmap['mmd'], 'data_center_name'))
            ET.SubElement(myel2, ET.QName(mynsmap['mmd'], 'long_name')).text = getattr(ncin, 'publisher_institution')
            ET.SubElement(myel2, ET.QName(mynsmap['mmd'], 'short_name')).text = ''
        if 'publisher_url' in myattrs:
            ET.SubElement(myel, ET.QName(mynsmap['mmd'], 'data_center_url')).text = getattr(ncin, 'publisher_url')

    # Assuming either Free or None or a combination of a URL with an identifier included in parantheses. MMD relies on the SPDX approach of licenses with a n identifier and a resource (URL) for the lcense. License_text is supported to handle free text approaches.
    # Add lookup on identifier in SPDX. spdx-lookup only returns text seemingly, not the URL
    def add_use_constraint(self, myxmltree, mynsmap, ncin):
        myurl = None
        mylicense = getattr(ncin, 'license')
        #print(mylicense)
        # If formatted appropriately, extract information to MMD element.
        if '(' in mylicense and ')' in mylicense:
            mylid = re.search('\(.+\)', mylicense)
            myurl = mylicense.replace(mylid.group(),'').strip()
            mylid = mylid.group().lstrip('(').rstrip(')')
            if not validators.url(myurl):
                myurl = None
                mytext = mylicense
            #print(mylid, myurl)
        if validators.url(mylicense):
            myurl = mylicense
            mylid = 'NA' # FIXME, use identifier in parantheses
        else:
            mytext = mylicense
        myel = ET.SubElement(myxmltree, ET.QName(mynsmap['mmd'], 'use_constraint'))
        if myurl:
            ET.SubElement(myel, ET.QName(mynsmap['mmd'], 'resource')).text = myurl
            ET.SubElement(myel, ET.QName(mynsmap['mmd'], 'identifier')).text = mylid
        else:
            ET.SubElement(myel, ET.QName(mynsmap['mmd'], 'license_text')).text = mytext

    # Relying on keywords and keywords_vocabulary
    # Assuming GCMDSK if only GCMD keyword is used. This is not entirely correct, but is according to normal behaviour among data providers.
    # Rewrite to extract from CF standard names later
    # Need to check if checking on GCMDLOC and GCMDPROV is required in the future and generate different keywords elements due to this
    # FIXME doesn't handle multiple lists yet
    def add_keywords(self, myxmltree, mynsmap, ncin, myattrs):
        # Consider to add valid_identifiers to a configuration file or to check acgainst a vocabulary
        valid_identifiers = ['None', 'GCMDSK', 'GCMDLOC', 'CF']
        mykeyw = getattr(ncin, 'keywords')
        # Set up MMD elements to fill with content
        if 'keywords_vocabulary' in myattrs:
            mykeyw_voc = getattr(ncin, 'keywords_vocabulary')
            # First handle multiple vocabularies
            myvocs = mykeyw_voc.split(',')
            for el in myvocs:
                if ':' in el:
                    myarr = el.split(':')
                    # Handle that the above will split URL's as well.
                    if len(myarr) > 3:
                        mykeyw_voc = myarr[0].strip()
                        mykeyw_nam = myarr[1].strip()
                        mykeyw_res = ':'.join(myarr[2:4])
                    else:
                        mykeyw_voc = myarr[0].strip()
                        mykeyw_nam = myarr[1].strip()
                else:
                    # Added exception to handle OSISAF
                    if el == "GCMD Science Keywords":
                        mykeyw_voc = "GCMDSK"
                    else:
                        # If no colon is present, assumes identifier is listed
                        mykeyw_voc = el
                # Set up MMD keywords elements to fill with content
                if mykeyw_voc in valid_identifiers:
                    if mykeyw_voc == 'None':
                        mykwnone = ET.SubElement(myxmltree,ET.QName(mynsmap['mmd'],'keywords'))
                        mykwnone.set('vocabulary',mykeyw_voc)
                    elif mykeyw_voc == 'GCMDSK':
                        mykwgcmdsk = ET.SubElement(myxmltree,ET.QName(mynsmap['mmd'],'keywords'))
                        mykwgcmdsk.set('vocabulary',mykeyw_voc)
                    elif mykeyw_voc == 'GCMDLOC':
                        mykwgcmdloc = ET.SubElement(myxmltree,ET.QName(mynsmap['mmd'],'keywords'))
                        mykwgcmdloc.set('vocabulary',mykeyw_voc)
                    elif mykeyw_voc == 'CF':
                        mykwcf = ET.SubElement(myxmltree,ET.QName(mynsmap['mmd'],'keywords'))
                        mykwcf.set('vocabulary',mykeyw_voc)
                # Special hack for IGPAS, should be removed later
                if mykeyw_voc in 'GCMD_Keywords':
                    mykwgcmdsk = ET.SubElement(myxmltree,ET.QName(mynsmap['mmd'],'keywords'))
                    mykwgcmdsk.set('vocabulary','GCMDSK')
                # Special hack for backwards compatibility for NIRD published data from UiT
                #if mykeyw_voc in 'GCMD':
                #    mykwgcmdsk = ET.SubElement(myxmltree,ET.QName(mynsmap['mmd'],'keywords'))
                #    mykwgcmdsk.set('vocabulary','GCMDSK')

        else:
            mykeyw_voc = 'None'
            mykwnone = ET.SubElement(myxmltree,ET.QName(mynsmap['mmd'],'keywords'))
            mykwnone.set('vocabulary',mykeyw_voc)
        # Now we fill MMD keywords elements with content...
        ##myel = ET.SubElement(myxmltree,ET.QName(mynsmap['mmd'],'keywords'))
        tmp = getattr(ncin, 'keywords')
        if re.search('Earth Science >',tmp,re.IGNORECASE):
            mykwgcmdsk = ET.SubElement(myxmltree,ET.QName(mynsmap['mmd'],'keywords'))
            mykwgcmdsk.set('vocabulary','GCMDSK')
        values = getattr(ncin,'keywords').split(',')
        for el in values:
            if ':' in el:
                kw = el.split(':')
                # Currently we are ignoring those providing identifiers in the keywords list...
                if len(kw) >= 2:
                    myvoc = kw[0].strip()
                    kw = kw[1].strip()
                else:
                    kw = kw[0].strip()
            else:
                myvoc = 'None'
                # Check that this exists, if not create what is needed
                if 'mykwnone' not in vars():
                    mykwnone = ET.SubElement(myxmltree,ET.QName(mynsmap['mmd'],'keywords'))
                    mykwnone.set('vocabulary',myvoc)
                kw = el.strip()
            if myvoc == 'GCMDSK' or myvoc == 'GCMD_Keywords' or myvoc == 'GCMD':
                # Hack for IGPAS... Should be removed in the future...
                ET.SubElement(mykwgcmdsk, ET.QName(mynsmap['mmd'],'keyword')).text = kw
            elif myvoc == 'GCMDLOC':
                ET.SubElement(mykwgcmdloc, ET.QName(mynsmap['mmd'],'keyword')).text = kw
            elif myvoc == 'CF':
                ET.SubElement(mykwcf, ET.QName(mynsmap['mmd'],'keyword')).text = kw
            elif myvoc == 'None':
                if re.match('Earth Science >',el,re.IGNORECASE):
                    ET.SubElement(mykwgcmdsk, ET.QName(mynsmap['mmd'],'keyword')).text = kw
                else:
                    ET.SubElement(mykwnone, ET.QName(mynsmap['mmd'],'keyword')).text = kw

    def add_project(self, myxmltree, mynsmap, ncin):
        myprojects = getattr(ncin, 'project').split(',')
        for el in myprojects:
            myel = ET.SubElement(myxmltree,ET.QName(mynsmap['mmd'],'project'))
            # Split in short and long name for each project
            if '(' in el:
                mylongname = el.split('(')[0]
                myshortname = el.split('(')[1].rstrip(')')
            else:
                mylongname = el
                myshortname = ''
            myel2 = ET.SubElement(myel,ET.QName(mynsmap['mmd'],'long_name'))
            myel2.text = mylongname.strip()
            myel2 = ET.SubElement(myel,ET.QName(mynsmap['mmd'],'short_name'))
            myel2.text = myshortname.strip()

    # Add platform, relies on controlled vocabulary in MMD, will read platform and platform_vocabulary from ACDD if the latter is present and map
    def add_platform(self, myxmltree, mynsmap, ncin, myattrs):
        myplatform = getattr(ncin, 'platform')
        if ',' in myplatform:
            # Split string in multiple elements
            myplatform = myplatform.split(',')
        if isinstance(myplatform, list):
            myel = ET.SubElement(myxmltree,ET.QName(mynsmap['mmd'],'platform'))
            for el in platform:
                myel2 = ET.SubElement(myel,ET.QName(mynsmap['mmd'],'long_name'))
                # Not added yet since MMD only relies on satellite data for now.
                valid_statements = []
                myel2.text = el
        else:
            myel = ET.SubElement(myxmltree,ET.QName(mynsmap['mmd'],'platform'))
            myel2 = ET.SubElement(myel,ET.QName(mynsmap['mmd'],'long_name'))
            # Not added yet since MMD only relies on satellite data for now.
            valid_statements = []
            myel2.text = myplatform

    # Add activity_type, relies on source and controlled vocabulary. If vocabulary isn't used leave open. Uses local extension if available
    def add_activity_type(self, myxmltree, mynsmap, ncin, myattrs):
        if 'activity_type' in myattrs:
            myactivity = getattr(ncin, 'activity_type')
        else:
            myactivity = getattr(ncin, 'source')
        myel = ET.SubElement(myxmltree,ET.QName(mynsmap['mmd'],'activity_type'))
        # Not added yet since MMD only relies on satellite data for now.
        valid_statements = ['Aircraft', 'Space Borne Instrument','Numerical Simulation', 'Climate Indicator', 'In Situ Land-based station(Land station) (Field Experiment)', 'In Situ Ship-based station(Cruise)', 'In Situ Ocean fixed station(Moored instrument)', 'In Situ Ocean moving station(Float)', 'In Situ Ice-based station(Ice station) (Field Experiment)', 'Interview/Questionnaire(Interview) (Questionnaire)', 'Maps/Charts/Photographs(Maps) (Charts)(Photographs)']
        if myactivity in valid_statements:
            myel.text = myactivity
        else:
            myel.text = 'Not available'

    def add_wigos_related_info(self, myxmltree, mynsmap, ncin, myattrs):
        wigos_id = getattr(ncin, 'wigosId')
        querywigosid = "https://oscar.wmo.int/surface/rest/api/search/station?wigosId="
        try:
            with ul.urlopen(querywigosid+wigos_id, timeout=10) as response:
                wigos_data = json.load(response)
                station_name = wigos_data['stationSearchResults'][0]['name']
                station_url = 'https://oscar.wmo.int/surface/#/search/station/stationReportDetails/'+ wigos_id
                myel = ET.SubElement(myxmltree,ET.QName(mynsmap['mmd'],'related_information'))
                ET.SubElement(myel, ET.QName(mynsmap['mmd'], 'type')).text = 'Observation facility'
                ET.SubElement(myel, ET.QName(mynsmap['mmd'], 'description')).text = 'WIGOS Station: '+station_name+' ('+wigos_id+')'
                ET.SubElement(myel, ET.QName(mynsmap['mmd'], 'resource')).text = 'https://oscar.wmo.int/surface/#/search/station/stationReportDetails/'+wigos_id
        except:
            print('Could not add wigos information')

    # This relies on specific formatting of the global attribute. If not followed, it will be ignored and status set to Scientific. Use local extension to ACDD if available
    def add_operational_status(self, myxmltree, mynsmap, ncin, myattrs):
        if 'operational_status' in myattrs:
            myoperstat = getattr(ncin, 'operational_status')
        else:
            myoperstat = getattr(ncin, 'processing_level')
        if myoperstat == 'operational':
            myoperstat = 'Operational'
        myel = ET.SubElement(myxmltree,ET.QName(mynsmap['mmd'],'operational_status'))
        valid_statements = ['Operational', 'Pre-Operational','Experimental', 'Scientific']
        if myoperstat in valid_statements:
            myel.text = myoperstat
        else:
            myel.text = 'Scientific'

    # Add dataset_citation, relies on ACDD element references and these being present as a DOI. FIXME
    def add_dataset_citation(self, myxmltree, mynsmap, ncin):
        myref = getattr(ncin, 'references')
        # Check if attribute is URL
        regex = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
        urls = re.findall(regex,myref)
        refurl = None
        refdoi = None
        if urls is not None:
            for u in urls:
                if isinstance(u,tuple):
                    if validators.url(u[0]):
                        if 'doi.org' in u[0]:
                            refdoi = u[0]
                        else:
                            refurl = u[0]
        if refdoi or refurl:
            myel = ET.SubElement(myxmltree,ET.QName(mynsmap['mmd'],'dataset_citation'))
        if refdoi is not None:
            ET.SubElement(myel,ET.QName(mynsmap['mmd'],'doi')).text = refdoi
        if refurl is not None:
            ET.SubElement(myel,ET.QName(mynsmap['mmd'],'url')).text = refurl

    # Add OPeNDAP URL etc if processing an OPeNDAP URL. 
    def add_web_services(self, myxmltree, mynsmap):
        # Add OPeNDAP data_access if "netcdf_product" is OPeNDAP url
        if ('dodsC' in self.netcdf_product or 'opendap' in self.netcdf_product) and self.parse_services == True:
            da_element = ET.SubElement(myxmltree, ET.QName(mynsmap['mmd'], 'data_access'))
            type_sub_element = ET.SubElement(da_element, ET.QName(mynsmap['mmd'], 'type'))
            description_sub_element = ET.SubElement(da_element, ET.QName(mynsmap['mmd'], 'description'))
            resource_sub_element = ET.SubElement(da_element, ET.QName(mynsmap['mmd'], 'resource'))
            type_sub_element.text = "OPeNDAP"
            description_sub_element.text = "Open-source Project for a Network Data Access Protocol"
            resource_sub_element.text = self.netcdf_product

            _desc = ['Open-source Project for a Network Data Access Protocol.',
                     'OGC Web Mapping Service, URI to GetCapabilities Document.']
            _res = [self.netcdf_product.replace('dodsC', 'fileServer'),
                    self.netcdf_product.replace('dodsC', 'wms')]
            access_list = []
            _desc = []
            _res = []
            # Not able to guess URL's for HYRAX. This is not very robust.
            if 'dodsC' in self.netcdf_product:
                add_wms_data_access = True
                add_http_data_access = True
            else:
                add_wms_data_access = False
                add_http_data_access = False
            if add_wms_data_access:
                access_list.append('OGC WMS')
                _desc.append('OGC Web Mapping Service, URI to GetCapabilities Document.')
                _res.append(self.netcdf_product.replace('dodsC', 'wms'))
            if add_http_data_access:
                access_list.append('HTTP')
                _desc.append('Direct download of file')
                _res.append(self.netcdf_product.replace('dodsC', 'fileServer'))
            for prot_type, desc, res in zip(access_list, _desc, _res):
                dacc = ET.SubElement(myxmltree, ET.QName(mynsmap['mmd'], 'data_access'))
                dacc_type = ET.SubElement(dacc, ET.QName(mynsmap['mmd'], 'type'))
                dacc_type.text = prot_type
                dacc_desc = ET.SubElement(dacc, ET.QName(mynsmap['mmd'], 'description'))
                dacc_desc.text = str(desc)
                dacc_res = ET.SubElement(dacc, ET.QName(mynsmap['mmd'], 'resource'))
                if 'OGC WMS' in prot_type:
                    if self.parse_wmslayers:
                        wms_layers = ET.SubElement(dacc, ET.QName(mynsmap['mmd'], 'wms_layers'))
                        # Don't add variables containing these names to the wms layers
                        skip_layers = ['latitude', 'longitude', 'angle']
                        for w_layer in all_netcdf_variables:
                            if any(skip_layer in w_layer for skip_layer in skip_layers):
                                continue
                            wms_layer = ET.SubElement(wms_layers, ET.QName(mynsmap['mmd'], 'wms_layer'))
                            wms_layer.text = w_layer
                    # Need to add get capabilities to the wms resource
                    res += '?service=WMS&version=1.3.0&request=GetCapabilities'
                dacc_res.text = res

def main(input_file=None, output_path='./',parse_services=False,parse_wmslayers=False, print_file=False):
    """Run the the mdd creation from netcdf"""

    if input_file:
        # This will extract the stem of the netcdf product filename
        output_name = '{}'.format(Path(input_file).stem)
    else:
        output_name = 'multisensor_sic.xml'
        input_file = ('https://thredds.met.no/thredds/dodsC/sea_ice/'
                      'SIW-METNO-ARC-SEAICE_HR-OBS/ice_conc_svalbard_aggregated')
    md = Nc_to_mmd(output_path, output_name, input_file, parse_services, parse_wmslayers, print_file)
    md.to_mmd()
