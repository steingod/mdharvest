#!/usr/bin/env python3
from SPARQLWrapper import SPARQLWrapper, JSON
import datetime
import argparse
import sys


if __name__ == '__main__':

    def get_MMDvocab(collections, vocabno):

        prefixes = '''
            prefix skos:<http://www.w3.org/2004/02/skos/core#>
            prefix text:<http://jena.apache.org/text#>
            prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            prefix owl:<http://www.w3.org/2002/07/owl#>
            prefix dc:<http://purl.org/dc/terms/>'''

        vocabularies = '''select distinct ?concname FROM <https://vocab.met.no/mmd> WHERE {
            ?collection skos:prefLabel "%(collection)s"@en .
            ?collection skos:member ?concept .
            ?concept skos:prefLabel ?concname .
            }'''

        try:
            fullvoc = ''
            for collection in collections:
                #set up call to vocabno
                vocabno.setQuery(prefixes + vocabularies % {'collection': collection})
                vocabno.setReturnFormat(JSON)
                vocabs = vocabno.query().convert()

                #create valid entries per collection
                members = []
                for result in vocabs["results"]["bindings"]:
                    members.append(result['concname']['value'])
                #create a more useful lookup for licenses
                if collection == 'Use Constraint':
                    licenses = lookup_license(members, vocabno)
                    fullvoc += "".join(collection.split()) + ' = ' + str(licenses) + "\n"
                else:
                    fullvoc += "".join(collection.split()) + ' = ' + str(members) + "\n"


            f = open('ControlledVocabulary.py','w')
            update = datetime.datetime.now()
            f.write('#last fetch: '+str(update)+"\n")
            f.write(fullvoc)
            f.close()
        except:
            print('Error fetching MMD concepts: use last fetched vocabulary list!')

        return

    def lookup_license(list_identifiers,vocabno):

        license_lookup = {}

        prefixes = '''
            prefix skos:<http://www.w3.org/2004/02/skos/core#>
            prefix text:<http://jena.apache.org/text#>
            prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            prefix owl:<http://www.w3.org/2002/07/owl#>
            prefix dc:<http://purl.org/dc/terms/>'''

        matching_exactmatch = '''select distinct ?exactMatch FROM <https://vocab.met.no/mmd> WHERE {
            ?concept skos:prefLabel "%(id)s"@en .
            ?concept skos:exactMatch ?exactMatch .
            }'''

        matching_altlabel = '''select distinct ?altLabel FROM <https://vocab.met.no/mmd> WHERE {
            ?concept skos:prefLabel "%(id)s"@en .
            ?concept skos:altLabel ?altLabel .
            FILTER (lang(?altLabel) = "en") .
            }'''

        for licid in list_identifiers:
            vocabno.setQuery(prefixes + matching_exactmatch % {'id': licid})
            vocabno.setReturnFormat(JSON)
            exactmatch = vocabno.query().convert()

            vocabno.setQuery(prefixes + matching_altlabel % {'id': licid})
            vocabno.setReturnFormat(JSON)
            altlabel = vocabno.query().convert()


            licurl = []
            for result in exactmatch["results"]["bindings"]:
                if '/rdf' not in result['exactMatch']['value']:
                    licurl.append(result['exactMatch']['value'])

            licalt = []
            for result in altlabel["results"]["bindings"]:
                licalt.append(result['altLabel']['value'])

            license_lookup[licid] = {'exactMatch' : licurl, 'altLabel' : licalt}


        return license_lookup

    def get_cfnames(vocabno):
        cfnames = {}
        getcfnames = """

        prefix skos:<http://www.w3.org/2004/02/skos/core#>
        prefix text:<http://jena.apache.org/text#>
        prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        prefix owl:<http://www.w3.org/2002/07/owl#>
        prefix dc:<http://purl.org/dc/terms/>

        select distinct ?cflabel from <https://vocab.met.no/CFSTDN> WHERE {
          ?cfconcept rdf:type skos:Concept .
          ?cfconcept skos:prefLabel ?cflabel .
          }
        """
        try:
            listnames = ''
            vocabno.setQuery(getcfnames)
            vocabno.setReturnFormat(JSON)
            results = vocabno.query().convert()
            for res in results['results']['bindings']:
                key = res['cflabel']['value']
                print(key)
                value = cf_to_gcmd_match(res['cflabel']['value'], vocabno)
                cfnames[key] = value
            listnames += "CFNAMES = "  + str(cfnames) + "\n"

            f = open('CFGCMD.py', 'w')
            update = datetime.datetime.now()
            f.write('#last fetch: '+str(update)+"\n")
            f.write(listnames)
            f.close()
        except:
            print('Error fetching CF concepts: use last fetched vocabulary list!')

        return

    def cf_to_gcmd_match(cfname,vocabno):
        prefixes = '''
            prefix skos:<http://www.w3.org/2004/02/skos/core#>
            prefix text:<http://jena.apache.org/text#>
            prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            prefix owl:<http://www.w3.org/2002/07/owl#>
            prefix dc:<http://purl.org/dc/terms/>'''

        queryclose = '''select distinct ?cf ?closematch FROM <https://vocab.met.no/CFSTDN> WHERE {
            ?cf skos:prefLabel "%(name)s"@en.
            ?cf skos:closeMatch ?closematch .
            FILTER contains(str(?closematch),"GCMDSK") .
            }'''

        querybroad = '''select distinct ?cf ?broadmatch FROM <https://vocab.met.no/CFSTDN> WHERE {
            ?cf skos:prefLabel "%(name)s"@en.
            ?cf skos:broadMatch ?broadmatch .
            FILTER contains(str(?broadmatch),"GCMDSK") .
            }'''

        try:
            vocabno.setQuery(prefixes + queryclose % {'name': cfname})
            vocabno.setReturnFormat(JSON)
            resultsclose = vocabno.query().convert()
            close = []
            for result in resultsclose["results"]["bindings"]:
                #print('close',result["closematch"]["value"])
                close.append(gcmd_label(result["closematch"]["value"],vocabno))

            vocabno.setQuery(prefixes + querybroad % {'name': cfname})
            vocabno.setReturnFormat(JSON)
            resultsbroad = vocabno.query().convert()
            broad = []
            for result in resultsbroad["results"]["bindings"]:
                #print('broad',result["broadmatch"]["value"])
                broad.append(gcmd_label(result["broadmatch"]["value"],vocabno))

            matching_terms = {'close': close, 'broad': broad}

        except:
            print("Could not fetch GCMD matching for: ", ncname)
            return

        return(matching_terms)

    def gcmd_label(gcmduri,vocabno):


        getgcmdlabel = '''
        prefix skos:<http://www.w3.org/2004/02/skos/core#>
        prefix text:<http://jena.apache.org/text#>
        prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        prefix owl:<http://www.w3.org/2002/07/owl#>
        prefix dc:<http://purl.org/dc/terms/>

        select distinct ?gcmdlabel from <https://vocab.met.no/GCMDSK> WHERE {
          <%(concept)s> skos:prefLabel ?gcmdlabel .
          }
        '''
        try:
            vocabno.setQuery(getgcmdlabel % {'concept': gcmduri})
            vocabno.setReturnFormat(JSON)
            gcmdlabel = vocabno.query().convert()

            label = gcmdlabel["results"]["bindings"][0]["gcmdlabel"]["value"]

        except:
            print("Could not fetch GCMD label")
            return

        return(label)


    def main(voc):
        vocabno = SPARQLWrapper("https://vocab.met.no/skosmos/sparql")

        collections = ['Use Constraint',
                   'Access Constraint',
                   'Activity Type',
                   'Operational Status',
                   'Access Constraint',
                   'Collection Keywords',
                   'ISO Topic Category',
                   'Dataset Production Status',
                   'Related Information Types',
                   'Keywords Vocabulary']
        if voc == 'mmd':
            get_MMDvocab(collections, vocabno)
        if voc == 'cf':
            cfnames = get_cfnames(vocabno)

    parser = argparse.ArgumentParser(description='Update controlled vocabularies: MMD and CF/GCMD')
    parser.add_argument('vocabulary_type',
                        choices = ['mmd','cf'],
                        help="which vocabularies to update: mmd or cf are valid strings")
    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        sys.exit()

    main(voc=args.vocabulary_type)
