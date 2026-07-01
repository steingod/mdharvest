from SPARQLWrapper import SPARQLWrapper, JSON

def query_info_fromexternalvocab(vocab, vocnumber, name):
    sparql = SPARQLWrapper(vocab)
    prefixes = '''
        prefix skos:<http://www.w3.org/2004/02/skos/core#>
        prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        prefix dc:<http://purl.org/dc/terms/>'''

    get_info = '''SELECT ?a ?id ?prefLabel ?altLabel WHERE {
        { ?a skos:prefLabel "%(name)s"@en . }
          UNION
        { ?a skos:altLabel "%(name)s" }
        ?a dc:identifier ?id .
        ?a skos:prefLabel ?prefLabel .
        ?a skos:altLabel ?altLabel .
        FILTER contains(str(?a),"%(vocnumber)s") .
        } LIMIT 1'''

    info = {}

    try:
        sparql.setQuery(prefixes + get_info % {'name': name, 'vocnumber': vocnumber})
        sparql.setReturnFormat(JSON)
        vocabs = sparql.query().convert()

        if len(vocabs["results"]["bindings"]) == 1:
            info['id'] = vocabs["results"]["bindings"][0]['id']['value']
            info['longname'] = vocabs["results"]["bindings"][0]['prefLabel']['value']
            info['shortname'] = vocabs["results"]["bindings"][0]['altLabel']['value']
            info['resource'] = vocabs["results"]["bindings"][0]['a']['value']
        else:
            info = None
    except:
        info = None

    return info
