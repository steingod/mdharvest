# Notes concerning harvesting schema.org

## End points to test
    - https://arcticdata.io/metacat/sitemaps/sitemap1.xml (NSF/ADC)
    - https://data.g-e-m.dk/sitemap (GEM)
    - http://hedeby.uwaterloo.ca/api/ (PDC)
    - http://hedeby.uwaterloo.ca/aggregator/ (CCADI)
    - https://doi.pangaea.de/sitemap.xml (multiple levels)
    - https://doi.pangaea.de/sitemap-0.xml.gz

## Lessons learned

### NSF ADC
    - lazy loading of web pages require delayed scraping

### GEM
    - Not all records tested, but not complying with SOSO
    - Most records are skipped
    - don't think we should support this, test against NSF/ADC instead

### PDC/CCADI
    - All interaction through API
    - Use http://hedeby.uwaterloo.ca/aggregator/metadata?page=1 etc

### PANGAEA
    - sitemap with multiple levels (need rewrite)
    - not confirming to ESIPs SOSO
    - need some rewrite
