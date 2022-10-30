# mdharvest

Python code to harvest discovery metdata using OAI-PMH, OpenSearch and OGC CSW. 
Support for schema.org is currently in a prototype status, while DCAT harvesting is under development.
Operationally OAI-PMH and OGC CSW is fully supported.

Harvesting is coupled with filtering of records harvested according to GCMD Science keywords and latitude/longitude. 
The latter filtering is yet not configurable in top level functions.
