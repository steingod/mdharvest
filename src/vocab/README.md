# Vocabulary module

This module fetches/updates the MMD controlled vocabularies and the CF standard names with available matches to GCMD Science keywords from vocab.met.no. The vocabularies are used when parsing netCDF files through nc_to_mmd.py

## How to update vocabularies

Run:

- ./get_vocab.py mmd to update the MMD controlled vocabularies
- ./get_vocab.py cf to update the CF standard names and mapping

Note that fetching vocabularies works only when connected to the network. If the connection fails, the old vocabularies are not overwritten and can be used instead.
