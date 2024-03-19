## IPUMS

This respository contains code to run a pipeline which extracts consistently defined cities from IPUMS full count census data. 
The steps are:
- Build a population raster for the entire continental United States
- Smooth the raster using convolution
- Extract clusters using density thresholds and DBSCAN
