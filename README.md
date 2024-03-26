## IPUMS

This respository contains a pipeline that extracts consistently defined cities from IPUMS full count census data. 
The steps are:
- Build a population raster for the entire continental United States
- Smooth the raster using convolution
- Extract clusters using density thresholds and DBSCAN
