# Data Parsing and Quality Issues

**Note:** Before settling on an XML parser, I had originally tried some other methods to convert the .osm.pbf file to Parquet. These included:

* <u>Using Osmium:</u> I attempted to convert PBF to Parquet via an intermediate CSV/JSON format so I can adjust and clean data before converting to Parquet, but this approach resulted in incorrect parsing.
* <u>Using osm-parquetizer:</u> I also tried the osm-parquetizer package recommended by OSM Wiki, but it failed to process the data correctly.

Eventually, since neither of these approaches were successful, I used an XML parser, which had better control over data extraction (just the nodes) and formatting. However, I ran into some data parsing and quality issues:

## 1. Large File Size and Performance
**Issue:** The OSM XML file of 16.8 GB was too large to load into memory all at once.  

**Resolution:** Used an iterative XML parser (`ElementTree.iterparse`) to process the file in chunks instead of loading everything at once.


## 2. Missing or Incomplete Data
**Issue:** Some elements in the XML file had missing attributes or incomplete data fields, such as missing `amenity`,  `brand`, `cuisine`, `name`, and `shop` values for certain nodes.  

**Resolution:** During parsing, I implemented checks to handle missing values appropriately. If an attribute was missing but non-essential (e.g., brand for local restaurants), I marked it as null instead of skipping the entry. (This is not really an issue when working with the data, since this is a valid reason that a certain location would not have a specific attribute)


## 3. Unexpected Data Types
**Issue:** The latitude and longitude (`lat` and `lon`) were stored as strings, which caused errors in calculations, for example the distance between certain food places.

**Resolution:** Converted numerical attributes to floats.

---
By addressing these issues, I ensured that the parsed data was clean, structured, and ready for further analysis without inconsistencies or missing critical attributes.