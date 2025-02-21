# Final Analysis Proposal

## 1. What question are you trying to answer?
I am investigating how the distribution of food places (e.g. restaurants, cafes, fast food, etc.) in San Luis Obispo compares to other college towns (e.g. Davis, Berkeley, Stanford etc.). 

To answer this question, I have other *exploratory questions* like:
* How does the density of food places in SLO compare to other towns?
* Where do food places tend to cluster in SLO compared to larger towns?
* How does the distribution of commercial vs. residential land use affect the clustering of food-related places?
* How far apart are food places on average in SLO versus other towns?

## 2. Why is this question worth answering?
This question is important both personally and in general:
* **Personally:** As a student at Cal Poly SLO, I've noticed that the food scene in SLO is pretty limited, especially compared to larger college towns I've visited. This analysis could help explain why and offer insights on how the food scene could be improved. Also, I want to learn more about city planning and geographical data, which is similar to my Data Science Capstone currently.

* **Generally:** Understanding the distribution of food places in college towns can help students, city planners, and business owners identify underserved areas, improve food accessibility, and promote local businesses. Comparing SLO to other college towns can determine whether this is a local issue or its a more common trend among smaller university towns.

## 3. What is your hypothesis? Why leads you towards that hypothesis?
I hypothesize that SLO has fewer and less varied food places compared to other college towns. However, I think is due to SLO being smaller and the topography of SLO is somewhat rural. So, in college towns located in bigger, urban areas, they would have a diverse food collection and more choices.

This is based on personal experience and what I've learned about for cities and geography. I know a smaller market leads to less options and diversity of goods, college town size matters, and geographical topography affect where structures are built.   

## 4. What is the primary dataset(s) you will use to answer the question?
To anwer this question, I will use OpenStreetMap data, specifically the north-america-latest.osm.pbf file. This detailed geospatial dataset contains a bunch of information like addresses, coordinates, amenities, type of landuse, etc., but I will focus specifically on food-related businesses under amenities.

The OSM dataset is in PBF (Protocol Buffers) format, which I'm not entirely familiar with, but I plan to convert this binary format to Parquet, which is also a binary format, and something I'm more familiary with. A lot of the filtering can be done with command-line tools like **osmium**, so I can extract before converting it to a Parquet file. After preprocessing, I plan to use **polars** to perform data exploration and **folium** for map visualizations.

The dataset can be downloaded here: https://download.geofabrik.de/