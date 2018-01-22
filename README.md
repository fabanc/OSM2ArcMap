# OSM2ArcMap

## Description

This geoprocessing tool reads an Open Street Map file (.osm) compressed in the the .bz2 format. It writes the output to a file geodatabase. Note that when parsing the relations, only multipolygons will be built and added to the multipolygon feature class. Ways with a tag highway that is associated with a value will be considered as lines, even lollipops. This tool is written for ArcMap.

## XML Parsing and use of lxml.

This geoprocessing tool reads an Open Street Map file (.osm) compressed in the the .bz2 format. It writes the output to a file geodatabase. Note that when parsing the relations, only multipolygons will be built and added to the multipolygon feature class. Ways with a tag highway that is associated with a value will be considered as lines, even lollipops. This tool is written for ArcMap.

## Compatibility with Python 3 and ArcGIS Pro

Not tested yet. I am hoping to tackle that soon.

## Tribute to OSM Tools.

I have been using OSM Tools a lot and it has served me well: [OSM Tools repository](https://www.arcgis.com/home/item.html?id=a8769c63b6524b20891cdc92248772c4)

However, I wanted to test parsing OSM data as XML vs text to get a better code readability. As much as it slows down the code a bit when using Python's default XML libraries, my code is much faster when using lxml.

I have also a few bugs that I ran into with OSM tools that I was unable to get fixed. So I played with the code myself and solve those issues.

