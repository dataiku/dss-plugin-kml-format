# KML & KMZ format extractor

This plugin contains a KML or KMZ parser allowing dataset creation from this format in DSS.
Each row in the output dataset will contain a geospatial object (Coordinates, LineString ...) and any additional comments associated with it. 

## Build the plugin

To build the plugin using Gradle, do:
```bash
./gradlew build
./gradlew dist
```

It will create a zip of the plugin in the dist folder. DSS uses this zip to install the plugin.

## Test

To test the plugin using Gradle, do:
```bash
./gradlew test
```

## Upload to DSS

To upload the plugin as a zip to DSS, run:
```bash
make plugin
```