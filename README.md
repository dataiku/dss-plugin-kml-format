# KML & KMZ format extractor

- Expose a KML/KMZ parser
- Run tests

## Building the plugin

To build the plugin using Gradle, do:
```bash
./gradlew build
./gradlew dist
```

It will create a zip of the plugin in the dist folder. DSS uses this zip to install the plugin.

## Testing the plugin

To test the plugin using Gradle, do:
```bash
./gradlew test
```
