PLUGIN_VERSION=0.1.1
PLUGIN_ID=kml-format

plugin:
	./gradlew dist
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json lib java-formats
