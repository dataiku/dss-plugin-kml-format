PLUGIN_ID=`cat plugin.json | python -c "import sys, json; print(str(json.load(sys.stdin)['id']).replace('/',''))"`
PLUGIN_VERSION=`cat plugin.json | python -c "import sys, json; print(str(json.load(sys.stdin)['version']).replace('/',''))"`

clean:
	./gradlew clean --info --no-daemon
	rm -rf lib
	rm -rf dist

build:
	./gradlew dist --info --no-daemon

plugin: clean build
	cat plugin.json|json_pp > /dev/null
	mkdir dist
	zip -MM -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json lib java-formats
