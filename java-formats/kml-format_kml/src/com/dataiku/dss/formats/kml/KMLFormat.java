package com.dataiku.dss.formats.kml;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import com.dataiku.dss.utils.KMLParser;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.dataiku.dip.coremodel.Schema;
import com.dataiku.dip.datalayer.ColumnFactory;
import com.dataiku.dip.datalayer.ProcessorOutput;
import com.dataiku.dip.datalayer.Row;
import com.dataiku.dip.datalayer.RowFactory;
import com.dataiku.dip.plugin.CustomFormat;
import com.dataiku.dip.plugin.CustomFormatInput;
import com.dataiku.dip.plugin.CustomFormatOutput;
import com.dataiku.dip.plugin.CustomFormatSchemaDetector;
import com.dataiku.dip.plugin.InputStreamWithContextInfo;
import com.dataiku.dip.shaker.types.GeoPoint.Coords;
import com.dataiku.dip.util.XMLUtils;
import com.dataiku.dip.warnings.WarningsContext;
import com.google.gson.JsonObject;

public class KMLFormat implements CustomFormat {
    /**
     * Create a new instance of the format
     */
    public KMLFormat() {
    }

    /**
     * Create a reader for a stream in the format
     */
    @Override
    public CustomFormatInput getReader(JsonObject config, JsonObject pluginConfig) {
        return new KMLFormatInput();
    }

    /**
     * Create a writer for a stream in the format
     */
    @Override
    public CustomFormatOutput getWriter(JsonObject config, JsonObject pluginConfig) {
        return new KMLFormatOutput();
    }

    /**
     * Create a schema detector for a stream in the format (used if canReadSchema=true in the json)
     */
    @Override
    public CustomFormatSchemaDetector getDetector(JsonObject config, JsonObject pluginConfig) {
        return new KMLFormatDetector();
    }

    public  class KMLFormatInput implements CustomFormatInput {
        /**
         * Called if the schema is available (ie, dataset has been created)
         */
        @Override
        public void setSchema(Schema schema, boolean allowExtraColumns) {
        }

        @Override
        public void setWarningsContext(WarningsContext warnContext) {
        }

        /**
         * extract data from the input stream. The emitRow() on the out will throw exceptions to
         * enforce limits set to number of rows read, so these should not be caught and hidden.
         */
        @Override
        public void run(InputStreamWithContextInfo in, ProcessorOutput out, ColumnFactory cf, RowFactory rf) throws Exception {
            if (in.getFilename() != null && in.getFilename().endsWith(".kmz")) {
                throw new IllegalArgumentException("KMZ not supported yet");
            } else {
                logger.info("Parsing KML");
                Document domDoc = XMLUtils.parse(in.getInputStream());
                KMLParser kmlParser = new KMLParser();
                Element kmlElt = domDoc.getDocumentElement();
                Element documentElt = kmlParser.getFirstNodeByTagName(kmlElt,  "Document");
                logger.info("GOT documentNode " + documentElt);
                kmlParser.parseContainer(documentElt, out, cf, rf);
            }
        }

        @Override
        public void close() throws IOException {
        }
    }

    public static class KMLFormatOutput implements CustomFormatOutput {
        @Override
        public void close() throws IOException {
        }

        @Override
        public void header(ColumnFactory cf, OutputStream os) throws IOException, Exception {
        }

        @Override
        public void format(Row row, ColumnFactory cf, OutputStream os) throws IOException, Exception {
        }

        @Override
        public void footer(ColumnFactory cf, OutputStream os) throws IOException, Exception {
        }

        @Override
        public void cancel(OutputStream os) throws IOException, Exception {
        }

        @Override
        public void setOutputSchema(Schema schema) {
        }

        @Override
        public void setWarningsContext(WarningsContext warningsContext) {
        }
    }

    public static class KMLFormatDetector implements CustomFormatSchemaDetector {
        @Override
        public Schema readSchema(InputStreamWithContextInfo in) throws Exception {
            return null;
        }

        @Override
        public void close() throws IOException {
        }
    }

    private static Logger logger = Logger.getLogger("dku");
}
