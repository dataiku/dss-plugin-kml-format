package com.dataiku.dss.formats.kml;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.dataiku.dss.utils.KMLParser;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

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
import com.dataiku.dip.util.XMLUtils;
import com.dataiku.dip.warnings.WarningsContext;
import com.google.gson.JsonObject;
import com.dataiku.dip.utils.DKULogger;

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
            InputStream is;
            if (in.getFilename() != null && in.getFilename().endsWith(".kmz")) {
                logger.info("Parsing KMZ");
                logger.info("Get following filename: " + in.getFilename());
                InputStream inputStream = in.getInputStream();
                ZipInputStream zis = new ZipInputStream(inputStream);
                ZipEntry entry;
                ByteArrayOutputStream os = null;
                while ((entry = zis.getNextEntry()) != null){
                    int count;
                    if (! entry.getName().equals("doc.kml")){
                        continue;
                    } else {
                        os = new ByteArrayOutputStream();
                        byte[] data = new byte[1024];
                        while ((count = zis.read(data, 0, 1024)) != -1){
                            os.write(data, 0, count);
                        }
                    }
                }
                is = new ByteArrayInputStream(os.toByteArray());
            } else {
                logger.info("Parsing KML");
                logger.infoV("Get following filename: {}", in.getFilename());
                is = in.getInputStream();
            }
            Document domDoc = XMLUtils.parse(is);
            KMLParser kmlParser = new KMLParser();
            Element kmlElt = domDoc.getDocumentElement();
            Element documentElt = kmlParser.getFirstNodeByTagName(kmlElt,  "Document");
            kmlParser.parseContainer(documentElt, out, cf, rf);
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
    protected static DKULogger logger = DKULogger.getLogger("dku");
    // private static Logger logger = Logger.getLogger("dku");
}
