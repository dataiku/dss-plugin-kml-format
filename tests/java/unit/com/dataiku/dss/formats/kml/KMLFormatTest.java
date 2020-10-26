package com.dataiku.dss.formats.kml;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import com.dataiku.dip.ApplicationConfigurator;
import com.dataiku.dip.datalayer.Column;
import com.dataiku.dip.datalayer.ColumnFactory;
import com.dataiku.dip.datalayer.ProcessorOutput;
import com.dataiku.dip.datalayer.Row;
import com.dataiku.dip.datalayer.RowFactory;
import com.dataiku.dip.datalayer.streamimpl.StreamColumnFactory;
import com.dataiku.dip.datalayer.streamimpl.StreamRowFactory;
import com.dataiku.dip.plugin.CustomFormatInput;
import com.dataiku.dip.plugin.InputStreamWithContextInfo;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;


public class KMLFormatTest {

    static class MockRow{
        String name;
        String description;
        String geom;

        public MockRow(String name, String description, String geom) {
            this.name = name;
            this.description = description;
            this.geom = geom;
        }

        @Override
        public String toString() {
            return "MockRow{" +
                    "name='" + name + '\'' +
                    ", description='" + description + '\'' +
                    ", geom='" + geom + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MockRow mockRow = (MockRow) o;
            return Objects.equals(name, mockRow.name) &&
                    Objects.equals(description, mockRow.description) &&
                    Objects.equals(geom, mockRow.geom);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, description, geom);
        }
    }

    @Test
    public void testReadFile() throws IOException {
        // For testing purpose only, make sure we can read from a file
        InputStream is = KMLFormatTest.class.getClassLoader().getResourceAsStream("com/dataiku/dss/formats/kml/sample_kml.kml");
        byte[] buffer = new byte[is.available()];
        is.read(buffer);
        String str = new String(buffer, "UTF-8");
        System.out.println("Ran tests..." + str);
    }


    @BeforeEach()
    public void setupDSS() {
        ApplicationConfigurator.autoconfigure();
    }

    @Test
    public void testReadKML() throws Exception {
        // Testing the class externally
        InputStream is = KMLFormatTest.class.getClassLoader().getResourceAsStream("com/dataiku/dss/formats/kml/light_kml.kml");
        // Create empty mock InputStreamWithContextInfo
        InputStreamWithContextInfo isci = new InputStreamWithContextInfo(is, null, "light_kml.kml", null);
        // rows will be useful for the implementation of the ProcessorOutput
        final List<MockRow> rows = new ArrayList<>();

        ColumnFactory colFactory = new StreamColumnFactory();
        RowFactory rowFactory = new StreamRowFactory();

        final Column colName = colFactory.column("name");
        final Column colDescription = colFactory.column("description");
        final Column colGeom = colFactory.column("geom");

        ProcessorOutput accumulateRow = new ProcessorOutput() {
            @Override
            public void emitRow(Row row) throws Exception {
                System.out.println(row);
                MockRow mockRow = new MockRow(row.get(colName), row.get(colDescription), row.get(colGeom));
                rows.add(mockRow);
            }

            @Override
            public void lastRowEmitted() throws Exception {

            }

            @Override
            public void cancel() throws Exception {

            }

            @Override
            public void setMaxMemoryUsed(long l) {

            }
        };

        KMLFormat kmlFormat = new KMLFormat();
        CustomFormatInput kmlFormatInput = kmlFormat.getReader(null, null);
        kmlFormatInput.run(isci, accumulateRow, colFactory, rowFactory);

        List<MockRow> expected = Arrays.asList(
                new MockRow("Extruded placemark",
                        "Tethered to the ground by a customizable\n          \"tail\"",
                        "POINT(-122.0857667006183 37.42156927867553)"),
                new MockRow("Tessellated",
                        "If the \u003ctessellate\u003e tag has a value of 1, the line will contour to the underlying terrain",
                        "LINESTRING(36.10677870477137 -112.0814237830345,36.0905099328766 -112.0870267752693)"),
                new MockRow("Building 40",
                        null,
                        null)
        );

        assertThat(rows, is(expected));
    }
}
