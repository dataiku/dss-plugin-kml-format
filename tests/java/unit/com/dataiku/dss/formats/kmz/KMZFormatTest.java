package com.dataiku.dss.formats.kmz;

// import static org.hamcrest.MatcherAssert.assertThat;
// import static org.hamcrest.Matchers.*;

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


import com.dataiku.dss.formats.kmz.KMZFormat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class KMZFormatTest {

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


    @BeforeEach()
    public void setupDSS() {
        ApplicationConfigurator.autoconfigure();
    }

    @Test
    public void testReadKMZ() throws Exception {
        // Testing the class externally
        InputStream is = KMZFormatTest.class.getClassLoader().getResourceAsStream("com/dataiku/dss/formats/kmz/sample_kmz.kmz");
        InputStreamWithContextInfo isci = new InputStreamWithContextInfo(is, null, null, null);
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

        KMZFormat kmzFormat = new KMZFormat();
        CustomFormatInput kmzFormatInput = kmzFormat.getReader(null, null);
        kmzFormatInput.run(isci, accumulateRow, colFactory, rowFactory);

        System.out.println("Those are some rows:");


        List<MockRow> expected = Arrays.asList(
                new MockRow(
                    "Start! - Peterborough, Ontario",
                    "Of course, we will be starting our great wolrd tour of the 7 Wonders in our beautiful home town of Peterborough, Ontario! I hope you\u0027re ready or alot of flying, because we\u0027re going around the world!",
                    "POINT(-78.33121958124474 44.29645582562842)")
        );



        // assertThat(rows, is(expected));

    }
}
