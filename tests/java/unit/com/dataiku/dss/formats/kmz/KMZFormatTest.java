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
                    "POINT(-78.33121958124474 44.29645582562842)"),
                new MockRow(
                    "Stop #1 - The Grand Canyon, U.S.A",
                    "The Grand Canyon, located in Colorado is the largest gorge in the world with a 290 mile long range across the Colorado Plateau. From rim to rim it measures up to 18 miles across with an average widthe of 10 miles and an average depth of 1 mile. The canyon also has a wide selection of ecozones; there are as many ecological regions within the Grand Canyon as their are between Canada and Mexico. There is everything from snow to desert, and habitats for more than 400 animals and over 1500 plants. Since the Grand Canyon National Park was established in 1919, tourism has grown hugely with about 5 million visitors to the canyon every year.",
                    "POINT(-113.2400138888889 36.14438888888889)"),
                new MockRow(
                    "Stop #2 - Paricutin Volcano, Mexico",
                    "Paricutin Volcano is located in the Mexican state of Michoacan, and literally appeared out of nowhere! As a famrer was readying his fields in the spring, the ground beneath him began to shake, opening up a hole about 150 ft. long, which then raised the ground up about 2 and a half meters high. By the next morning, the volcano had risen up to 30 ft. and was violently throwing out rocks, continuing to grow another 120 ft. that day throwing rocks and lava up to 1000 ft. in the air. Within that same year the volcano reached 1,100 ft. which would be four fifths of its final height, and left ashes falling on cities as far away as Mexico City. The volcano had appeared in an area well know for volcanoes called the Mexican Volcano Belt which stretches 700 miles east to west along souther Mexico.",
                    "POINT(-102.2405291948281 19.48327194920295)"),
                new MockRow(
                    "Stop #3 - Rio De Janeiro Harbour, Rio De Janeiro",
                    "The Portugese discovered the Harbour of Rio De Janeiro in the 16th century, and because of the narrow entrance of the harbour which stretches 20 miles inland, and the beautiful mountains surrounding the opening of the harbour, they thought they had found the entrance to a great inland river; they named the area River of the First January, as they had discovered it on January 1st, of 1502. The bay that appeared as a river is said to be only one of the many illusions that Rio held, and discovers said that because of the such strange and striking shapes and forms that surrounded the bay, almost everything appeared as something else! One of the most famous sights of the bay is the Corcovado Mountain which lies bare and lopsided and now is home to a  huge statue of Christ the Redeemer.",
                    "POINT(-43.1879756649956 -22.89383583837741)"),
                new MockRow(
                    "Stop #4 - Victoria Falls, Zimbabwe",
                    "Victoria Falls are the largest waterfalls in the world, and they are so large that it is said the best place to see them may possibly be from the air. The falls seperate Zimbabwe from Zambia, and are lined with a beautiful forest and serene lagoons where deadly hippo's and crocdiles can be found. Visitors have the option of a wide range of activities to explore the falls, which include kayaking, fishing, canoeing, walking safaris, horseback riding, or a flight over the falls. Whichever your choice it's sure to be exciting!', geom='POINT(25.85924489750097 -17.93289658717635)'}, MockRow{name='Stop #5 - Mount Everst, Nepal', description='Mount Everest is of course, the highest mountain in the world! It is part of the Himilayan mountain range,  located in Nepal. The first expedition to ever reach the top was in 1953, and this was also the eigth attempt. It's height reaches 29,028 ft. and although most may not want to climb the mountain fully, there are many camps at lower levels which tourists can visit until they feel they've gone far enough!",
                    "POINT(86.94381245648268 27.99506677671201)"),
                new MockRow(
                    "Stop #6 - The Great Barrier Reef, Australia",
                    "The Great Barrier Reef is located on Australia's Gold Coast and spans more than 2000 km. It is home to thousands of plant and animal species and the Australian government has designated it as a protected Marine Park. Because the reef spans in a north to south direction, it travels thourgh many different climates, with rain forests and mountains more predominant in the north and coral cay more predominant in the south. There is a wide range of activities for tourists to explore the coral reef, which include scuba diving, swimming, snorkelling, a variety of watersports, boating, birdwatching, etc. The opportunities are endless!",
                    "POINT(147.7738507813772 -18.53114834955963)"),
                new MockRow(
                    "Stop #7 - The Northern Lights, Nothwest Territories",
                    "We are going to be stopping in the city of Yellowknife in the Northwest Territories to view the spectacular Northern Lights. The northern lights have been bringing wonder and terror to the people of the north for thousands of years. Over the years many myths and stories had been created by these people to try and help them understand this phenomenon in the sky. From 'fox fires',  to dancing souls, to messages from their dead, each tribe had their own way of explaining the beautiful lights. The lights are actually produced from strong solar winds that come into contact with the earth's magnetic field and as the winds become trapped in these magnetic fields, the oxygen and nitrogen that are produded clash with eachother and create the colours and pictures that we see from the ground. The clash produces awe-inspiring lights and patterns and produce many colours from green, bright pink, blue and violet. The lights form a 2,000 mile wide oval over the noth poles, and have been known to shine as far down as Mexico. This phenomenon can be seen day in and day out regardless the time of year in the far north of Canada. ",
                    "POINT(-114.3712500010722 62.45432116005)"),
                new MockRow(
                    "Stop #8 - Home Sweet Home!",
                    "Well I hope you have enjoyed our trip around the world as much as I have! Getting a chance to see the top 7 Natural Wonders of the World will definently be a highlight I will never forget! I hope you're not too tired and that yo got to learn a few new things too. Thanks for coming for the ride!",
                    "POINT(-78.32974664886251 44.3016392161836)")
                );

        // assertThat(rows, is(expected));
        }
}
