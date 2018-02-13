package training.bigdata.epam;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;


public class TrafficAnalyzerTest {


        MapDriver<Object, Text, Text, MapWritable> mapDriver;
        ReduceDriver<Text, MapWritable, Text, Text> reduceDriver;
        MapReduceDriver<Object, Text, Text, MapWritable, Text, Text> mapReduceDriver;

        @Before
        public void setUp() {

            TrafficAnalyzer.TrafficAnalyzerMapper mapper = new TrafficAnalyzer.TrafficAnalyzerMapper();
            TrafficAnalyzer.TrafficAnalyzerCombiner combiner = new TrafficAnalyzer.TrafficAnalyzerCombiner();
            TrafficAnalyzer.TrafficAnalyzerReducer reducer = new TrafficAnalyzer.TrafficAnalyzerReducer();

            mapDriver = MapDriver.newMapDriver(mapper);
            reduceDriver = ReduceDriver.newReduceDriver(reducer);
            mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer).withCombiner(combiner);

        }


        @Test
        public void testMapper() throws IOException {

            mapDriver.withInput(new Text("key"), new Text("ip140 - - [24/Apr/2011:12:34:53 -0400] \"GET /sunFAQ/ HTTP/1.1\" 200 8342 \"http://host2/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16 ( .NET CLR 3.5.30729)\""));

            List<Pair<Text, MapWritable>> results = new ArrayList<Pair<Text, MapWritable>>();
            results = mapDriver.run();

            assertEquals (new Text("ip140"), results.get(0).getFirst());
            assertEquals (new FloatWritable(8342), results.get(0).getSecond().get(new Text("total")));
            assertEquals (new FloatWritable(8342), results.get(0).getSecond().get(new Text("average")));

        }


        @Test
        public void testReducer() throws IOException {

            MapWritable inputMapRecord1 = new MapWritable();
            inputMapRecord1.put(new Text("total"), new FloatWritable(8342));
            inputMapRecord1.put(new Text("average"), new FloatWritable(8342));

            MapWritable inputMapRecord2 = new MapWritable();
            inputMapRecord2.put(new Text("total"), new FloatWritable(29554));
            inputMapRecord2.put(new Text("average"), new FloatWritable(29554));

            List<MapWritable> values = new ArrayList<MapWritable>();
            values.add(inputMapRecord1);
            values.add(inputMapRecord2);

            reduceDriver.withInput(new Text("ip140"), values);

            List<Pair<Text,Text>> results = new ArrayList<Pair<Text,Text>>();
            results = reduceDriver.run();

            assertEquals ("ip140",results.get(0).getFirst().toString());
            assertEquals ("Total:37896.0 Average:18948.0", results.get(0).getSecond().toString());

        }

        @Test
        public void testMapReduce() throws IOException {
            mapReduceDriver.withInput(
                    new Text("key"),
                    new Text("ip140 - - [24/Apr/2011:20:23:20 -0400] \"GET /sunFAQ/411howto/411_not.gif HTTP/1.1\" 200 29554 \"http://host2/sunFAQ/411howto/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.0; ja; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16 ( .NET CLR 3.5.30729)\"\n" +
                            "ip140 - - [24/Apr/2011:20:23:20 -0400] \"GET /sunFAQ/411howto/411_hole.gif HTTP/1.1\" 200 58611 \"http://host2/sunFAQ/411howto/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.0; ja; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16 ( .NET CLR 3.5.30729)\"\n")
            );
            List<Pair<Text,Text>> results = new ArrayList<Pair<Text, Text>>();
            results = mapReduceDriver.run();

            assertEquals ("ip140",results.get(0).getFirst().toString());
            assertEquals ("Total:88165.0 Average:44082.5", results.get(0).getSecond().toString());

        }

}




