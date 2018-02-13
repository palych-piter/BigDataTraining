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


        MapDriver<Object, Text, Text, FloatWritable> mapDriver;
        ReduceDriver<Text, FloatWritable, Text, Text> reduceDriver;
        MapReduceDriver<Object, Text, Text, FloatWritable, Text, Text> mapReduceDriver;

        @Before
        public void setUp() {

            TrafficAnalyzer.TokenizerMapper mapper = new TrafficAnalyzer.TokenizerMapper();
            //TrafficAnalyzer.Combiner combiner = new TrafficAnalyzer.Combiner();
            TrafficAnalyzer.IntSumReducer reducer = new TrafficAnalyzer.IntSumReducer();

            mapDriver = MapDriver.newMapDriver(mapper);
            reduceDriver = ReduceDriver.newReduceDriver(reducer);
            mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

        }


        @Test
        public void testMapper() throws IOException {

            mapDriver.withInput(new Text("key"), new Text("ip140 - - [24/Apr/2011:12:34:53 -0400] \"GET /sunFAQ/ HTTP/1.1\" 200 8342 \"http://host2/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16 ( .NET CLR 3.5.30729)\""));

            List<Pair<Text, FloatWritable>> results = new ArrayList<Pair<Text, FloatWritable>>();
            results = mapDriver.run();

            assertEquals (new Text("ip140"), results.get(0).getFirst());
            assertEquals (new FloatWritable(8342), results.get(0).getSecond());

        }


        @Test
        public void testReducer() throws IOException {

            List<FloatWritable> values = new ArrayList<FloatWritable>();

            values.add(new FloatWritable(8342));
            values.add(new FloatWritable(29554));

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




