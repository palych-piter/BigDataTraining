package training.bigdata.epam;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.hadoop.io.*;
import org.junit.*;

import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.tools.ant.types.Assertions;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import training.bigdata.epam.WordAnalyzer;

import static org.junit.Assert.assertEquals;


public class TrafficAnalyzerTest {
            MapDriver<Object, Text, Text, IntWritable> mapDriver;
            ReduceDriver<Text, IntWritable, Text, MapWritable> reduceDriver;
            MapReduceDriver<Object, Text, Text, IntWritable, Text, MapWritable> mapReduceDriver;


        @Before
        public void setUp() {
            TrafficAnalyzer.TokenizerMapper mapper = new TrafficAnalyzer.TokenizerMapper();
            TrafficAnalyzer.IntSumReducer reducer = new TrafficAnalyzer.IntSumReducer();
            mapDriver = MapDriver.newMapDriver(mapper);
            reduceDriver = ReduceDriver.newReduceDriver(reducer);
            mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        }


        @Test
        public void testMapper() throws IOException {
            mapDriver.withInput(new Text("key"), new Text("ip140 - - [24/Apr/2011:12:34:53 -0400] \"GET /sunFAQ/ HTTP/1.1\" 200 8342 \"http://host2/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16 ( .NET CLR 3.5.30729)\""));

            List<Pair<Text, IntWritable>> results = new ArrayList<Pair<Text, IntWritable>>();
            results = mapDriver.run();

            assertEquals ("ip140",results.get(0).getFirst().toString());
            assertEquals ("8342",results.get(0).getSecond().toString());

        }


        @Test
        public void testReducer() throws IOException {

            List<IntWritable> values = new ArrayList<IntWritable>();
            values.add(new IntWritable(8342));
            values.add(new IntWritable(29554));

            reduceDriver.withInput(new Text("ip140"), values);

            List<Pair<Text,MapWritable>> results = new ArrayList<Pair<Text,MapWritable>>();
            results = reduceDriver.run();

            assertEquals ("ip140",results.get(0).getFirst().toString());

            Iterator<Map.Entry<Writable, Writable>> entryIterator = results.get(0).getSecond().entrySet().iterator();

            Map.Entry<Writable, Writable> entryAverage = entryIterator.next();
            assertEquals (new Text("average"), (Text) entryAverage.getKey());
            assertEquals (new DoubleWritable(18948), entryAverage.getValue());

            Map.Entry<Writable, Writable> entryTotal = entryIterator.next();
            assertEquals (new Text("total"),  entryTotal.getKey());
            assertEquals (new DoubleWritable(37896), entryTotal.getValue());

        }

        @Test
        public void testMapReduce() throws IOException {
            mapReduceDriver.withInput(
                    new Text("key"),
                    new Text("ip140 - - [24/Apr/2011:20:23:20 -0400] \"GET /sunFAQ/411howto/411_not.gif HTTP/1.1\" 200 29554 \"http://host2/sunFAQ/411howto/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.0; ja; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16 ( .NET CLR 3.5.30729)\"\n" +
                            "ip140 - - [24/Apr/2011:20:23:20 -0400] \"GET /sunFAQ/411howto/411_hole.gif HTTP/1.1\" 200 58611 \"http://host2/sunFAQ/411howto/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.0; ja; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16 ( .NET CLR 3.5.30729)\"\n")
            );
            List<Pair<Text,MapWritable>> results = new ArrayList<Pair<Text, MapWritable>>();
            results = mapReduceDriver.run();

            assertEquals ("ip140",results.get(0).getFirst().toString());

            Iterator<Map.Entry<Writable, Writable>> entryIterator = results.get(0).getSecond().entrySet().iterator();

            Map.Entry<Writable, Writable> entryAverage = entryIterator.next();
            assertEquals (new Text("average"), (Text) entryAverage.getKey());
            assertEquals (new DoubleWritable(44082.5), entryAverage.getValue());

            Map.Entry<Writable, Writable> entryTotal = entryIterator.next();
            assertEquals (new Text("total"),  entryTotal.getKey());
            assertEquals (new DoubleWritable(88165), entryTotal.getValue());

        }

}




