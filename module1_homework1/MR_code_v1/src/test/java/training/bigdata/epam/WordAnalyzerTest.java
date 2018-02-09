package training.bigdata.epam;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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


public class WordAnalyzerTest {
            org.apache.hadoop.mrunit.MapDriver<Object, Text, Text, MapWritable> mapDriver;
            ReduceDriver<Text, MapWritable, Text, IntWritable> reduceDriver;
            MapReduceDriver<Object, Text, Text, MapWritable, Text, IntWritable> mapReduceDriver;


        @Before
        public void setUp() {
            WordAnalyzer.TokenizerMapper mapper = new WordAnalyzer.TokenizerMapper();
            WordAnalyzer.IntSumReducer reducer = new WordAnalyzer.IntSumReducer();
            mapDriver = MapDriver.newMapDriver(mapper);
            reduceDriver = ReduceDriver.newReduceDriver(reducer);
            mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        }


        @Test
        public void testMapper() throws IOException {
            mapDriver.withInput(new Text("key"), new Text("12345"));
            List<Pair<Text,MapWritable>> results = new ArrayList<Pair<Text,MapWritable>>();
            results = mapDriver.run();
            Map.Entry<Writable, Writable> entry = results.get(0).getSecond().entrySet().iterator().next();

            assertEquals ("key",results.get(0).getFirst().toString());
            assertEquals (new Text("12345"), (Text) entry.getKey());
            assertEquals (new IntWritable(5), (IntWritable) entry.getValue());
        }


        @Test
        public void testReducer() throws IOException {
            List<MapWritable> values = new ArrayList<MapWritable>();

            MapWritable mapRecord1 = new MapWritable();
            mapRecord1.put(new Text("1"), new IntWritable(1));
            mapRecord1.put(new Text("12"), new IntWritable(2));

            MapWritable mapRecord2 = new MapWritable();
            mapRecord2.put(new Text("123"), new IntWritable(3));
            mapRecord2.put(new Text("1234"), new IntWritable(4));
            mapRecord2.put(new Text("12345"), new IntWritable(5));

            values.add(mapRecord1);
            values.add(mapRecord2);

            reduceDriver.withInput(new Text("key"), values);
            reduceDriver.withOutput(new Text("12345"), new IntWritable(5));
            reduceDriver.runTest();
        }

        @Test
        public void testMapReduce() throws IOException {
            mapReduceDriver.withInput(new Text("key"), new Text("1 12 123 1234 12345"));
            List<Pair<Text,IntWritable>> results = new ArrayList<Pair<Text,IntWritable>>();
            results = mapReduceDriver.run();

            assertEquals ("12345",results.get(0).getFirst().toString());
            assertEquals (new IntWritable(5),results.get(0).getSecond());
        }
    }




