package training.bigdata.epam;

import java.io.IOException;
import java.nio.ByteBuffer;
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
            MapDriver<Object, Text, IntWritable, Text> mapDriver;
            ReduceDriver<IntWritable, Text, IntWritable, Text> reduceDriver;
            MapReduceDriver<Object, Text, IntWritable, Text, IntWritable, Text> mapReduceDriver;


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
            mapDriver.withInput(new Text("key"), new Text("12345 1234 123 12 1 54321"));

            List<Pair<IntWritable,Text>> results = new ArrayList<Pair<IntWritable,Text>>();
            results = mapDriver.run();

            assertEquals ("5",results.get(0).getFirst().toString());
            assertEquals ("12345",results.get(0).getSecond().toString());

            assertEquals ("4",results.get(1).getFirst().toString());
            assertEquals ("1234",results.get(1).getSecond().toString());

            assertEquals ("3",results.get(2).getFirst().toString());
            assertEquals ("123",results.get(2).getSecond().toString());

            assertEquals ("2",results.get(3).getFirst().toString());
            assertEquals ("12",results.get(3).getSecond().toString());

            assertEquals ("1",results.get(4).getFirst().toString());
            assertEquals ("1",results.get(4).getSecond().toString());

            assertEquals ("5",results.get(5).getFirst().toString());
            assertEquals ("54321",results.get(5).getSecond().toString());

        }


        @Test
        public void testReducer() throws IOException {

            List<Text> values = new ArrayList<Text>();

            values.add(new Text("12345"));
            values.add(new Text("54321"));
            reduceDriver.withInput(new IntWritable(5), values);
            reduceDriver.withOutput(new IntWritable(5),new Text("54321, 12345"));
            reduceDriver.runTest();

        }

        @Test
        public void testMapReduce() throws IOException {
            mapReduceDriver.withInput(new Text("key"), new Text("12345 1234 123 12 1 54321"));
            List<Pair<IntWritable,Text>> results = new ArrayList<Pair<IntWritable,Text>>();
            results = mapReduceDriver.run();

            assertEquals (new IntWritable(5),results.get(4).getFirst());
            assertEquals ("54321, 12345",results.get(4).getSecond().toString());

            assertEquals (new IntWritable(4),results.get(3).getFirst());
            assertEquals ("1234",results.get(3).getSecond().toString());

            assertEquals (new IntWritable(3),results.get(2).getFirst());
            assertEquals ("123",results.get(2).getSecond().toString());

            assertEquals (new IntWritable(2),results.get(1).getFirst());
            assertEquals ("12",results.get(1).getSecond().toString());

            assertEquals (new IntWritable(1),results.get(0).getFirst());
            assertEquals ("1",results.get(0).getSecond().toString());

        }


}




