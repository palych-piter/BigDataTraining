package training.bigdata.epam;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class BiddingPriceAnalyzerTest {


        MapDriver<Object, Text, Text, MapperOutputWritable> mapDriver;
        ReduceDriver<Text, MapperOutputWritable, Text, IntWritable> reduceDriver;
        MapReduceDriver<Object, Text, Text, MapperOutputWritable, Text, IntWritable> mapReduceDriver;

        @Before
        public void setUp() {

            BiddingPriceAnalyzer.BiddingPriceMapper mapper = new BiddingPriceAnalyzer.BiddingPriceMapper();
            BiddingPriceAnalyzer.BiddingPriceReducer reducer = new BiddingPriceAnalyzer.BiddingPriceReducer();

            mapDriver = MapDriver.newMapDriver(mapper);
            reduceDriver = ReduceDriver.newReduceDriver(reducer);
            mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

        }


        @Test
        public void testMapper() throws IOException {

            mapDriver.withInput(new Text("key"), new Text("2e72d1bd7185fb76d69c852c57436d37\t20131019025500549\t1\tCAD06D3WCtf\tMozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)\t113.117.187.*\t216\t234\t2\t33235ca84c5fee9254e6512a41b3ad5e\t8bbb5a81cc3d680dd0c27cf4886ddeae\tnull\t3061584349\t728\t90\tOtherView\tNa\t5\t7330\t277\t48\tnull\t2259\t10057,13800,13496,10079,10076,10075,10093,10129,10024,10006,10110,13776,10146,10120,10115,10063"));
            mapDriver.addCacheFile("city.en.txt");

            List<Pair<Text, MapperOutputWritable>> results = new ArrayList<Pair<Text, MapperOutputWritable>>();
            results = mapDriver.run();

            assertEquals (new Text("zhongshan"), results.get(0).getFirst());
            assertEquals (277, results.get(0).getSecond().getBiddingPrice());
            assertEquals ("WINDOWS_XP", results.get(0).getSecond().getOperatingSystem());

        }



        @Test
        public void testReducer() throws IOException {

            MapperOutputWritable inputMapRecord1 = new MapperOutputWritable("WINDOWS_XP", 255);
            MapperOutputWritable inputMapRecord2 = new MapperOutputWritable("LINUX", 500);

            List<MapperOutputWritable> values = new ArrayList<MapperOutputWritable>();
            values.add(inputMapRecord1);
            values.add(inputMapRecord2);

            reduceDriver.withInput(new Text("zhongshan"), values);

            List<Pair<Text,IntWritable>> results = new ArrayList<Pair<Text,IntWritable>>();
            results = reduceDriver.run();

            assertEquals ("zhongshan",results.get(0).getFirst().toString());
            assertEquals (new IntWritable(2), results.get(0).getSecond());

        }

        @Test
        public void testMapReduce() throws IOException {
            mapReduceDriver.withInput(
                    new Text("key"),
                    new Text("2e72d1bd7185fb76d69c852c57436d37\t20131019025500549\t1\tCAD06D3WCtf\tMozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)\t113.117.187.*\t216\t234\t2\t33235ca84c5fee9254e6512a41b3ad5e\t8bbb5a81cc3d680dd0c27cf4886ddeae\tnull\t3061584349\t728\t90\tOtherView\tNa\t5\t7330\t277\t48\tnull\t2259\t10057,13800,13496,10079,10076,10075,10093,10129,10024,10006,10110,13776,10146,10120,10115,10063\n" +
                                  "93074d8125fa8945c5a971c2374e55a8\t20131019161502142\t3\tCAH9FYCtgQf\tMozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727)\t119.145.140.*\t216\t234\t1\t20fc675468712705dbf5d3eda94126da\t9c1ecbb8a301d89a8d85436ebf393f7f\tnull\tmm_10982364_973726_8930541\t300\t250\tFourthView\tNa\t0\t7323\t294\t201\tnull\t2259\t10057,10059,10083,10102,10024,10006,10110,10031,10063,10116\n" +
                                  "bcbc973f1a93e22de83133f360759f04\t20131019134022114\t1\tCALAIF9UcIi\tMozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; SE 2.X MetaSr 1.0)\t59.34.170.*\t216\t234\t3\t7ed515fe566938ee6cfbb6ebb7ea4995\tea4e49e1a4b0edabd72386ee533de32f\tnull\tALLINONE_F_Width2\t1000\t90\tNa\tNa\t50\t7336\t294\t50\tnull\t2259\t10059,14273,10117,10075,10083,10102,10006,10148,11423,10110,10031,10126,13403,10063\n")
            );
            mapReduceDriver.addCacheFile("city.en.txt");

            List<Pair<Text,IntWritable>> results = new ArrayList<Pair<Text, IntWritable>>();
            results = mapReduceDriver.run();

            assertEquals (new Text("zhongshan"), results.get(0).getFirst());
            assertEquals (new IntWritable(2), results.get(0).getSecond());

        }
}




