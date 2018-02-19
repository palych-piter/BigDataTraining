package training.bigdata.epam;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import eu.bitwalker.useragentutils.UserAgent;


public class TrafficAnalyzer {

    public static final Logger logger = LoggerFactory.getLogger(TrafficAnalyzer.class);

    public static enum BROWSER_COUNTER {
        IE6,
        FIREFOX3
    };

    public static class TrafficAnalyzerMapper
            extends Mapper<Object, Text, Text, MapWritable> {

        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String bytes = new String();
            float floatBytes ;
            String ip = new String();
            String inputString = new String(value.toString());

            Scanner scanner = new Scanner(inputString);
            while (scanner.hasNextLine()) {

                MapWritable outputMapRecord = new MapWritable();

                String line = scanner.nextLine();
                String[] splitLine = line.split(" ");
                ip = splitLine[0];
                floatBytes = splitLine[9].equals("-")?0:Float.parseFloat(splitLine[9]);

                UserAgent userAgent = UserAgent.parseUserAgentString(line);
                if (userAgent.getBrowser().toString().equals("FIREFOX3")) {
                    context.getCounter(BROWSER_COUNTER.FIREFOX3).increment(1);
                } else {
                    if (userAgent.getBrowser().toString().equals("IE6")) {
                        context.getCounter(BROWSER_COUNTER.IE6).increment(1);
                    }
                }

                outputMapRecord.put(new Text("total"), new FloatWritable(floatBytes));
                outputMapRecord.put(new Text("average"), new FloatWritable(floatBytes));

                context.write(new Text(ip), outputMapRecord );

            }
            scanner.close();
        }
    }


    public static class TrafficAnalyzerCombiner
            extends Reducer<Text, MapWritable, Text, MapWritable> {

        public void reduce(Text key, Iterable<MapWritable> byteValues,
                           Context context
        ) throws IOException, InterruptedException {

            MapWritable outputMapRecord = new MapWritable();

            int counter = 0;
            float totalBytes = 0;
            float averageBytes = 0;

            for (MapWritable bytes : byteValues) {
                totalBytes += Float.valueOf(bytes.get(new Text("total")).toString());
                counter++;
            }
            averageBytes = (float) totalBytes/(float) counter;

            outputMapRecord.put(new Text("total"), new FloatWritable(totalBytes));
            outputMapRecord.put(new Text("average"), new FloatWritable(averageBytes));

            context.write(key, outputMapRecord );

        }
    }



    public static class TrafficAnalyzerReducer
            extends Reducer<Text, MapWritable, Text, Text> {

        public void reduce(Text key, Iterable<MapWritable> byteValues,
                           Context context
        ) throws IOException, InterruptedException {

            int counter = 0;
            float totalBytes = 0;
            float averageBytes = 0;
            float averageBytesTotal = 0;

            for (MapWritable bytes : byteValues) {
                totalBytes += Float.valueOf(bytes.get(new Text("total")).toString());
                averageBytes += Float.valueOf(bytes.get(new Text("average")).toString());
                counter++;
            }
            averageBytesTotal = (float) averageBytes/(float) counter;

            context.write(key, new Text("Total:" + String.valueOf(totalBytes) + " Average:" + String.valueOf(averageBytesTotal)));
        }
    }




    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "traffic analyzer");
        job.setJarByClass(TrafficAnalyzer.class);
        job.setMapperClass(TrafficAnalyzerMapper.class);
        job.setCombinerClass(TrafficAnalyzerCombiner.class);
        job.setReducerClass(TrafficAnalyzerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        //long counter = job.getCounters().findCounter(BROWSER_COUNTER.FIREFOX3).getValue();
        //System.out.println("Firefox3 Counter: " + counter);

    }


}