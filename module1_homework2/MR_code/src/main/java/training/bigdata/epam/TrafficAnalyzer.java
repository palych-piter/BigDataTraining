package training.bigdata.epam;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import eu.bitwalker.useragentutils.UserAgent;

public class TrafficAnalyzer {

    public static final Logger logger = LoggerFactory.getLogger(TrafficAnalyzer.class);



    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, FloatWritable> {

        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String bytes = new String();
            float floatBytes ;
            String ip = new String();
            String inputString = new String(value.toString());
            MapWritable outputMapRecord = new MapWritable();

            Scanner scanner = new Scanner(inputString);
            while (scanner.hasNextLine()) {

                String line = scanner.nextLine();
                String[] splitLine = line.split(" ");

                ip = splitLine[0];
                floatBytes = bytes.equals("-")?0:Float.parseFloat(splitLine[9]);

                UserAgent userAgent = UserAgent.parseUserAgentString(line);

                context.write(new Text(ip), new FloatWritable(floatBytes));

            }
            scanner.close();
        }
    }


    public static class Combiner
            extends Reducer<Text, FloatWritable, Text, MapWritable> {

        public void reduce(Text key, Iterable<FloatWritable> byteValues,
                           Context context
        ) throws IOException, InterruptedException {

            MapWritable outputMapRecord = new MapWritable();
            float totalBytes = 0;
            int counter = 0;
            double avarageBytes;

            for (FloatWritable bytes : byteValues) {
                totalBytes = totalBytes + Float.parseFloat(bytes.toString());
                counter++;
            }

            avarageBytes = (double) totalBytes / (double) counter;

            outputMapRecord.put(new Text("total"), new DoubleWritable(totalBytes));
            outputMapRecord.put(new Text("average"), new DoubleWritable(avarageBytes));

            context.write(key, outputMapRecord );

        }
    }



    public static class IntSumReducer
            extends Reducer<Text, MapWritable, Text, Text> {

        public void reduce(Text key, Iterable<MapWritable> byteValues,
                           Context context
        ) throws IOException, InterruptedException {

            double totalBytes = 0;
            int counter = 0;
            double averageBytes;

            for (MapWritable bytes : byteValues) {
                totalBytes = totalBytes + Double.parseDouble(bytes.get(new Text("total")).toString());
                counter++;
            }

            averageBytes = (double) totalBytes/(double) counter;

            context.write(key, new Text("Total:" + String.valueOf(totalBytes) + " Average:" + String.valueOf(averageBytes)));

        }
    }




    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "traffic analyzer");
        job.setJarByClass(TrafficAnalyzer.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setSortComparatorClass(IntComparator.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }


    public static class IntComparator extends WritableComparator {

        public IntComparator() {
            super(IntWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
            Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

            return v1.compareTo(v2) * (-1);
        }
    }



}