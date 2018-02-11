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
            extends Mapper<Object, Text, Text, IntWritable> {

        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String bytes = new String();
            String ip = new String();
            String inputString = new String(value.toString());

            Scanner scanner = new Scanner(inputString);
            while (scanner.hasNextLine()) {

                String line = scanner.nextLine();
                String[] splitLine = line.split(" ");

                ip = splitLine[0];
                bytes = splitLine[9];

                UserAgent userAgent = UserAgent.parseUserAgentString(line);

                context.write(new Text(ip), new IntWritable(Integer.parseInt(bytes)));

            }
            scanner.close();
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, MapWritable> {

        public void reduce(Text key, Iterable<IntWritable> byteValues,
                           Context context
        ) throws IOException, InterruptedException {

            MapWritable outputMapRecord = new MapWritable();
            int totalBytes = 0;
            int counter = 0;
            double avarageBytes;

            for (IntWritable bytes : byteValues) {
                totalBytes = totalBytes + bytes.get();
                counter++;
            }


            avarageBytes = (double)totalBytes/(double) counter;

            outputMapRecord.put(new Text("total"), new DoubleWritable(totalBytes));
            outputMapRecord.put(new Text("average"), new DoubleWritable(avarageBytes));

            context.write(key, outputMapRecord );

        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "traffic analyzer");
        job.setJarByClass(TrafficAnalyzer.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputValueClass(Text.class);
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