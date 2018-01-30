package training.bigdata.epam;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordAnalyzer {

    public static final Logger logger = LoggerFactory.getLogger(WordAnalyzer.class);

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, MapWritable>{

        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                MapWritable mapRecord = new MapWritable();
                mapRecord.put(word, new IntWritable(word.toString().length()));
                context.write(new Text("key"), mapRecord);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, MapWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();
        private IntWritable maxlength = new IntWritable(0);
        private Text outputKey = new Text();

        public void reduce(Text key, Iterable<MapWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (MapWritable val : values) {
                for (MapWritable.Entry entry : val.entrySet())
                {
                    if (maxlength.compareTo(entry.getValue()) < 0){
                        maxlength = (IntWritable) entry.getValue();
                        outputKey = (Text) entry.getKey();
                    }
                }
            }
            context.write(new Text(outputKey), maxlength);
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word analyzer");
        job.setJarByClass(WordAnalyzer.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputValueClass(MapWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }

}