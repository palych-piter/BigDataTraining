package training.bigdata.epam;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.SkipBadRecords;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BiddingPriceAnalyzer {

    public static final Logger logger = LoggerFactory.getLogger(BiddingPriceAnalyzer.class);
    public static final Integer HIGH_BID_PRICED_VALUE = 250;
    public static final Integer IMPRESSION_EVENT_TYPE_VALUE = 1;

    public static enum BIDDING_PRICE_RECORD_COUNTER {
        TotalNumber,
        Processed
    }

    ;


    public static class BiddingPriceMapper
            extends Mapper<Object, Text, Text, MapperOutputWritable> {

        private Text word = new Text();
        private Configuration conf;
        private BufferedReader cityFile;
        private Map<Integer, String> cityNames = new HashMap<Integer, String>();


        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();
            URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
            for (URI patternsURI : patternsURIs) {
                Path patternsPath = new Path(patternsURI.getPath());
                String patternsFileName = patternsPath.getName().toString();
                parseCityFile(patternsFileName);
            }
        }


        private void parseCityFile(String fileName) {
            try {
                cityFile = new BufferedReader(new FileReader(fileName));
                String cityRecord = null;
                while ((cityRecord = cityFile.readLine()) != null) {
                    String[] splitCityRecord = cityRecord.split("\t");
                    cityNames.put(Integer.valueOf(splitCityRecord[0]), splitCityRecord[1]);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {


            Integer biddingPrice = new Integer(0);
            Integer logType = new Integer(0);
            Integer cityId = new Integer(0);
            String cityName = new String();
            String agentInfo = new String();
            String operatingSystem = new String();
            String bidID = new String();
            String inputString = new String(value.toString());


            Scanner scanner = new Scanner(inputString);

            while (scanner.hasNextLine()) {

                Integer numberImpressionEvents = new Integer(0);

                String line = scanner.nextLine();
                context.getCounter(BIDDING_PRICE_RECORD_COUNTER.TotalNumber).increment(1);

                if (line != null) {
                    String[] splitLine = line.split("\t");

                    if (splitLine.length > 19) {
                        Boolean isTypeNumeric = splitLine[2].chars().allMatch(Character::isDigit);
                        Boolean isCityIdNumeric = splitLine[7].chars().allMatch(Character::isDigit);
                        Boolean isBiddingPriceNumeric = splitLine[19].chars().allMatch(Character::isDigit);

                        if (isBiddingPriceNumeric && isTypeNumeric && isCityIdNumeric) {

                            cityId = Integer.valueOf(splitLine[7]);
                            logType = Integer.valueOf(splitLine[2]);
                            biddingPrice = Integer.valueOf(splitLine[19]);
                            agentInfo = splitLine[4];

                            UserAgent userAgent = UserAgent.parseUserAgentString(agentInfo);
                            operatingSystem = userAgent.getOperatingSystem().toString();

                            if (logType.equals(IMPRESSION_EVENT_TYPE_VALUE) && biddingPrice > HIGH_BID_PRICED_VALUE) {
                                cityName = (cityNames.get(cityId) == null) ? "Unknown" : cityNames.get(cityId);
                                MapperOutputWritable outputMapper = new MapperOutputWritable(operatingSystem, biddingPrice);
                                context.write(new Text(cityName), outputMapper);

                            }
                        }
                    }
                }
            }
            scanner.close();
        }
    }


    public static class BiddingPricePartitioner extends
            Partitioner<Text, MapperOutputWritable> {
        @Override
        public int getPartition(Text key, MapperOutputWritable value, int numReduceTasks) {

            if (numReduceTasks == 0) {
                return 0;
            }

            if (value.getOperatingSystem().equals("WINDOWS_XP")) {
                return 0;
            } else {
                return 1;
            }
        }
    }


    public static class BiddingPriceReducer
            extends Reducer<Text, MapperOutputWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<MapperOutputWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            Integer totalNumberImpressionEvents = new Integer(0);
            for (MapperOutputWritable biddingPrice : values) {
                totalNumberImpressionEvents++;
            }
            context.write(key, new IntWritable(totalNumberImpressionEvents));
        }
    }


        public static void main(String[] args) throws Exception {

            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "bidding price analyzer");
            job.setJarByClass(BiddingPriceAnalyzer.class);
            job.setMapperClass(BiddingPriceMapper.class);
            job.setPartitionerClass(BiddingPricePartitioner.class);
            job.setReducerClass(BiddingPriceReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(MapperOutputWritable.class);

            job.setNumReduceTasks(2);

            SkipBadRecords.setMapperMaxSkipRecords(conf, 100000);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.addCacheFile(new Path(args[2]).toUri());

//        FileInputFormat.addInputPath(job, new Path("input"));
//        FileOutputFormat.setOutputPath(job, new Path("output"));

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        }

}


