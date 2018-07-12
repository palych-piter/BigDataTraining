package training.bigdata.epam;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class BidConvertToSeq {

    public static void convertBid(SparkSession spark, String fileName) {

        //provide path to input text files
        String bidsPath = Driver.class.getResource("/" + fileName).getPath();

        //get JavaSparkContext
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        JavaRDD<String> bidRDD = jsc
                .textFile(bidsPath, 1)
                .filter(s -> !s.split(",")[2].contains("ERROR"));

        //read bid text files to RDD
        JavaPairRDD<Text, Text> bidPairRDD = bidRDD
                .mapToPair((String w) -> {

                    String[] array = w.split(",",-1);

                    //key
                    String key = array[0] + "," + array[1];

                    //value
                    String value = "";
                    //value = array[2];
                    for(int i=2; i<=17; i++) {
                        value = value + "," + array[i];
                    }

                    return new Tuple2<>(
                            new Text(key),
                            new Text(value)
                    );

                });

        bidPairRDD.saveAsHadoopFile("./output/bids.seq", Text.class, Text.class, SequenceFileOutputFormat.class);

    }

}
