package training.bigdata.epam;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;


public class Driver {

    public static DecimalFormat numberFormat = new DecimalFormat("#0.000");
    public static DateFormat dateFormatOutput = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S", Locale.ENGLISH);
    public static DateFormat dateFormatInput = new SimpleDateFormat("HH-dd-MM-yyyy", Locale.ENGLISH);


    public static void main(String[] args) {

        List<String> errorCounts ;

        // configure spark
        SparkConf sparkConf = new SparkConf()
                .setAppName("spark-core")
                .setMaster("local[2]")
        ;

        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // provide path to input text files
        String bidsPath = Driver.class.getResource("/bids.txt").getPath();
        String exchangeRatePath = Driver.class.getResource("/exchange_rate.txt").getPath();
        String motelsPath = Driver.class.getResource("/motels.txt").getPath();

        // read text files to RDD
        JavaRDD<String> bids = sc.textFile(bidsPath,1);
        //Task 2: load currency, prepare for joining
        JavaPairRDD<String,String> exchangeRateMap = sc.textFile(exchangeRatePath,1)
                .mapToPair(w -> new Tuple2<>(w.split(",")[0], w.split(",")[3]));
        //Task 4: load hotels, prepare for joining
        JavaPairRDD<Integer,String> motels = sc.textFile(motelsPath,1)
                .mapToPair(h -> new Tuple2<>(Integer.parseInt(h.split(",")[0]), h.split(",")[3]));


        //Task 1 : count errors
        JavaPairRDD<String, Integer>  errorCountsMap = errorCounts(bids);
        errorCountsMap.saveAsTextFile(System.getProperty("user.dir")+ "/output/error-map");

        //Task 3: explode the bids, convert to a pair rdd
        JavaPairRDD<String,String> explodedBids = explodeBids(bids);
        explodedBids.saveAsTextFile(System.getProperty("user.dir")+ "/output/exploded-bids");

        //join with currency
        JavaPairRDD<String, Tuple2<String, String>> explodedBidsJoinedWithCurrency =
                explodedBids.join(exchangeRateMap);
        explodedBidsJoinedWithCurrency.saveAsTextFile(System.getProperty("user.dir")+ "/output/currency-joined--bids");

        JavaRDD<String> convertedBids = convertBids(explodedBidsJoinedWithCurrency);
        convertedBids.saveAsTextFile(System.getProperty("user.dir")+ "/output/converted-bids");

       sc.close();

    }


    private static JavaPairRDD<String, Integer> errorCounts(JavaRDD<String> bids) {
         return bids
          //filter only errors
          .filter(s -> s.split(",")[2].contains("ERROR"))
          //get only date and error message
          .mapToPair(w -> new Tuple2<>(w.split(",")[1] + "," + w.split(",")[2], 1))
          //reduce by date and error message
          .reduceByKey((a, b) -> a + b);
    }


    private static JavaPairRDD explodeBids(JavaRDD<String> bids) {
        JavaRDD<String> _tmpBids =  bids
                //filter records without errors
                .filter(s -> !s.split(",")[2].contains("ERROR"))
                //get only required columns
                .map(s ->
                        //id
                        s.split(",")[0] + "," +
                        //date
                        s.split(",")[1] + "," +
                        //sum
                        "US," + s.split(",")[5] + "\t" +

                        //id
                        s.split(",")[0] + "," +
                        //date
                        s.split(",")[1] + "," +
                        //sum
                        "MX," + s.split(",")[6] + "\t" +

                        //id
                        s.split(",")[0] + "," +
                        //date
                        s.split(",")[1] + "," +
                        //sum
                        "CA," + s.split(",")[8] + "\t"
                )
                .flatMap(s -> Arrays.asList(s.split("\t")).iterator())
                //filter rows with amount > 0
                .filter(s -> s.split(",").length == 4);


                //convert to pair rdd
                JavaPairRDD<String, String> explodedPairsRdd =
                        _tmpBids.mapToPair(new PairFunction<String, String, String>() {
                            public Tuple2<String, String> call(String s) {
                                String[] transactionSplit = s.split(",");
                                return new Tuple2<String, String>(
                                           transactionSplit[1],
                                        transactionSplit[0] + "," +
                                           transactionSplit[2] + "," +
                                           transactionSplit[3]
                                );
                            }
                });
                return explodedPairsRdd;
    }


    private static JavaRDD<String> convertBids(JavaPairRDD<String, Tuple2<String, String>> explodedBidsJoinedWithCurrency) {
        return explodedBidsJoinedWithCurrency
                .map( s->
                        //hotel id
                        s._2._1.split(",")[0] + "," +
                                //date
                                dateFormatOutput.format(dateFormatInput.parse(s._1)) + "," +
                                //country
                                s._2._1.split(",")[1] + "," +
                                //converted sum
                                numberFormat.format(Double.parseDouble(s._2._1.split(",")[2]) * Double.parseDouble(s._2._2))
                );
    }

}
