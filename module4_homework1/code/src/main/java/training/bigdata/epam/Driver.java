package training.bigdata.epam;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;
import scala.tools.cmd.gen.AnyVals;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;


public class Driver {

    public static JavaPairRDD<String, Double>  exchangeRateMap;

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
        JavaRDD<String> exchangeRates = sc.textFile(exchangeRatePath,1);
        JavaRDD<String> motels = sc.textFile(motelsPath,1);


        //Task 1 : count errors
        JavaPairRDD<String, Integer>  errorCountsMap = errorCounts(bids);
        errorCountsMap.saveAsTextFile(System.getProperty("user.dir")+ "/output/error-map");


        //Task 2: create currency map table
        exchangeRateMap = createExchangeRateMap(exchangeRates);
        exchangeRateMap.saveAsTextFile(System.getProperty("user.dir")+ "/output/exchange-rate-map");


        //Task 3: explode the bids
        JavaRDD explodedBids = explodeBids(bids);
        explodedBids.saveAsTextFile(System.getProperty("user.dir")+ "/output/exploded-bids");

        JavaRDD convertedBids = convertBids(explodedBids);
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


    private static JavaPairRDD<String, Double> createExchangeRateMap(JavaRDD<String> exchange_rates) {
        DateFormat format_input = new SimpleDateFormat("HH-dd-MM-yyyy", Locale.ENGLISH);
        DateFormat format_output = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S", Locale.ENGLISH);
        return exchange_rates
                .mapToPair(w ->
                        new Tuple2<>(format_input.format(format_input.parse(w.split(",")[0])), Double.parseDouble(w.split(",")[3]))
                );
    }


    private static JavaRDD explodeBids(JavaRDD<String> bids) {
        return bids
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
                .filter(s -> s.split(",").length == 4)
         ;
    }


    private static JavaRDD convertBids(JavaRDD<String> bids) {
        DateFormat format_input = new SimpleDateFormat("HH-dd-MM-yyyy", Locale.ENGLISH);
        DateFormat format_output = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S", Locale.ENGLISH);
        return bids
                .map(s ->
                        s.split(",")[0] + "," +
                        format_output.format(format_input.parse(s.split(",")[1])) + "," +
                        s.split(",")[2] + "," +
                        convertCurrency(
                                s.split(",")[1],
                                Double.parseDouble(s.split(",")[3])
                        )
                )
                ;
    }


    private static Double convertCurrency(String date, Double amount) {
        DecimalFormat numberFormat = new DecimalFormat("#.000");
        return
                Double.parseDouble(
                        numberFormat.format(amount * exchangeRateMap.filter(s -> s._1.equals(date)).first()._2)
                )
        ;
    }


}
