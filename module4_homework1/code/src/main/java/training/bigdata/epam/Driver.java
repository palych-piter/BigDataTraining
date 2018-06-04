package training.bigdata.epam;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
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

    public static JavaRDD<String> bids;
    public static JavaPairRDD<String, String> exchangeRateMap;
    public static JavaPairRDD<Integer, String> motels;

    public static JavaSparkContext sc;

    public static void main(String[] args) {

        List<String> errorCounts;

        //establish Spark context
        sc = establishSparkContext();

        //read the data
        readData(sc);

        //Task 1 : count errors
        JavaPairRDD<String, Integer> errorCountsMap = errorCounts(bids);

        //Task 3: explode the bids, convert to a pair rdd
        JavaPairRDD<String, String> explodedBids = explodeBids(bids);

        //join with currency
        JavaPairRDD<String, Tuple2<String, String>> explodedBidsJoinedWithCurrency =
                explodedBids.join(exchangeRateMap);

        JavaPairRDD<Integer, String> convertedBids = convertBids(explodedBidsJoinedWithCurrency);

        //Task 5: join with hotels
        JavaPairRDD<Integer, Tuple2<String, String>> explodedBidsJoinedWithHotels =
                convertedBids.join(motels);

        //convert to pair map to group by motesid/date and find maximum
        JavaPairRDD<String,String> finalResult = findMaximum(explodedBidsJoinedWithHotels);

        //save the final results
        finalResult.saveAsTextFile(System.getProperty("user.dir") + "/output/final-result");

        sc.close();

    }

    public static JavaSparkContext establishSparkContext() {

        // configure spark
        SparkConf sparkConf = new SparkConf()
                .setAppName("spark-core")
                .setMaster("local[2]");

        // start a spark context
        return new JavaSparkContext(sparkConf);

    }


    public static void readData (JavaSparkContext sc){

        // provide path to input text files
        String bidsPath = Driver.class.getResource("/bids.txt").getPath();
        String exchangeRatePath = Driver.class.getResource("/exchange_rate.txt").getPath();
        String motelsPath = Driver.class.getResource("/motels.txt").getPath();

        // read text files to RDD
        bids = sc.textFile(bidsPath, 1);
        //Task 2: load currency, prepare for joining
        exchangeRateMap = sc.textFile(exchangeRatePath, 1)
                .mapToPair(w -> new Tuple2<>(w.split(",")[0], w.split(",")[3]));
        //Task 4: load hotels, prepare for joining
        motels = sc.textFile(motelsPath, 1)
                .mapToPair(h -> new Tuple2<>(Integer.parseInt(h.split(",")[0]), h.split(",")[1]));

    }



    public static JavaPairRDD<String, Integer> errorCounts(JavaRDD<String> bids) {
        return bids
                //filter only errors
                .filter(s -> s.split(",")[2].contains("ERROR"))
                //get only date and error message
                .mapToPair(w -> new Tuple2<>(w.split(",")[1] + "," + w.split(",")[2], 1))
                //reduce by date and error message
                .reduceByKey((a, b) -> a + b);
    }


    public static JavaPairRDD<String, String> explodeBids(JavaRDD<String> bids) {
        JavaRDD<String> _tmpBids = bids
                //filter records without errors
                .filter(s -> !s.split(",")[2].contains("ERROR"))
                //get only required columns
                .map(s -> {
                            String[] array = s.split(",");
                            //id
                            return array[0] + "," +
                                    //date
                                    array[1] + "," +
                                    //sum
                                    "US," + array[5] + "\t" +

                                    //id
                                    array[0] + "," +
                                    //date
                                    array[1] + "," +
                                    //sum
                                    "MX," + array[6] + "\t" +

                                    //id
                                    array[0] + "," +
                                    //date
                                    array[1] + "," +
                                    //sum
                                    "CA," + array[8] + "\t";
                        }
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


    public static JavaPairRDD<Integer, String> convertBids(JavaPairRDD<String, Tuple2<String, String>> explodedBidsJoinedWithCurrency) {
        JavaRDD<String> _tmpBids = explodedBidsJoinedWithCurrency
                .map(s -> {
                                String hotelId = s._2._1.split(",")[0];
                                String date = dateFormatOutput.format(dateFormatInput.parse(s._1));
                                String country = s._2._1.split(",")[1];
                                String convertedSum = numberFormat.format(Double.parseDouble(s._2._1.split(",")[2]) * Double.parseDouble(s._2._2));

                                return hotelId + "," + date + "," + country + "," + convertedSum;
                        }
                );


        //convert to pair rdd
        JavaPairRDD<Integer, String> convertedPairsRdd =
                _tmpBids.mapToPair(h -> {

                    String[] array = h.split(",");
                    return new Tuple2<>(
                                Integer.parseInt(array[0]),
                            array[1] + "," + array[2] + "," + array[3]
                        );
                }
                );

        return convertedPairsRdd;

    }


    public static JavaPairRDD<String,String> findMaximum(JavaPairRDD<Integer, Tuple2<String, String>> bids) {

        return bids
                .mapToPair(w -> {

                            String[] array = w._2._1.split(",");
                            String date = w._1.toString() + "," + array[0];
                            String value = array[1] + "," + array[2] +  "," + w._2._2;

                            return new Tuple2<>(
                                    date,
                                    value
                            );
                })
                .reduceByKey(new Function2<String, String, String>() {
                    @Override
                    public String call(String v1, String v2) throws Exception {

                        String[] array1 = v1.split(",");
                        String[] array2 = v2.split(",");

                        if (Double.parseDouble(array1[1]) > Double.parseDouble(array2[1]))
                        {
                            return array1[0] + "," + array1[1] + "," + array2[2];
                        } else {
                            return array1[0] + "," + array2[1] + "," + array2[2];
                        }
                    }
                });
    }


}



