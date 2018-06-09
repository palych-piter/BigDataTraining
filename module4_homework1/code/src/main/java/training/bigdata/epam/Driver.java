package training.bigdata.epam;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
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

    public static void main(String[] args) throws FileNotFoundException {

        List<String> errorCounts;

        //establish Spark context
        sc = establishSparkContext();

        //read the data
        readData(sc);

        //Task 1 : count errors
        JavaRDD<BidError> errorCountsMap = errorCountsCustom(bids);
        //save results
        PrintWriter outputError = new PrintWriter("errors.txt");
        for(BidError resultItem : errorCountsMap.collect()) {
            outputError.println(
                    resultItem.getDate() + "," +
                            resultItem.getErrorMessage() + "," +
                            resultItem.getCount().toString()
            );
        }
        outputError.close();



        //Task 3: explode the bids, convert to a pair rdd
        JavaRDD<BidItem> explodedBids = explodeBids(bids);

        //join with currency
        //convert to JavaPairRDD for joining, the key is date
        JavaPairRDD<String, BidItem> explodedBidsToJoin =
                explodedBids.mapToPair( s-> new Tuple2<>(s.getDate(),s) );

        JavaPairRDD<String, Tuple2<BidItem, String>> explodedBidsJoinedWithCurrency =
                explodedBidsToJoin.join(exchangeRateMap);


        JavaRDD<BidConverted> convertedBids = convertBids(explodedBidsJoinedWithCurrency);
        //convert to JavaPairRDD for joining, the key is motelid
        JavaPairRDD<Integer, BidConverted> convertedBidsToJoin =
                convertedBids.mapToPair( s-> new Tuple2<>(s.getMotelId(),s) );

        //Task 5: join with hotels
        JavaPairRDD<Integer, Tuple2<BidConverted, String>> explodedBidsJoinedWithHotels =
                convertedBidsToJoin.join(motels);

        //convert to pair map to group by motelid / date and find maximum
        JavaRDD<EnrichedItem> finalResult = findMaximum(explodedBidsJoinedWithHotels);

        //save the final results
        //finalResult.saveAsTextFile(System.getProperty("user.dir") + "/output/final-result");
        PrintWriter outputFinal = new PrintWriter("final.txt");
        for(EnrichedItem resultItem : finalResult.collect()){
            outputFinal.println(
                    resultItem.getDate() + "," +
                    resultItem.getMotelId() + "," +
                    resultItem.getHotelName() + "," +
                    resultItem.getPrice() + "," +
                    resultItem.getLoSa()
            );
        }
        outputFinal.close();

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


    public static JavaRDD<BidError> errorCountsCustom(JavaRDD<String> bids) {

        return bids
                //filter only errors
                .filter(s -> s.split(",")[2].contains("ERROR"))
                //get only date and error message
                .mapToPair(w -> new Tuple2<>(w.split(",")[1] + "," + w.split(",")[2], 1))
                //reduce by date and error message
                .reduceByKey((a, b) -> a + b)
                .map( s->  {
                    String date = s._1.split(",")[0];
                    String message = s._1.split(",")[1];
                    Integer count = s._2;
                        return new BidError(date, message,count);
                })
                ;
    }


    public static JavaRDD<BidItem> explodeBids(JavaRDD<String> bids) {

        JavaRDD<String> _tmpBids = bids
                //filter records without errors
                .filter(s -> !s.split(",")[2].contains("ERROR"))
                //get only required columns
                .map(s -> {
                            String[] array = s.split(",");

                                    //hotel_id
                            return  array[0] + "," +
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
                .filter(s -> s.split(",").length == 4)
                ;


        //convert to pair rdd
        JavaRDD<BidItem> explodedCustomRdd =
                _tmpBids
                .map( s->  {
                    String[] array = s.split(",");

                    String date = array[1];
                    String motelId = array[0];
                    String loSa = array[2];
                    Double price = Double.valueOf(array[3]);

                    return new BidItem(date,motelId,loSa,price);

                })
                ;

        return explodedCustomRdd;

    }


    public static JavaRDD<BidConverted> convertBids(JavaPairRDD<String, Tuple2<BidItem, String>> explodedBidsJoinedWithCurrency) {

        //convert to pair rdd
        JavaRDD<BidConverted> convertedPriceRdd =
                explodedBidsJoinedWithCurrency
                .map(s -> {

                    Integer motelid = Integer.parseInt(s._2._1.getMotelId());
                    String date = dateFormatOutput.format(dateFormatInput.parse(s._1));
                    String country = s._2._1.getLoSa();
                    Double convertedSum = Double.parseDouble(numberFormat.format(s._2._1.getPrice() * Double.parseDouble(s._2._2)));

                    return new BidConverted(date,motelid,country,convertedSum);

                }
                );

        return convertedPriceRdd;

    }


    public static JavaRDD<EnrichedItem> findMaximum(JavaPairRDD<Integer, Tuple2<BidConverted, String>> bids) {

        return bids
                .mapToPair(w -> {

                    //key = hotelid + date
                    String date = w._1.toString() + "," + w._2._1.getDate();
                    //value = country + price + hotelName
                    String value = w._2._1.getLoSa() + "," + w._2._1.getPrice() + "," +  w._2._2;

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
                })
                .map(s -> {

                    //hotelid + date
                    String[] array1 = s._1.split(",");
                    String hotelid = array1[0];
                    String date = array1[1];

                    //losa + price + hotelname
                    String[] array2 = s._2.split(",");
                    Double price = Double.parseDouble(array2[1]);
                    String losa = array2[0];
                    String hotemname = array2[2];

                    return new EnrichedItem(date,hotelid,losa,price,hotemname);

                  }
                )

                ;
    }


}



