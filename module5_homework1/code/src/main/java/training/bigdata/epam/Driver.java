package training.bigdata.epam;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.apache.spark.sql.functions.col;
import static training.bigdata.epam.ReadBidData.readBidData;
import static training.bigdata.epam.ReadErrorData.readErrorData;
import static training.bigdata.epam.ReadHotelData.readHotelData;
import static training.bigdata.epam.ReadRateData.readRateData;
import static training.bigdata.epam.SaveCSV.saveCsv;


public class Driver {

    public static DecimalFormat numberFormat = new DecimalFormat("#0.000");
    public static DateFormat dateFormatOutput = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S", Locale.ENGLISH);
    public static DateFormat dateFormatInput = new SimpleDateFormat("HH-dd-MM-yyyy", Locale.ENGLISH);

    public static JavaRDD<String> bids;
    public static JavaPairRDD<String, String> exchangeRateMap;
    public static JavaPairRDD<Integer, String> motels;

    public static Dataset<Row> bidDataFrame;
    public static Dataset<Row> rateDataFrame;
    public static Dataset<Row> hotelDataFrame;
    public static Dataset<Row> errorDataFrame;

    public static SparkSession sc;

    public static void main(String[] args) throws FileNotFoundException {

        List<String> errorCounts;

        //establish Spark context
        sc = establishSparkContext();

        //read the data, initialize initial datasets
        //create a schema programmatically
        bidDataFrame = readBidData(sc);
        rateDataFrame = readRateData(sc);
        hotelDataFrame = readHotelData(sc);
        //use a custom class and reflection
        errorDataFrame = readErrorData(sc);

        //Task 1 : count errors
        errorDataFrame = errorDataFrame
                .groupBy(col("date"), col("errorMessage"))
                .agg(functions.max(col("count")).alias("count"));
        //save results
        saveCsv(errorDataFrame,"./output/", "Overwrite");


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

    public static SparkSession establishSparkContext() {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[2]")
                .appName("hotel application")
                .getOrCreate()
                ;
        return sparkSession;
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

    //replace nulls in dataset
    public static Dataset<Row> replaceNulls(Dataset<Row> _dataset, String stringToReplace) {
        String[] columnList = _dataset.columns();
        //change column type to string and replace nulls
        for (String columnName : columnList) {
            _dataset = _dataset
                    .withColumn(columnName, col(columnName).cast(DataTypes.StringType))
                    .na().fill(stringToReplace, new String[]{columnName});
        }
        return _dataset;
    }



}



