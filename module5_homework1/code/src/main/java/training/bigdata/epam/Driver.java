package training.bigdata.epam;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Locale;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.round;
import static training.bigdata.epam.ExplodeBids.explodeBids;
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
    public static Dataset<Row> bidDataFrameExploded;
    public static Dataset<Row> bidDataFrameConverted;

    public static Dataset<Row> rateDataFrame;
    public static Dataset<Row> hotelDataFrame;
    public static Dataset<Row> errorDataFrame;

    public static SparkSession sc;

    public static void main(String[] args) throws FileNotFoundException {

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
                .agg(functions.sum(col("count")).alias("count"));
        //save results
        saveCsv(errorDataFrame, "./output/errors/", "Overwrite");

        //Task2/3 : explode the bids, convert the currency
        bidDataFrameExploded = explodeBids(bidDataFrame);
        bidDataFrameConverted = bidDataFrameExploded
                .join(rateDataFrame, rateDataFrame.col("ValidFrom").equalTo(bidDataFrameExploded.col("date"))
                        , "INNER")
                .select(col("date"),
                        col("motelId"),
                        col("LoSa"),
                        lit(col("price").multiply(col("ExchangeRate")).alias("price"))
                )
                .withColumn("price",round(col("price"),4));


        bidDataFrameConverted.show();


        //save results
        //saveCsv(bidDataFrameExploded,"./output/exploded/", "Overwrite");

        sc.close();

    }


    public static SparkSession establishSparkContext() {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[2]")
                .appName("hotel application")
                .getOrCreate();
        return sparkSession;
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

                                    return new BidConverted(date, motelid, country, convertedSum);

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
                    String value = w._2._1.getLoSa() + "," + w._2._1.getPrice() + "," + w._2._2;

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

                        if (Double.parseDouble(array1[1]) > Double.parseDouble(array2[1])) {
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

                            return new EnrichedItem(date, hotelid, losa, price, hotemname);

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



