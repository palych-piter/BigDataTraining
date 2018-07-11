package training.bigdata.epam;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.io.FileNotFoundException;

import static org.apache.spark.sql.functions.*;
import static training.bigdata.epam.ConstantsLoader.Constants;
import static training.bigdata.epam.ExplodeBids.explodeBids;
import static training.bigdata.epam.ReadBidData.readBidData;
import static training.bigdata.epam.ReadErrorData.readErrorData;
import static training.bigdata.epam.ReadHotelData.readHotelData;
import static training.bigdata.epam.ReadRateData.readRateData;
import static training.bigdata.epam.SaveCSV.saveCsv;


public class Driver {

    public static JavaRDD<String> bids;
    public static JavaPairRDD<String, String> exchangeRateMap;
    public static JavaPairRDD<Integer, String> motels;

    public static Dataset<Row> bidDataFrame;
    public static Dataset<Row> bidDataFrameExploded;
    public static Dataset<Row> bidDataFrameConverted;
    public static Dataset<Row> bidDataFrameFinal;
    public static Dataset<Row> bidDataFrameGrouped;

    public static Dataset<Row> rateDataFrame;
    public static Dataset<Row> hotelDataFrame;
    public static Dataset<Row> errorDataFrame;

    public static SparkSession sc;

    public static void main(String[] args) throws FileNotFoundException {

        //establish Spark context
        sc = establishSparkContext();

        //read the data, initialize initial datasets
        //create a schema programmatically
        bidDataFrame = readBidData(sc,"bids.txt");
        rateDataFrame = readRateData(sc);
        hotelDataFrame = readHotelData(sc);
        //use a custom class and reflection
        errorDataFrame = readErrorData(sc,"bids.txt");


        //Task 1 : count errors
        errorDataFrame = countErrors(errorDataFrame);
        saveCsv(errorDataFrame, "./output/errors/", "Overwrite");

        //Task2/3 : explode the bids, convert the currency
        bidDataFrameExploded = explodeBids(bidDataFrame);
        bidDataFrameConverted = convertCurrency(bidDataFrameExploded, rateDataFrame);

        //Task 4/5 : find maximum + enrich the data with hotel names
        bidDataFrameFinal = findMaxPrice(bidDataFrameConverted, hotelDataFrame);
        saveCsv(bidDataFrameFinal, "./output/final/", "Overwrite");

        sc.close();

    }


    public static SparkSession establishSparkContext() {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[2]")
                .appName("hotel application")
                .getOrCreate();

        sparkSession.sql("set spark.sql.caseSensitive=true");

        return sparkSession;
    }


    public static Dataset<Row> countErrors(Dataset<Row> inputDataFrame) {
        return errorDataFrame = inputDataFrame
                .groupBy(col("date"), col("errorMessage"))
                .agg(functions.sum(col("count")).alias("count"));
    }


    public static Dataset<Row> convertCurrency(Dataset<Row> inputDataFrame, Dataset<Row> ratesDf) {
        return  inputDataFrame.join(ratesDf, col("ValidFrom").equalTo(col("date"))
                , "INNER")
                .select(col("date"),
                        col("motelId"),
                        col("LoSa"),
                        lit(col("price").multiply(col("ExchangeRate")).alias("price"))
                )
                .withColumn("price", round(col("price"), 4));
    }


    public static Dataset<Row> findMaxPrice(Dataset<Row> inputDataFrame, Dataset<Row> hotelDf ) {
        WindowSpec window = Window.partitionBy(col("date"), col("motelId")).orderBy(col("date"), col("motelId"),col("price").desc());
        Column maxPrice = functions.first("price").over(window);
        return inputDataFrame
                //find maximum
                .select(
                        col("date"),
                        col("motelId"),
                        col("LoSa"),
                        maxPrice.alias("max_price"))
                .filter(col("price").equalTo(col("max_price")))
                .join(hotelDf, col("motelId").equalTo(col("MotelID")), "inner"
                )
                //join with motels to enrich with motel names
                .select(
                        date_format(to_timestamp(col("date"), Constants.dateFormatInput), Constants.dateFormatOutput).alias("date"),
                        col("MotelName"),
                        col("LoSa"),
                        col("max_price")
                )
        ;
    }

}



