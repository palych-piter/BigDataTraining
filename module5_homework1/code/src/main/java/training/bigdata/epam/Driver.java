package training.bigdata.epam;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.io.FileNotFoundException;

import static org.apache.spark.sql.functions.*;
import static training.bigdata.epam.ExplodeBids.explodeBids;
import static training.bigdata.epam.ReadBidData.readBidData;
import static training.bigdata.epam.ReadErrorData.readErrorData;
import static training.bigdata.epam.ReadHotelData.readHotelData;
import static training.bigdata.epam.ReadRateData.readRateData;
import static training.bigdata.epam.SaveCSV.saveCsv;
import static training.bigdata.epam.ConstantsLoader.Constants;


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
                .join(rateDataFrame, col("ValidFrom").equalTo(col("date"))
                        , "INNER")
                .select(col("date"),
                        col("motelId"),
                        col("LoSa"),
                        lit(col("price").multiply(col("ExchangeRate")).alias("price"))
                )
                .withColumn("price", round(col("price"), 4))
        ;


        //Task 4/5 : enrich the data with hotel names + find maximum

        //group to fins maximum value
        bidDataFrameGrouped = bidDataFrameConverted
                .groupBy(col("date"), col("motelId"))
                .agg(max(col("price")).alias("price"))
        ;


        bidDataFrameFinal = bidDataFrameGrouped
                //join to get all records that have the maximum value
                .join(bidDataFrameConverted,
                        bidDataFrameGrouped.col("date").equalTo(bidDataFrameConverted.col("date"))
                                .and(bidDataFrameGrouped.col("motelId").equalTo(bidDataFrameConverted.col("motelId")))
                                .and(bidDataFrameGrouped.col("price").equalTo(bidDataFrameConverted.col("price")))
                )
                .select(
                        bidDataFrameGrouped.col("date"),
                        bidDataFrameGrouped.col("motelId"),
                        bidDataFrameGrouped.col("price"),
                        col("LoSa")
                )
                //join to enrich the date with hotel names
                .join(hotelDataFrame, col("motelId").equalTo(col("MotelID")), "inner"
                )
                .select(
                        date_format(to_timestamp(col("date"),Constants.dateFormatInput),Constants.dateFormatOutput).alias("date"),
                        col("MotelName"),
                        col("LoSa"),
                        col("price")
                )
        ;

        //save results
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

}



