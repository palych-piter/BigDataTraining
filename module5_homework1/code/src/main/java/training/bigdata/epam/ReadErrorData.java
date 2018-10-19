package training.bigdata.epam;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

import java.util.Collections;



public class ReadErrorData {

    public static Dataset<Row> readErrorData(SparkSession spark, String fileName) {

        // provide path to input text files
        String bidsPath = Driver.class.getResource("/" + fileName).getPath();

        // read bid text files to RDD
        JavaRDD<BidError> errorRDD = spark.sparkContext()
                .textFile(bidsPath, 1).toJavaRDD()
                .filter(s -> s.split(",")[2].contains("ERROR"))
                .map(s -> {
                    String[] arrayError = s.split(",");
                    BidError bidError = new BidError();
                    bidError.setDate(arrayError[1]);
                    bidError.setErrorMessage(arrayError[2].trim());
                    bidError.setCount(1);
                    return bidError;
                });


        //test the typed dataset API
        BidError bidError = new BidError();
        bidError.setErrorMessage("test_error");
        Encoder<BidError> outerClassEncoder = Encoders.bean(BidError.class);
        Dataset<BidError> test = spark.createDataset(Collections.singletonList(bidError), outerClassEncoder);

        //UnTyped - using col function, when using col function casting Row will be enabled this is why untyped
        Dataset<BidError> testUntyped = test.filter(col("errorMessage").equalTo("test_error"));
        //Typed - using lambda
        Dataset<BidError> testTyped = test.filter((BidError be) -> be.getErrorMessage().contains("test_error"));

        //example - query files directly from files
        Dataset<Row> files = spark.sql("SELECT * FROM parquet.`./output/bids.parquet`");

        //example - working with udfs

        //register a new internal UDF
        spark.udf().register("xToUpperCase", new UDF1<String, String>() {
            @Override
            public String call(String x) {
                return x.toUpperCase();
            }
        }, DataTypes.StringType);
        //apply the udf
        Dataset<Row> files1 = files.select(callUDF("xToUpperCase",col("MotelID")));

        //take a look at the physical plan
        files1.explain();

        return spark.createDataFrame(errorRDD, BidError.class);

    }

}
