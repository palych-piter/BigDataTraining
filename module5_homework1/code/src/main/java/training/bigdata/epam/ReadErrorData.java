package training.bigdata.epam;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import training.bigdata.epam.ConstantsLoader.Constants;

public class ReadErrorData {

    public static Dataset<Row> readErrorData(SparkSession spark) {

        // provide path to input text files
        String bidsPath = Driver.class.getResource("/bids.txt").getPath();

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

        return spark.createDataFrame(errorRDD, BidError.class);

    }

}
