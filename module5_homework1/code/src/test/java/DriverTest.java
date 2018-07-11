
import com.holdenkarau.spark.testing.JavaRDDComparisons;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import training.bigdata.epam.Driver;

import java.util.Arrays;
import java.util.List;

import static training.bigdata.epam.Driver.convertCurrency;
import static training.bigdata.epam.Driver.findMaxPrice;
import static training.bigdata.epam.ExplodeBids.explodeBids;
import static training.bigdata.epam.ReadBidData.readBidData;
import static training.bigdata.epam.ReadErrorData.readErrorData;
import static training.bigdata.epam.ReadHotelData.readHotelData;
import static training.bigdata.epam.ReadRateData.readRateData;


public class DriverTest {

    SparkSession sc;
    Dataset<Row> bidDataFrame;
    Dataset<Row> errorDataFrame;
    Dataset<Row> errorDataFrameGrouped;
    Dataset<Row> rateDataFrame;
    Dataset<Row> hotelDataFrame;
    Dataset<Row> bidDataFrameExploded;
    Dataset<Row> bidDataFrameConverted;
    Dataset<Row> bidDataFrameFinal;

    @Before
    public void initialize() {

        // configure spark
        sc = Driver.establishSparkContext();

        //read the test data
        bidDataFrame = readBidData(sc, "bids_test.txt");
        errorDataFrame = readErrorData(sc, "bids_test.txt");
        rateDataFrame = readRateData(sc);
        hotelDataFrame = readHotelData(sc);

        errorDataFrameGrouped = Driver.countErrors(errorDataFrame);
        bidDataFrameExploded = explodeBids(bidDataFrame);
        bidDataFrameConverted = convertCurrency(bidDataFrameExploded,rateDataFrame);
        bidDataFrameFinal = findMaxPrice(bidDataFrameConverted,hotelDataFrame);

    }

    @After
    public void tearDown() {
        sc.close();
    }

    @Test
    public void compare_error_counts() {
        //create expected dataset
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc.sparkContext());
        List<String> listExpected = Arrays.asList("22-04-08-2016,ERROR_INCONSISTENT_DATA,2");
        JavaRDD<String> errorDataExpected = jsc.parallelize(listExpected, 1);

        //convert actual dataset to JavaRDD<String> to compare
        JavaRDD<String> errorDataActual = errorDataFrameGrouped.toJavaRDD()
                .map((Row row) -> {
                    return row.get(0).toString() + "," +
                           row.get(1).toString() + "," +
                           row.get(2).toString();
                });
        JavaRDDComparisons.assertRDDEquals(errorDataExpected, errorDataActual);
    }

    @Test
    public void compare_exployed_bids() {
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc.sparkContext());
        List<String> listExpected = Arrays.asList("21-04-08-2016,0000004,MX,1.79","21-04-08-2016,0000004,CA,0.00","21-04-08-2016,0000004,US,1.60");
        JavaRDD<String> explodedBidsDataExpected = jsc.parallelize(listExpected, 1);

        //convert actual dataset to JavaRDD<String> to compare
        JavaRDD<String> errorDataActual = bidDataFrameExploded.toJavaRDD()
                .map((Row row) -> {
                    return row.get(0).toString() + "," +
                           row.get(1).toString() + "," +
                           row.get(2).toString() + "," +
                           row.get(3).toString();
                });
        JavaRDDComparisons.assertRDDEquals(explodedBidsDataExpected, errorDataActual);
    }


    @Test
    public void compare_converted_bids() {
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc.sparkContext());
        List<String> listExpected = Arrays.asList("21-04-08-2016,0000004,MX,1.6701","21-04-08-2016,0000004,CA,0.0","21-04-08-2016,0000004,US,1.4928");
        JavaRDD<String> explodedBidsDataExpected = jsc.parallelize(listExpected, 1);

        //convert actual dataset to JavaRDD<String> to compare
        JavaRDD<String> errorDataActual = bidDataFrameConverted.toJavaRDD()
                .map((Row row) -> {
                    return row.get(0).toString() + "," +
                            row.get(1).toString() + "," +
                            row.get(2).toString() + "," +
                            row.get(3).toString();
                });
        JavaRDDComparisons.assertRDDEquals(explodedBidsDataExpected, errorDataActual);
    }



    @Test
    public void compare_final_data_sets() {
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc.sparkContext());
        List<String> listExpected = Arrays.asList("2016-08-04 21:00:00.0,Majestic Big River Elegance Plaza,MX,1.6701");
        JavaRDD<String> explodedBidsDataExpected = jsc.parallelize(listExpected, 1);

        //convert actual dataset to JavaRDD<String> to compare
        JavaRDD<String> errorDataActual = bidDataFrameFinal.toJavaRDD()
                .map((Row row) -> {
                    return row.get(0).toString() + "," +
                            row.get(1).toString() + "," +
                            row.get(2).toString() + "," +
                            row.get(3).toString();
                });
        JavaRDDComparisons.assertRDDEquals(explodedBidsDataExpected, errorDataActual);
    }


}



