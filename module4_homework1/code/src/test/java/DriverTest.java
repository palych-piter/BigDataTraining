import com.holdenkarau.spark.testing.JavaRDDComparisons;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;
import training.bigdata.epam.Driver;

import java.util.Arrays;
import java.util.List;


public class DriverTest {

    SparkConf sparkConf;
    JavaSparkContext sc;

    JavaRDD<String> finalResultActual;
    JavaRDD<String> finalResultExpected;
    JavaRDD<String> errorCountsActual;
    JavaRDD<String> errorCountsExpected;
    JavaRDD<String> actualExplodedBids;
    JavaRDD<String> expectedExplodedBids;


    @Before
    public void initialize()  {

        // configure spark
        sc = Driver.establishSparkContext();

        List<String> finalResultList = Arrays.asList("8,2015-10-18 12:00:00.0,US,1.726,Sheraton Moos' Motor Inn");
        finalResultExpected = sc.parallelize(finalResultList);

        List<String> errorCounttList = Arrays.asList("05-21-11-2015,ERROR_ACCESS_DENIED,1");
        errorCountsExpected = sc.parallelize(errorCounttList);

        List<String> expectedBidstList = Arrays.asList("11-05-08-2016,0000002,US,0.68");
        expectedExplodedBids = sc.parallelize(expectedBidstList);

        Driver.readData(sc);

        errorCountsActual = Driver.errorCounts(Driver.bids)
                .map(s-> s._1 + "," + s._2)
                .filter(s-> {
                     String [] array = s.split(",");
                     return array[0].equals("05-21-11-2015")
                             && array[1].equals("ERROR_ACCESS_DENIED");
                });

        JavaPairRDD<String,String> explodedBids = Driver.explodeBids(Driver.bids);
        actualExplodedBids = explodedBids
                .map(s-> s._1 + "," + s._2)
                .filter(s-> {
                    String [] array = s.split(",");
                    return array[0].equals("11-05-08-2016")
                            && array[1].equals("0000002")
                            && array[2].equals("US");
                });

        JavaPairRDD<String, Tuple2<String, String>> explodedBidsJoinedWithCurrency =
                explodedBids.join(Driver.exchangeRateMap);

        JavaPairRDD<Integer, String> convertedBids = Driver.convertBids(explodedBidsJoinedWithCurrency);

        JavaPairRDD<Integer, Tuple2<String, String>> explodedBidsJoinedWithHotels =
                convertedBids.join(Driver.motels);

        finalResultActual = Driver.findMaximum(explodedBidsJoinedWithHotels)
                .map(s-> s._1 + "," + s._2).filter(s-> s.split(",")[0].equals("8") && s.split(",")[1].equals("2015-10-18 12:00:00.0"));


    }

    @After
    public void tearDown() {
        sc.close();
    }


    @Test
    public void compare_error_counts() {
        JavaRDDComparisons.assertRDDEquals(
                errorCountsExpected, errorCountsActual);
    }

    @Test
    public void compare_exployed_bids() {
        JavaRDDComparisons.assertRDDEquals(
                expectedExplodedBids, actualExplodedBids);
    }

    @Test
    public void compare_final_data_sets() {
        JavaRDDComparisons.assertRDDEquals(
                finalResultExpected, finalResultActual);
    }


}



