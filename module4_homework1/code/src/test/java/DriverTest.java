import com.google.common.collect.ImmutableList;
import com.holdenkarau.spark.testing.JavaRDDComparisons;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;
import training.bigdata.epam.BidError;
import training.bigdata.epam.BidItem;
import training.bigdata.epam.Driver;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class DriverTest {

    SparkConf sparkConf;
    JavaSparkContext sc;

    JavaRDD<String> finalResultActual;
    JavaRDD<String> finalResultExpected;
    JavaRDD<String> errorCountsActual;

    JavaRDD<BidError> errorCountsActualCustom;
    JavaRDD<BidError> errorCountsExpected;

    JavaRDD<BidItem> actualExplodedBids;
    JavaRDD<BidItem> expectedExplodedBids;


    @Before
    public void initialize()  {

        // configure spark
        sc = Driver.establishSparkContext();

        List<String> finalResultList = Arrays.asList("8,2015-10-18 12:00:00.0,US,1.726,Sheraton Moos' Motor Inn");
        finalResultExpected = sc.parallelize(finalResultList);

        List<BidError> bidErrorList = ImmutableList.of(
                new BidError("05-21-11-2015","ERROR_ACCESS_DENIED", 1));
        errorCountsExpected = sc.parallelize(bidErrorList);


        List<BidItem> bidItemList = ImmutableList.of(
                new BidItem("11-05-08-2016","0000002","US",0.68));
        expectedExplodedBids = sc.parallelize(bidItemList);


        Driver.readData(sc);

        errorCountsActualCustom = Driver.errorCountsCustom(Driver.bids)
                .filter(s-> {
                                return s.getDate().equals("05-21-11-2015")
                                    && s.getCount().equals(1)
                                    && s.getErrorMessage().equals("ERROR_ACCESS_DENIED");
                            }
                        );

        JavaRDD<BidItem> explodedBids = Driver.explodeBids(Driver.bids);
        actualExplodedBids = explodedBids
                .filter(s-> {
                    return s.getDate().equals("11-05-08-2016")
                        && s.getMotelId().equals("0000002")
                        && s.getLoSa().equals("US")
                        && s.getPrice().equals(0.68);
                });


        JavaPairRDD<String, BidItem> explodedBidsToJoin =
                explodedBids.mapToPair( s-> new Tuple2<>(s.getDate(),s) );
        JavaPairRDD<String, Tuple2<BidItem, String>> explodedBidsJoinedWithCurrency =
                explodedBidsToJoin.join(Driver.exchangeRateMap);

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
                assertEquals(
                        errorCountsExpected.collect().get(0).getDate() +
                                errorCountsExpected.collect().get(0).getErrorMessage() +
                                errorCountsExpected.collect().get(0).getCount().toString() ,
                        errorCountsActualCustom.collect().get(0).getDate() +
                              errorCountsActualCustom.collect().get(0).getErrorMessage() +
                              errorCountsActualCustom.collect().get(0).getCount().toString()
                );
    }

    @Test
    public void compare_exployed_bids() {
                assertEquals(

                        expectedExplodedBids.collect().get(0).getDate() +
                                expectedExplodedBids.collect().get(0).getLoSa() +
                                expectedExplodedBids.collect().get(0).getMotelId() +
                                expectedExplodedBids.collect().get(0).getPrice().toString() ,
                        actualExplodedBids.collect().get(0).getDate() +
                                actualExplodedBids.collect().get(0).getLoSa() +
                                actualExplodedBids.collect().get(0).getMotelId() +
                                actualExplodedBids.collect().get(0).getPrice().toString()

                );
    }

    @Test
    public void compare_final_data_sets() {
        JavaRDDComparisons.assertRDDEquals(
                finalResultExpected, finalResultActual);
    }


}



