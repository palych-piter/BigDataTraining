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
import training.bigdata.epam.*;

import java.util.List;

import static org.junit.Assert.assertEquals;


public class DriverTest {

    SparkConf sparkConf;
    JavaSparkContext sc;

    JavaRDD<EnrichedItem> finalResultActual;
    JavaRDD<EnrichedItem> finalResultExpected;
    JavaRDD<String> errorCountsActual;

    JavaRDD<BidError> errorCountsActualCustom;
    JavaRDD<BidError> errorCountsExpected;

    JavaRDD<BidItem> actualExplodedBids;
    JavaRDD<BidItem> expectedExplodedBids;


    @Before
    public void initialize()  {

        // configure spark
        sc = Driver.establishSparkContext();

        List<EnrichedItem> enrichedList = ImmutableList.of(
                new EnrichedItem("2015-10-18 12:00:00.0","8" ,"US",1.726,"Sheraton Moos' Motor Inn"));
        finalResultExpected = sc.parallelize(enrichedList);


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

        JavaRDD<BidConverted> convertedBids = Driver.convertBids(explodedBidsJoinedWithCurrency);
        //convert to JavaPairRDD for joining, the key is motelid
        JavaPairRDD<Integer, BidConverted> convertedBidsToJoin =
                convertedBids.mapToPair( s-> new Tuple2<>(s.getMotelId(),s) );

        JavaPairRDD<Integer, Tuple2<BidConverted, String>> explodedBidsJoinedWithHotels =
                convertedBidsToJoin.join(Driver.motels);

        finalResultActual = Driver.findMaximum(explodedBidsJoinedWithHotels)
                .filter(s-> s.getMotelId().equals("8") && s.getDate().equals("2015-10-18 12:00:00.0") && s.getPrice().equals(1.726));

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
                assertEquals(

                         finalResultExpected.collect().get(0).getDate() +
                                 finalResultExpected.collect().get(0).getLoSa() +
                                 finalResultExpected.collect().get(0).getMotelId() +
                                 finalResultExpected.collect().get(0).getPrice().toString() ,
                        finalResultExpected.collect().get(0).getDate() +
                                finalResultExpected.collect().get(0).getLoSa() +
                                finalResultExpected.collect().get(0).getMotelId() +
                                finalResultExpected.collect().get(0).getPrice().toString()

                );
    }

}



