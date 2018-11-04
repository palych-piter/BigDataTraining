package com.epam.bdcc.spark;

import com.epam.bdcc.htm.HTMNetwork;
import com.epam.bdcc.htm.MonitoringRecord;
import com.epam.bdcc.htm.ResultState;
import com.epam.bdcc.kafka.KafkaHelper;
import com.epam.bdcc.utils.GlobalConstants;
import com.epam.bdcc.utils.PropertiesLoader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import scala.Tuple2;

import java.util.*;

import static com.epam.bdcc.utils.PropertiesLoader.getKafkaConsumerProperties;

public class AnomalyDetector implements GlobalConstants {
    /**
     * TODO :
     * 1. Define Spark configuration (register serializers, if needed)
     * 2. Initialize streaming context with checkpoint directory
     * 3. Read records from kafka topic "monitoring20" (com.epam.bdcc.kafka.KafkaHelper can help)
     * 4. Organized records by key and map with HTMNetwork state for each device
     * (com.epam.bdcc.kafka.KafkaHelper.getKey - unique key for the device),
     * for detecting anomalies and updating HTMNetwork state (com.epam.bdcc.spark.AnomalyDetector.mappingFunc can help)
     * 5. Send enriched records to topic "monitoringEnriched2" for further visualization
     **/
    public static void main(String[] args) throws Exception {
        //load a properties file from class path, inside static method
        final Properties applicationProperties = PropertiesLoader.getGlobalProperties();
        if (!applicationProperties.isEmpty()) {
            final String appName = applicationProperties.getProperty(SPARK_APP_NAME_CONFIG);
            final String rawTopicName = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);
            final String enrichedTopicName = applicationProperties.getProperty(KAFKA_ENRICHED_TOPIC_CONFIG);
            final String checkpointDir = applicationProperties.getProperty(SPARK_CHECKPOINT_DIR_CONFIG);
            final Duration batchDuration = Duration.apply(Long.parseLong(applicationProperties.getProperty(SPARK_BATCH_DURATION_CONFIG)));
            final Duration checkpointInterval = Duration.apply(Long.parseLong(applicationProperties.getProperty(SPARK_CHECKPOINT_INTERVAL_CONFIG)));
            final String sampleFolder = applicationProperties.getProperty(GENERATOR_SAMPLE_FILE_CONFIG);
            final String master = applicationProperties.getProperty(MASTER);


            //establish the Spark context
            SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);

            //tuning kafka to avoid a batch processing delay
            conf.set("spark.streaming.backpressure.enabled","true");
            conf.set("spark.streaming.kafka.maxRatePerPartition","80");

            //establish the streaming context
            JavaStreamingContext jssc = new JavaStreamingContext(conf, batchDuration);
            //set checkpoit directory
            jssc.checkpoint(checkpointDir);


            //initialize the topic name to pass to pass to the stream initialization
            Set<String> topicsSet = new HashSet<>(Arrays.asList(rawTopicName.split(",")));


            //initiate the stream
            JavaDStream<ConsumerRecord<String, MonitoringRecord>> lines = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(topicsSet, getKafkaConsumerProperties())
            );


            //convert to pair rdd streaming to apply the mapWithState in the next step
            JavaPairDStream<String, MonitoringRecord> pairLines = lines.mapToPair(
                    new PairFunction<ConsumerRecord<String, MonitoringRecord>,String,MonitoringRecord> (){
                        @Override
                        public Tuple2<String, MonitoringRecord> call(ConsumerRecord<String, MonitoringRecord> s){
                            return new Tuple2<>(s.key(), s.value()) ;                       }
            });


            //applying a function, detecting anomalies
            JavaMapWithStateDStream<String, MonitoringRecord, HTMNetwork, MonitoringRecord>
                    stateLines = pairLines.mapWithState(StateSpec.function(mappingFunc));


            // putting enriched records into the enriched topic
            stateLines.foreachRDD(rdd -> {
                rdd.foreachPartition(partition -> {
                    KafkaProducer<String, MonitoringRecord> producer = KafkaHelper.createProducer();
                    //record is a Producer Record
                    partition.forEachRemaining( record -> {
                        ProducerRecord<String, MonitoringRecord> monitoringRecord =
                                new ProducerRecord<>(
                                        enrichedTopicName, KafkaHelper.getKey(record), record
                                );
                        producer.send(monitoringRecord);

                    });
                    // release kafka resources
                    producer.close();
                });
            });

            jssc.start();
            jssc.awaitTermination();

        }
    }

    private static Function3<String, Optional<MonitoringRecord>, State<HTMNetwork>, MonitoringRecord> mappingFunc =
            (deviceID, recordOpt, state) -> {
                // case 0: timeout
                if (!recordOpt.isPresent())
                    return null;

                // either new or existing device
                if (!state.exists())
                    state.update(new HTMNetwork(deviceID));
                HTMNetwork htmNetwork = state.get();
                String stateDeviceID = htmNetwork.getId();
                if (!stateDeviceID.equals(deviceID))
                    throw new Exception("Wrong behaviour of Spark: stream key is $deviceID%s, while the actual state key is $stateDeviceID%s");
                MonitoringRecord record = recordOpt.get();

                // get the value of DT and Measurement and pass it to the HTM
                HashMap<String, Object> m = new java.util.HashMap<>();
                m.put("DT", DateTime.parse(record.getDateGMT() + " " + record.getTimeGMT(), DateTimeFormat.forPattern("YY-MM-dd HH:mm")));
                m.put("Measurement", Double.parseDouble(record.getSampleMeasurement()));
                ResultState rs = htmNetwork.compute(m);
                record.setPrediction(rs.getPrediction());
                record.setError(rs.getError());
                record.setAnomaly(rs.getAnomaly());
                record.setPredictionNext(rs.getPredictionNext());

                return record;
            };

}