package com.epam.bdcc.spark;

import com.epam.bdcc.htm.HTMNetwork;
import com.epam.bdcc.htm.MonitoringRecord;
import com.epam.bdcc.htm.ResultState;
import com.epam.bdcc.kafka.KafkaHelper;
import com.epam.bdcc.utils.GlobalConstants;
import com.epam.bdcc.utils.PropertiesLoader;
import com.holdenkarau.spark.testing.JavaStreamingSuiteBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.*;
import java.util.*;

import static com.epam.bdcc.spark.AnomalyDetector.*;



class AnomalyDetectorTest extends JavaStreamingSuiteBase implements GlobalConstants, Serializable {

    String sampleFolder;
    transient JavaStreamingContext jssc;
    String rawTopicName;

    @org.junit.jupiter.api.BeforeEach
    void setUp() {

        //load a properties file from class path, inside static method
        final Properties applicationProperties = PropertiesLoader.getGlobalProperties();
        if (!applicationProperties.isEmpty()) {
            final String appName = applicationProperties.getProperty(SPARK_APP_NAME_CONFIG);
            rawTopicName = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);
            final String checkpointDir = applicationProperties.getProperty(SPARK_CHECKPOINT_DIR_CONFIG);
            final long batchPeriod = Long.parseLong(applicationProperties.getProperty(SPARK_BATCH_DURATION_CONFIG));
            final String master = applicationProperties.getProperty(MASTER);
            sampleFolder = applicationProperties.getProperty(GENERATOR_SAMPLE_FILE_CONFIG);

            SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
            jssc = new JavaStreamingContext(conf, new Duration(batchPeriod));

            //set checkpoit directory
            //jssc.checkpoint(checkpointDir);

            Set<String> rawTopicsSet = new HashSet<>(Arrays.asList(rawTopicName.split(",")));
            //Set<String> rawTopicsSet = new HashSet<>(Arrays.asList(rawTopicName.split(",")));

        }

    }


    @org.junit.jupiter.api.Test
    void main() throws InterruptedException {

//        JavaDStream<String> linesTopicGenerator = jssc.textFileStream(sampleFolder);
//
//        linesTopicGenerator.foreachRDD(rdd -> {
//            rdd.foreachPartition(partition -> {
//                KafkaProducer<String, MonitoringRecord> producer = KafkaHelper.createProducer();
//                partition.forEachRemaining(p -> {
//                    MonitoringRecord monitoringRecord = new MonitoringRecord();
//                    ProducerRecord<String, MonitoringRecord> record =
//                            new ProducerRecord<>(
//                                    rawTopicName, KafkaHelper.getKey(monitoringRecord), monitoringRecord
//                            );
//                    producer.send(record);
//
//                });
//            });
//        });

//        jssc.start();
//        jssc.awaitTermination();

//      List<List<Integer>> input = Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4, 5));
//      List<List<Integer>> expectedOutput = Arrays.asList(Arrays.asList(-2), Arrays.asList(4));


        //generate output monitoring record object to compare
        MonitoringRecord monitoringRecord = new MonitoringRecord();
        String line;
        try {
            BufferedReader br = new BufferedReader(new FileReader("C:/abespalov/gitHubRepositories/git-palych-piter/BigDataTraining/module6_homework1/unittest_data/unit_test.csv"));
            while ((line = br.readLine()) != null) {
                String[] arrayMonitoringRecord = line.split(",");

                monitoringRecord.setStateCode(arrayMonitoringRecord[0]);
                monitoringRecord.setCountyCode(arrayMonitoringRecord[1]);
                monitoringRecord.setSiteNum(arrayMonitoringRecord[2]);
                monitoringRecord.setParameterCode(arrayMonitoringRecord[3]);
                monitoringRecord.setPoc(arrayMonitoringRecord[4]);
                monitoringRecord.setLatitude(arrayMonitoringRecord[5]);
                monitoringRecord.setLongitude(arrayMonitoringRecord[6]);
                monitoringRecord.setDatum(arrayMonitoringRecord[7]);
                monitoringRecord.setParameterName(arrayMonitoringRecord[8]);
                monitoringRecord.setDateLocal(arrayMonitoringRecord[9]);
                monitoringRecord.setTimeLocal(arrayMonitoringRecord[10]);
                monitoringRecord.setDateGMT(arrayMonitoringRecord[11]);
                monitoringRecord.setTimeGMT(arrayMonitoringRecord[12]);
                monitoringRecord.setSampleMeasurement(arrayMonitoringRecord[13]);
                monitoringRecord.setUnitsOfMeasure(arrayMonitoringRecord[14]);
                monitoringRecord.setMdl(arrayMonitoringRecord[15]);
                monitoringRecord.setUncertainty(arrayMonitoringRecord[16]);
                monitoringRecord.setQualifier(arrayMonitoringRecord[17]);
                monitoringRecord.setMethodType(arrayMonitoringRecord[18]);
                monitoringRecord.setMethodCode(arrayMonitoringRecord[19]);
                monitoringRecord.setMethodName(arrayMonitoringRecord[20]);
                monitoringRecord.setStateName(arrayMonitoringRecord[21]);
                monitoringRecord.setCountyName(arrayMonitoringRecord[22]);
                monitoringRecord.setDateOfLastChange(arrayMonitoringRecord[23]);

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        // input and outputs are lists
        HTMNetwork inputHTM = new HTMNetwork();
        List<List<String>, List<Optional<MonitoringRecord>>, List<State<HTMNetwork>>> inputMonitoringRecords = Arrays.asList(Arrays.asList("key"), Arrays.asList(monitoringRecord), Arrays.asList(inputHTM));
        List<List<MonitoringRecord>> outputMonitoringRecords = Arrays.asList(Arrays.asList(monitoringRecord));
        testOperation(input, mappingFunc, outputMonitoringRecords);


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

