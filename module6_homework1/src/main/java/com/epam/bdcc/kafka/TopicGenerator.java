package com.epam.bdcc.kafka;

import com.epam.bdcc.htm.MonitoringRecord;
import com.epam.bdcc.utils.GlobalConstants;
import com.epam.bdcc.utils.PropertiesLoader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class TopicGenerator implements GlobalConstants {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicGenerator.class);

    public static void main(String[] args) throws Exception {
        // load a properties file from class path, inside static method
        Properties applicationProperties = PropertiesLoader.getGlobalProperties();
        if (!applicationProperties.isEmpty()) {
            final boolean skipHeader = Boolean
                    .parseBoolean(applicationProperties.getProperty(GENERATOR_SKIP_HEADER_CONFIG));
            final long batchSleep = Long.parseLong(applicationProperties.getProperty(GENERATOR_BATCH_SLEEP_CONFIG));
            final int batchSize = Integer.parseInt(applicationProperties.getProperty(BATCH_SIZE_CONFIG));
            final String sampleFolder = applicationProperties.getProperty(GENERATOR_SAMPLE_FILE_CONFIG);
            final String topicName = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);
            final String appName = applicationProperties.getProperty(SPARK_APP_NAME_CONFIG);
            final String master = applicationProperties.getProperty(MASTER);
            final long batchPeriod = Long.parseLong(applicationProperties.getProperty(SPARK_BATCH_DURATION_CONFIG));
            final String sparkSerializer = applicationProperties.getProperty(SPARK_INTERNAL_SERIALIZER_CONFIG);

            //Read the file (one_device_2015-2017.csv) and push records to Kafka raw topic.

            //initialize spark
            SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
            JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(batchPeriod));

            JavaDStream<String> lines = jssc.textFileStream(sampleFolder);

            lines.foreachRDD(rdd -> {
                rdd.foreachPartition(partition -> {
                    KafkaProducer<String, MonitoringRecord> producer = KafkaHelper.createProducer();
                    partition.forEachRemaining(p -> {

                        MonitoringRecord monitoringRecord = new MonitoringRecord();
                        String[] arrayMonitoringRecord = p.split(",");

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

                        ProducerRecord<String, MonitoringRecord> record =
                                new ProducerRecord<>(
                                        topicName, KafkaHelper.getKey(monitoringRecord), monitoringRecord
                                );
                                producer.send(record);

                    });
                });
            });

            jssc.start();
            jssc.awaitTermination();

        }
    }


    private static Properties loadProperties(String propertiesFileName) throws Exception {
        Properties properties = new Properties();
        try (InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(propertiesFileName)) {
            properties.load(inputStream);
        } catch (IOException e) {
            String msg = "Error load properties from file.";
            throw new Exception(msg);
        }
        return properties;
    }


}
