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
            final String sampleFolder = applicationProperties.getProperty(GENERATOR_SAMPLE_FILE_CONFIG);
            final String topicName = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);
            final String appName = applicationProperties.getProperty(SPARK_APP_NAME_CONFIG);
            final String master = applicationProperties.getProperty(MASTER);
            final Duration batchDuration = Duration.apply(Long.parseLong(applicationProperties.getProperty(SPARK_BATCH_DURATION_CONFIG)));


            //Read the file (one_device_2015-2017.csv) and push records to Kafka raw topic.

            //initialize spark
            SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
            JavaStreamingContext jssc = new JavaStreamingContext(conf, batchDuration);

            JavaDStream<String> lines = jssc.textFileStream(sampleFolder);

            lines.foreachRDD(rdd -> {
                rdd.foreachPartition(partition -> {
                    KafkaProducer<String, MonitoringRecord> producer = KafkaHelper.createProducer();
                    partition.forEachRemaining(p -> {

                        //for each line from a file create a MonitoringRecord object and initialize values
                        String[] arrayMonitoringRecord = p.split(",");
                        MonitoringRecord monitoringRecord = new MonitoringRecord(arrayMonitoringRecord);

                        //initialize a producer record to send
                        ProducerRecord<String, MonitoringRecord> record =
                                new ProducerRecord<>(
                                        topicName, KafkaHelper.getKey(monitoringRecord), monitoringRecord
                                );
                        //send the record
                        producer.send(record);

                    });
                    // release kafka resources
                    producer.close();

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
