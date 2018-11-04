package com.epam.bdcc.kafka;

import com.epam.bdcc.htm.MonitoringRecord;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class MonitoringRecordPartitioner extends DefaultPartitioner {
    private static final Logger LOGGER = LoggerFactory.getLogger(MonitoringRecordPartitioner.class);

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (value instanceof MonitoringRecord) {

            //use timestamps as values for partitioning records,
            //just using the hashcode() function returns the same code for records
            List partitions = cluster.availablePartitionsForTopic(topic);
            int partition = Math.abs((((MonitoringRecord) value).getDateGMT() + ((MonitoringRecord) value).getTimeGMT()).toString().hashCode() % partitions.size());

            return partition;

        } else {
            return super.partition(topic, key, keyBytes, value, valueBytes, cluster);
        }
    }

    public void close() {
    }

    public void configure(Map<String, ?> map) {
    }
}