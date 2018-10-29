package com.epam.bdcc.serde;

import com.epam.bdcc.htm.MonitoringRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;

public class KafkaJsonMonitoringRecordSerDe implements Deserializer<MonitoringRecord>, Serializer<MonitoringRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJsonMonitoringRecordSerDe.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //TODO : Add implementation for configure, if needed
        //throw new UnsupportedOperationException("Add implementation for configure");
    }

    @Override
    public byte[] serialize(String topic, MonitoringRecord data) {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(bos);
            oos.writeObject(data);
            oos.flush();
            oos.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return  bos.toByteArray();

    }

    @Override
    public MonitoringRecord deserialize(String topic, byte[] data)  {

        MonitoringRecord deserializedMonitoringRecord = new MonitoringRecord();

        ByteArrayInputStream in = new ByteArrayInputStream(data);
        try {
            ObjectInputStream is = new ObjectInputStream(in);
            deserializedMonitoringRecord = (MonitoringRecord) is.readObject();
            is.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return deserializedMonitoringRecord;

    }

    @Override
    public void close() {
        //TODO : Add implementation for close, if needed
        //throw new UnsupportedOperationException("Add implementation for close");
    }
}