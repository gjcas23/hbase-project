package com.xwtech.kafka;

import com.xwtech.util.LocationConfig;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import com.xwtech.util.HBaseUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class Consumer2Hbase {

    public static void main(String[] args) {
        Properties properties = new Properties() {{
            put("bootstrap.servers", "golden-02:9092");
            put("group.id", "testConsumer");
            put("enable.auto.commit", "true");
            put("auto.commit.interval.ms", "1000");
            put("session.timeout.ms", "30000");
            put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        }};
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("test1"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            for (ConsumerRecord<String, String> record : records) {
                //System.out.printf("topic = %s, offset = %d, key = %s, value = %s" + "\n",
                //        record.topic(), record.offset(), record.key(), record.value());
                Connection conn  = HBaseUtil.getConnection();
                HTable hbaseTable = HBaseUtil.getHTable(LocationConfig.hTable(), conn);
                hbaseTable.setAutoFlushTo(false);
                String[] arr = record.value().split("\\|");
                String rowkey = arr[0] + "_" + arr[1] ;
                System.out.println(rowkey);
                Put put = new Put(Bytes.toBytes(rowkey));
                put.addColumn(Bytes.toBytes("track-family"),
                        Bytes.toBytes("track"), Bytes.toBytes(record.value()));
                try {
                    hbaseTable.put(put);
                    hbaseTable.flushCommits();
                } catch (IOException e) {
                    e.printStackTrace();
                }  finally {
                    HBaseUtil.closeConnect(conn);
                }
            }
        }
    }
}
