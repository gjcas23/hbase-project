package com.xwtech.kafka;

import com.xwtech.util.HBaseUtil;
import com.xwtech.util.LocationConfig;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class Consumer2Hbase1 {

    public static void main(String[] args) {
        Connection conn  = HBaseUtil.getConnection();
        System.out.println(LocationConfig.hTable());
        HTable hbaseTable = HBaseUtil.getHTable(LocationConfig.hTable(), conn);
        hbaseTable.setAutoFlushTo(false);
        String rowkey = "1111111111" ;
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes("track-family"),
                Bytes.toBytes("track"), Bytes.toBytes("aaaaaaaa"));
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
