package com.xwtech.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

public class Producer {
    public static void main(String[] args) {
        Properties properties = new Properties() {{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "golden-02:9092");
            put(ProducerConfig.ACKS_CONFIG, "all");
            put(ProducerConfig.RETRIES_CONFIG, 0);
            put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
            put(ProducerConfig.LINGER_MS_CONFIG, 1);
            put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        }};
        KafkaProducer<Object, String> producer = new KafkaProducer<>(properties);
        try {
            String path = "/home/gold/work/input/test-topic/111";
            File file = new File(path);
            InputStream in = new FileInputStream(file);
            InputStreamReader isr =new InputStreamReader(in, "UTF-8");
            BufferedReader br = new BufferedReader(isr);
            String line="";
            while ((line=br.readLine())!=null) {
                System.out.println(line);
                producer.send(new ProducerRecord<>("test1",line));
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            br.close();
            isr.close();
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

