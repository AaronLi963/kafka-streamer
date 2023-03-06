package com.houzz.common;

import java.lang.Thread;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class Consumer extends Thread {
    static String kafkaHost = "localhost:29092";
    static String topic = "aaron-topic-grouped";
    static String groupId = "aaron";

    public void run() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        // poll for new data
        System.out.println("start consuming events");
        while(true){
            ConsumerRecords<byte[], byte[]> records =
                    consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<byte[], byte[]> record : records){
                System.out.println("Key: " + record.key() + ", Value: " + record.value());
                System.out.println("Partition: " + record.partition() + ", Offset:" + record.offset());
            }
        }
    }

    public static void main(String[] args) {
        Consumer consumer = new Consumer();
        consumer.run();
    }
}
