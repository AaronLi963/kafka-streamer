package com.houzz.common;

import java.lang.*;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
public class Producer extends Thread {
    static String kafkaHost = "localhost:29092";
    static String topic = "aaron-topic";

    public void run() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties);
        int counter = 0;

        while (true) {
            try {
                String key = "key";
                String value = "val" + String.valueOf(counter);
                ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topic, key.getBytes() , value.getBytes());
                producer.send(producerRecord);
                producer.flush();
                System.out.printf("key: %s, value: %s sent\n", key, value);
                counter++;
                Thread.sleep(1000);
            } catch (Exception e) {
                System.out.printf("producer exception: %s", e.toString());
            }
        }
        //producer.close();
    }

    public static void main(String[] args) {
        Producer producer = new Producer();
        producer.run();
    }
}
