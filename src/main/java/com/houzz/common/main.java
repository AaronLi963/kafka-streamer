package com.houzz.common;

import com.houzz.common.*;
import java.lang.Thread;

public class main {
    public static void main(String[] args) {
        Producer producer = new Producer();
        producer.start();
        Consumer consumer = new Consumer();
        consumer.start();
        KafkaStreamer streamer = new KafkaStreamer();
        streamer.start();
    }

}
