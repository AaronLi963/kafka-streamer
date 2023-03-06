package com.houzz.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.Thread;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.SessionStore;

import java.time.Duration;
import java.util.*;

public class KafkaStreamer extends Thread {

    static String kafkaHost = "localhost:29092";
    static String topic = "aaron-topic";
    static String groupedTopic = "aaron-topic-grouped";

    private static String ID_DELIMITER = "-";

    public void run() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "event-aggregator");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());

        Serializer<EventAggregator> eventAggregatorSerializer = new EventAggregatorSerializer();
        Deserializer<EventAggregator> eventAggregatorDeserializer = new EventAggregatorDeserializer();
        Serde<EventAggregator> eventAggregatorSerde = Serdes.serdeFrom(eventAggregatorSerializer, eventAggregatorDeserializer);

        StreamsBuilder builder = new StreamsBuilder();
        // Define a tumbling window of 5 seconds
        TimeWindowedKStream<Object, Object> windowedStream =
                        builder.stream(topic)
                        .groupByKey()
                        .windowedBy(TimeWindows.of(Duration.ofSeconds(5)));

        // Aggregate the events in the tumbling window
        KTable<Windowed<byte[]>, byte[]> resultTable =
                windowedStream.aggregate(
                                new EventAggregator(),
                                (k, v, aggregate) -> aggregate.add(v))
                        .mapValues(aggregator -> aggregator.asByteArray());

        // Write the result to the output topic
        resultTable.toStream().to(groupedTopic);
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);
        try {
            kafkaStreams.start();
            System.out.println("streams deployed");
        } catch (Exception e) {
            System.exit(1);
        }
        System.out.println("stream closed");
    }

    public static void main(String[] args) {
        KafkaStreamer streamer = new KafkaStreamer();
        streamer.run();
    }
}