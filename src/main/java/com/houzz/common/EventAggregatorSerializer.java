package com.houzz.common;

import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class EventAggregatorSerializer implements Serializer<EventAggregator> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public byte[] serialize(String topic, EventAggregator eventAggregator) {
        if (eventAggregator == null) {
            return null;
        }

        try {
            return eventAggregator.asByteArray();
        } catch (RuntimeException e) {
            throw new SerializationException("Error serializing value", e);
        }

    }
}
