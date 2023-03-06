package com.houzz.common;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class EventAggregatorDeserializer implements Deserializer<EventAggregator>{

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public EventAggregator deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        try {
            return new EventAggregator(bytes);
        } catch (RuntimeException e) {
            throw new SerializationException("Error deserializing value", e);
        }

    }
}
