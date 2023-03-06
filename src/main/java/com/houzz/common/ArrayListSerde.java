package com.houzz.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import java.util.ArrayList;
import java.util.Map;

public class ArrayListSerde<T> implements Serde<ArrayList<T>> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public void close() {}

    @Override
    public Serializer<ArrayList<T>> serializer() {
        return new Serializer<ArrayList<T>>() {
            @Override
            public byte[] serialize(String topic, ArrayList<T> data) {
                try {
                    return mapper.writeValueAsBytes(data);
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
        };
    }

    @Override
    public Deserializer<ArrayList<T>> deserializer() {
        return new Deserializer<ArrayList<T>>() {
            @Override
            public ArrayList<T> deserialize(String topic, byte[] data) {
                try {
                    return mapper.readValue(data, new TypeReference<ArrayList<T>>() {});
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
        };
    }
}
