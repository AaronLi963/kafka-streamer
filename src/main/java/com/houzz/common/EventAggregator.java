package com.houzz.common;
import org.apache.kafka.streams.kstream.Aggregator;
import com.google.gson.Gson;
import java.lang.reflect.Type;
import com.google.gson.reflect.TypeToken;


import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class EventAggregator implements Aggregator<byte[], byte[], EventAggregator>{
    ArrayList<EmailEvent> events = new ArrayList<>();
    Gson gson = new Gson();

    public EventAggregator() {

    }

    public EventAggregator(String jsonString) {
        ArrayList<EmailEvent> events = gson.fromJson(jsonString, new TypeToken<List<EmailEvent>>(){}.getType());
        this.events = events;
    }

    public EventAggregator(byte[] bytes) {
        this(new String(bytes));
    }

    public EventAggregator add(byte[] value) {
        String stringVal = new String(value);
        return this.add(stringVal);
    }

    public EventAggregator add(String stringValue) {
        String stringVal = new String(stringValue);
        EmailEvent event = gson.fromJson(stringVal, EmailEvent.class);
        this.events.add(event);
        return this;
    }

    public String asJsonString() {
        return gson.toJson(this.events);
    }

    public byte[] asByteArray() {
        return asJsonString().getBytes(StandardCharsets.UTF_8);
    }

    public EventAggregator apply(byte[] k, byte[] v, EventAggregator aggregator) {
        return aggregator.add(v);
    }
}