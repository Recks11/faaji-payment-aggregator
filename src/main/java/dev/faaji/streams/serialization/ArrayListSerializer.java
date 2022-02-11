package dev.faaji.streams.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import reactor.core.Exceptions;

import java.util.List;
import java.util.Map;

public class ArrayListSerializer<I> implements Serializer<List<I>> {
    private final ObjectMapper mapper = new ObjectMapper();
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, List<I> data) {
        try {
            return mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw Exceptions.propagate(e);
        }
    }

    @Override
    public byte[] serialize(String topic, Headers headers, List<I> data) {
        return serialize(topic, data);
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
