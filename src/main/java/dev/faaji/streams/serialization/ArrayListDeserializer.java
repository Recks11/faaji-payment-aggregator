package dev.faaji.streams.serialization;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ArrayListDeserializer<I> implements Deserializer<List<I>> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public List<I> deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, new TypeReference<ArrayList<I>>() {});
        } catch (IOException e) {
            return List.of();
        }
    }

    @Override
    public List<I> deserialize(String topic, Headers headers, byte[] data) {
        return deserialize(topic, data);
    }
}
