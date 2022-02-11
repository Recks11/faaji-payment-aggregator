package dev.faaji.streams.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.List;
import java.util.Map;

public class ArrayListSerde<I> implements Serde<List<I>> {
    private final Serde<List<I>> inner;

    public ArrayListSerde() {
        inner = new Serdes.WrapperSerde<>(new ArrayListSerializer<>(), new ArrayListDeserializer<>());
    }

    public ArrayListSerde(Serde<List<I>> inner) {
        this.inner = inner;
    }

    @Override
    public Serializer<List<I>> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<List<I>> deserializer() {
        return inner.deserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serde.super.configure(configs, isKey);
    }

    @Override
    public void close() {
        inner.serializer().close();
        inner.deserializer().close();
    }
}
