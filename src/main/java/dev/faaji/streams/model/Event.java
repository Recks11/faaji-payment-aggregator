package dev.faaji.streams.model;

public interface Event<D> {
    String getType();
    D getData();
}
