package dev.faaji.streams.service;

public interface EventProcessor<T, R> {

    R process(T event);
}
