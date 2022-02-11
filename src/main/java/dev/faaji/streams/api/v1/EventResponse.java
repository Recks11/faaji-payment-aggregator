package dev.faaji.streams.api.v1;

public record EventResponse<T>(String id, T data) {
}
