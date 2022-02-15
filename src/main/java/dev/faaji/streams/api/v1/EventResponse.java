package dev.faaji.streams.api.v1;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record EventResponse<T>(String id, T data, Map<String, Object> meta) {

    static <T> EventResponse<T> withMetadata(String id, T data, Map<String, Object> meta) {
        return new EventResponse<>(id, data, meta);
    }
    static <T> EventResponse<T> from(String id, T data) {
        return new EventResponse<>(id, data, null);
    }
}
