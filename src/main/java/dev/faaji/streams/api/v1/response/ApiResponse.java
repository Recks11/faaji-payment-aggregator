package dev.faaji.streams.api.v1.response;

public record ApiResponse<T>(int code, T data, boolean error) {
}
