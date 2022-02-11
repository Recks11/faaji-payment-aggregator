package dev.faaji.streams.api.v1.domain;

public record User(String id,
                   String gender,
                   String[] interests) {
}

