package dev.faaji.streams.api.v1.domain;

import java.util.Arrays;

public record UserRegistration(String userId, String[] interests, String gender, String eventId) {

    @Override
    public String toString() {
        return "UserRegistration {" +
                "userId='" + userId + '\'' +
                ", interests=" + Arrays.toString(interests) +
                ", gender='" + gender + '\'' +
                ", eventId='" + eventId + '\'' +
                '}';
    }
}
