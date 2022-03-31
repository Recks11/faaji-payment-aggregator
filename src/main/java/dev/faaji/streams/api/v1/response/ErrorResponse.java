package dev.faaji.streams.api.v1.response;

public record ErrorResponse(int status, String reason, String request) {
    @Override
    public String toString() {
        return "Error {" +
                "status=" + status +
                ", reason='" + reason + '\'' +
                ", request='" + request + '\'' +
                '}';
    }
}
