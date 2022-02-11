package dev.faaji.streams.api.v1.domain;

public class PartyModification {
    private String eventId;

    public PartyModification() {
    }

    public PartyModification(String eventId) {
        this.eventId = eventId;
    }

    public String getEventId() {
        return eventId;
    }
}