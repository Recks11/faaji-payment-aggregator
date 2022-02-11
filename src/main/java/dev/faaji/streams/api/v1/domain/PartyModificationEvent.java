package dev.faaji.streams.api.v1.domain;

import dev.faaji.streams.model.Event;

public class PartyModificationEvent implements Event<PartyModification> {
    private  String type;
    private  PartyModification data;

    public PartyModificationEvent() {
    }

    public PartyModificationEvent(String type, PartyModification data) {
        this.type = type;
        this.data = data;
    }

    @Override
    public String getType() {
        return null;
    }

    @Override
    public PartyModification getData() {
        return data;
    }
}

