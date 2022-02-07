package dev.faaji.streams.model;

public enum Events {
    REPLACE("REPLACE"),
    CREDIT("CREDIT"),
    DEBIT("DEBIT");
    private final String value;

    Events(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return getValue();
    }
}
