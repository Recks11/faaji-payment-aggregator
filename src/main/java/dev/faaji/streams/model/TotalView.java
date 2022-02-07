package dev.faaji.streams.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import dev.faaji.streams.serialization.CurrencySerializer;

import java.math.BigDecimal;

public record TotalView(String userId, String partyId,
                        @JsonSerialize(using = CurrencySerializer.class) BigDecimal total) {

    public static TotalView from(TotalView other) {
        return new TotalView(other.userId, other.partyId, other.total);
    }

    @Override
    public String toString() {
        return "{ \"userId\": \"%s\", \"partyId\": \"%s\" , \"total\": \"%s\"}".formatted(userId, partyId, total);
    }
}
