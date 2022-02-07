package dev.faaji.streams.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import dev.faaji.streams.serialization.CurrencySerializer;

import java.math.BigDecimal;
import java.math.RoundingMode;

public record Payment(String reference,
                      String paidAt,
                      String platform,
                      @JsonSerialize(using = CurrencySerializer.class) BigDecimal amount,
                      String status,
                      String userId,
                      String partyId) {

    public static Payment from(Payment other) {
        return new Payment(other.reference(), other.paidAt(),
                other.platform(),
                other.amount().setScale(2, RoundingMode.HALF_UP),
                other.status(),
                other.userId(), other.partyId());
    }
}
