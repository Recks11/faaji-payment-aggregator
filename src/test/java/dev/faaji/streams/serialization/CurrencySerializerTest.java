package dev.faaji.streams.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.faaji.streams.model.Payment;
import dev.faaji.streams.model.TotalView;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class CurrencySerializerTest {

    @Test
    void canSerializeNumbersTo2DecimalPlaces() throws Exception {
        Payment payment = new Payment(
                "",
                "",
                new BigDecimal("325.5"),
                "success",
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        );

        TotalView totalView = new TotalView(payment.userId(), payment.partyId(), new BigDecimal("486.5"));

        ObjectMapper om = new ObjectMapper();
        System.out.println(om.writeValueAsString(totalView));
        System.out.println(om.writeValueAsString(payment));
    }
}