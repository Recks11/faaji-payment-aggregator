package dev.faaji.streams.service.bindings;

public interface StreamBindings {
    String INPUT_BINDING = "input";
    String INPUT_BINDING_IN_DESTINATION = "FaajiEventPayments";
    String INPUT_BINDING_OUT_DESTINATION = "FaajiPaymentTotals";
    String PAYMENT_TOTAL_STATE_STORE = "payment-totals";


    String ROOM_RECOMMENDER = "room-recommender";
}
