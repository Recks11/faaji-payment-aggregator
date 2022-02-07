package dev.faaji.streams.model;

public class PaymentUpdateEvent implements Event<Payment> {
    private Events type;
    private Payment data;

    public PaymentUpdateEvent() {
    }

    public PaymentUpdateEvent(Events type, Payment data) {
        this.type = type;
        this.data = data;
    }

    @Override
    public Payment getData() {
        return data;
    }

    @Override
    public String getType() {
        return type.getValue();
    }

    public static PaymentUpdateEvent from(PaymentUpdateEvent other) {
        return new PaymentUpdateEvent(other.type, Payment.from(other.data));
    }

    @Override
    public String toString() {
        return "PaymentEvent {" +
                "type=" + type +
                ", data=" + data +
                '}';
    }
} // DEBIT / CREDIT
