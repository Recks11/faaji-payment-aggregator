package dev.faaji.streams.events.processor;

import dev.faaji.streams.model.PaymentUpdateEvent;
import dev.faaji.streams.model.TotalView;
import dev.faaji.streams.service.EventProcessor;
import dev.faaji.streams.service.bindings.StreamBindings;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Service;

import java.util.function.Function;

@Service
public class PaymentAggregator implements PaymentStreamProcessor<String, TotalView> {
    private static final Logger LOG = LoggerFactory.getLogger(PaymentAggregator.class);

    private final EventProcessor<KeyValue<String, PaymentUpdateEvent>, KeyValue<String, TotalView>> eventProcessor;

    public PaymentAggregator(EventProcessor<KeyValue<String, PaymentUpdateEvent>, KeyValue<String, TotalView>> eventProcessor) {
        this.eventProcessor = eventProcessor;
    }

    @Override
    @Bean(StreamBindings.INPUT_BINDING)
    public Function<KStream<String, PaymentUpdateEvent>,
            KTable<String, TotalView>> process() {
        return event -> event
                .filter((key, value) -> value.getData() != null && value.getType() != null)
                .map((key, value) -> eventProcessor.process(KeyValue.pair(key, value)))
                .groupBy((key, value) -> String.valueOf(value.partyId()), Grouped.with(null, new JsonSerde<>(TotalView.class)))
                .reduce((v1, v2) -> {
                    var total = v1.total().add(v2.total());
                    return new TotalView(v1.userId(), v2.partyId(), total); // this creates a new object everytime
                }, Materialized.as(StreamBindings.PAYMENT_TOTAL_STATE_STORE));
    }
}
