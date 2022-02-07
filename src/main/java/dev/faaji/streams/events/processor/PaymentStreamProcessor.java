package dev.faaji.streams.events.processor;

import dev.faaji.streams.model.PaymentUpdateEvent;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.function.Function;

/**
 * Interface to represent processors of the payment stream.
 * The input Key, Value pair is fixed but the destination types must be provided.
 * @param <DK> The destination key type
 * @param <DV> The destination Value type
 */
public interface PaymentStreamProcessor<DK, DV> {


    Function<KStream<String, PaymentUpdateEvent>,
            KTable<DK, DV>> process();
}
