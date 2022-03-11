package dev.faaji.streams.events.processor;

import dev.faaji.streams.api.v1.domain.PartyModificationEvent;
import dev.faaji.streams.api.v1.domain.UserRegistration;
import dev.faaji.streams.serialization.ArrayListSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.*;
import java.util.function.Function;

import static dev.faaji.streams.service.bindings.MaterialBinding.EVENT_ATTENDEE_STORE;
import static dev.faaji.streams.service.bindings.MaterialBinding.USER_INTEREST_STORE;
import static dev.faaji.streams.service.bindings.MaterialBinding.EVENT_CREATION_STORE;

@Configuration
public class MatchStreamProcessor {

    private final InteractiveQueryService queryService;

    public MatchStreamProcessor(InteractiveQueryService queryService) {
        this.queryService = queryService;
    }

    // this creates the event
    @Bean("partyinit")
    public Function<KStream<String, PartyModificationEvent>, KTable<String, List<String>>> initializeParty() {
        return partyModificationEvent -> partyModificationEvent
                .map((key, event) -> new KeyValue<>(String.valueOf(event.getData().getEventId()), List.<String>of()))
                .filter((key, value) -> {
                    ReadOnlyKeyValueStore<String, List<String>> store = queryService.getQueryableStore(
                            EVENT_CREATION_STORE, QueryableStoreTypes.keyValueStore());

                    if (store == null) return true;
                    return store.get(key) == null;
                })
                .toTable(Named.as("event-creation-processor"), Materialized.<String, List<String>, KeyValueStore<Bytes, byte[]>>as(EVENT_CREATION_STORE)
                        .withValueSerde(new ArrayListSerde<>())
                        .withKeySerde(Serdes.String()));
    }

    // stores users into the Event Attendee store, indexed by event.
    @Bean("userregister")
    public Function<KStream<String, UserRegistration>, KTable<String, List<String>>> eventUpdateStream() {
        return partyModificationEvent -> partyModificationEvent
                .map((key, event) -> {
                    var gender = event.gender() != null ? event.gender() : "non-binary";
                    return new KeyValue<>(String.valueOf(event.eventId()), List.of("%s:%s".formatted(event.userId(), gender)));
                })
                .groupByKey(Grouped.with(Serdes.String(), new ArrayListSerde<>()))
                .reduce((ev, nextEv) -> {
                    List<String> event = new ArrayList<>(ev);
                    event.addAll(nextEv);
                    return Arrays.asList(Set.copyOf(event).toArray(value -> new String[]{}));
                }, Materialized.<String, List<String>, KeyValueStore<Bytes, byte[]>>as(EVENT_ATTENDEE_STORE)
                        .withValueSerde(new ArrayListSerde<>())
                        .withKeySerde(Serdes.String()));
    }

    @Bean("userinterest")
    public Function<KStream<String, UserRegistration>, KTable<String, List<String>>> userRegistrationStream() {
        return event -> event.map((eventId, registration) -> {
            String scopedUserKey = "%s:%s".formatted(registration.eventId(), registration.userId());
            var inters = convertToList(registration.interests());
            return new KeyValue<>(scopedUserKey, inters);
        }).toTable(Named.as("user-interest-processor"),
                Materialized.<String, List<String>, KeyValueStore<Bytes, byte[]>>as(USER_INTEREST_STORE)
                .withValueSerde(new ArrayListSerde<>())
                .withKeySerde(Serdes.String()));
    }

    private <T> List<T> convertToList(T[] array) {
        return array == null ? List.of() : Arrays.asList(array);
    }

}