package dev.faaji.streams.api.v1;

import dev.faaji.streams.model.TotalView;
import dev.faaji.streams.service.bindings.StreamBindings;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("api/v1/payment")
public class PaymentApiEndpoint {

    private final InteractiveQueryService queryService;

    public PaymentApiEndpoint(InteractiveQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping("/{eventId}")
    public Mono<TotalView> getLatestPartyTotal(@PathVariable String eventId) {
        ReadOnlyKeyValueStore<String, TotalView> store = queryService.getQueryableStore(
                StreamBindings.PAYMENT_TOTAL_STATE_STORE, QueryableStoreTypes.keyValueStore());

        return Mono.fromCallable(() -> store.get(eventId))
                .switchIfEmpty(Mono.error(Exceptions.propagate(new ResponseStatusException(HttpStatus.NOT_FOUND, "Event Not Found"))));
    }
}
