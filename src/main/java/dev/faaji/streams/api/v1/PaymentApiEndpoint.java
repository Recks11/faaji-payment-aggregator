package dev.faaji.streams.api.v1;

import dev.faaji.streams.api.v1.domain.User;
import dev.faaji.streams.api.v1.domain.UserMatch;
import dev.faaji.streams.model.TotalView;
import dev.faaji.streams.service.bindings.StreamBindings;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static dev.faaji.streams.service.bindings.MaterialBinding.EVENT_ATTENDEE_STORE;
import static dev.faaji.streams.service.bindings.MaterialBinding.USER_INTEREST_STORE;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;

@Configuration
public class PaymentApiEndpoint {

    private final InteractiveQueryService queryService;

    public PaymentApiEndpoint(InteractiveQueryService queryService) {
        this.queryService = queryService;
    }

    @Bean
    public RouterFunction<ServerResponse> handleRoutes() {
        return route()
                .path("/api/v1", base -> base
                        .GET("/payment/{eventId}", request -> {
                            var eventId = request.pathVariable("eventId");
                            ReadOnlyKeyValueStore<String, TotalView> store = queryService.getQueryableStore(
                                    StreamBindings.PAYMENT_TOTAL_STATE_STORE, QueryableStoreTypes.keyValueStore());
                            Mono<TotalView> event = Mono.fromCallable(() -> store.get(eventId))
                                    .switchIfEmpty(Mono.error(Exceptions.propagate(new ResponseStatusException(HttpStatus.NOT_FOUND, "Event Not Found"))));

                            return ServerResponse.ok()
                                    .body(event, TotalView.class);
                        })
                        .GET("/find/{eventId}", request -> {
                            var eventId = request.pathVariable("eventId");
                            ReadOnlyKeyValueStore<String, List<String>> store = getQueryableStore(EVENT_ATTENDEE_STORE);
                            ReadOnlyKeyValueStore<String, List<String>> interestStore = getQueryableStore(USER_INTEREST_STORE);
                            List<String> data = Optional.ofNullable(store.get(eventId)).orElse(List.of("none"));
                            List<User> users = data.stream().map(userId -> {
                                var in = interestStore.get("%s:%s".formatted(eventId, userId));
                                var ins = in != null ? in.toArray(new String[]{}) : new String[]{};
                                return new User(userId, ins);
                            }).toList();
                            return ServerResponse.ok()
                                    .bodyValue(new EventResponse<>(eventId, users));
                        })
                        .GET("/user/{id}", request -> {
                            var userId = request.pathVariable("id");
                            ReadOnlyKeyValueStore<String, List<String>> store = getQueryableStore(USER_INTEREST_STORE);
                            List<String> data = Optional.ofNullable(store.get(userId)).orElse(List.of("none"));
                            return ServerResponse.ok()
                                    .bodyValue(new EventResponse<>(userId, data));
                        })
                ).route(RequestPredicates.all(), request -> ServerResponse.status(HttpStatus.FORBIDDEN).build()).build();

    }

    private <K, V> ReadOnlyKeyValueStore<K, V> getQueryableStore(String name) {
        return queryService.getQueryableStore(name, QueryableStoreTypes.keyValueStore());
    }
}
