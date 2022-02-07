package dev.faaji.streams.api.v1;

import dev.faaji.streams.model.TotalView;
import dev.faaji.streams.service.bindings.StreamBindings;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;

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
                ).route(RequestPredicates.all(), request -> ServerResponse.status(HttpStatus.FORBIDDEN).build()).build();

    }
}
