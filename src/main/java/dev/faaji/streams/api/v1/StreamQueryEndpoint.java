package dev.faaji.streams.api.v1;

import dev.faaji.streams.api.v1.domain.User;
import dev.faaji.streams.api.v1.domain.UserMatch;
import dev.faaji.streams.api.v1.response.ErrorResponse;
import dev.faaji.streams.api.v1.response.RoomRecommendationResponse;
import dev.faaji.streams.model.TotalView;
import dev.faaji.streams.service.UserMatchStreamProcessor;
import dev.faaji.streams.service.bindings.StreamBindings;
import dev.faaji.streams.util.KeyUtils;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static dev.faaji.streams.service.bindings.MaterialBinding.*;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;

@Configuration
public class StreamQueryEndpoint {
    private static final Logger LOG = LoggerFactory.getLogger(StreamQueryEndpoint.class);
    private final InteractiveQueryService queryService;
    private final UserMatchStreamProcessor processor;

    public StreamQueryEndpoint(InteractiveQueryService queryService, UserMatchStreamProcessor processor) {
        this.queryService = queryService;
        this.processor = processor;
    }

    @Bean
    public RouterFunction<ServerResponse> handleRoutes() {
        return route()
                .path("/api/v1", base -> base
                        .GET("/payment/{eventId}", request -> {
                            var eventId = request.pathVariable("eventId");
                            return getStoreMono(StreamBindings.PAYMENT_TOTAL_STATE_STORE)
                                    .flatMap(store -> Mono.justOrEmpty(store.get(eventId)))
                                    .switchIfEmpty(Mono.error(Exceptions.propagate(new ResponseStatusException(HttpStatus.NOT_FOUND, "Event Not Found"))))
                                    .flatMap((event) -> ServerResponse.ok().bodyValue(event));
                        })
                        .GET("/event/{eventId}", request -> {
                            var eventId = request.pathVariable("eventId");
                            var queue = getMatchQueue(eventId);
                            return ServerResponse.ok()
                                    .bodyValue(EventResponse.withMetadata(eventId, queue, Map.of("size", queue.size())));
                        })
                        .GET("/user/{id}", request -> {
                            var userId = request.pathVariable("id");
                            ReadOnlyKeyValueStore<String, List<String>> store = getQueryableStore(USER_INTEREST_STORE);
                            List<String> data = Optional.ofNullable(store.get(userId)).orElse(List.of("none"));
                            return ServerResponse.ok()
                                    .bodyValue(EventResponse.from(userId, data));
                        })
                        .POST("/event/{eventId}/start", request -> {
                            var eventId = request.pathVariable("eventId");
                            var queue = getMatchQueue(eventId);
                            processor.setMatchMatrix(queue, eventId);
                            return ServerResponse.ok().build();
                        })
                        .GET("/event/{eventId}/start/{round}", request -> {
                            var eventId = request.pathVariable("eventId");
                            var round = Integer.parseInt(request.pathVariable("round"));
                            List<UserMatch> matchQueue = processor.getRound(getMatchQueue(eventId), eventId, round);
                            return ServerResponse.ok().bodyValue(matchQueue);
                        })
                        .GET("/rooms/{eventId}/", request -> {
                            var eventId = request.pathVariable("eventId");
                            var userOp = request.queryParam("userId");

                            if (userOp.isEmpty()) return ServerResponse.badRequest().build();
                            String key = KeyUtils.merge(eventId, userOp.get());
                            var mono = getStoreMono(USER_ROOM_STORE)
                                    .flatMap(store -> Mono.justOrEmpty(store.get(key)))
                                    .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "user room not found")));

                            return ServerResponse.ok().body(mono, RoomRecommendationResponse.class);
                        })
                ).onError(ResponseStatusException.class, this::handleError)
                .route(RequestPredicates.all(), request -> ServerResponse.status(HttpStatus.FORBIDDEN).build())
                .build();

    }

    private Mono<ServerResponse> handleError(ResponseStatusException exception, ServerRequest request) {
        var errResponse = new ErrorResponse(exception.getRawStatusCode(), exception.getReason(), request.uri().toString());
        LOG.error("an error occurred while handling request: %s".formatted(errResponse));
        if (exception.getCause() != null) LOG.error(exception.getReason(), exception);

        return ServerResponse.status(exception.getStatus())
                .bodyValue(errResponse);
    }

    private <K, V> ReadOnlyKeyValueStore<K, V> getQueryableStore(String name) {
        try {
            return queryService.getQueryableStore(name, QueryableStoreTypes.keyValueStore());
        } catch (InvalidStateStoreException ex) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "The requested resource is being prepared");
        }
    }

    private <K, V> Mono<ReadOnlyKeyValueStore<K, V>> getStoreMono(String name) {
        return Mono.create(sink -> {
            try {
                var store = this.<K, V>getQueryableStore(name);
                sink.success(store);
            } catch (Exception ex) {
                LOG.error("Failed to resolve store", ex);
                sink.error(ex);
            }
        });
    }

    private List<User> getMatchQueue(String eventId) {
        ReadOnlyKeyValueStore<String, List<String>> store = getQueryableStore(EVENT_ATTENDEE_STORE);
        ReadOnlyKeyValueStore<String, List<String>> interestStore = getQueryableStore(USER_INTEREST_STORE);
        List<String> data = Optional.ofNullable(store.get(eventId)).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "data not found"));
        return data.stream().map(dt -> {
            var userData = dt.split(":");
            var in = interestStore.get("%s:%s".formatted(eventId, userData[0]));
            var ins = in != null ? in.toArray(new String[]{}) : new String[]{};
            return new User(userData[0], userData.length == 2 ? userData[1] : "non-binary", ins);
        }).toList();
    }
}
