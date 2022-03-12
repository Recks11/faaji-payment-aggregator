package dev.faaji.streams.events.processor;

import dev.faaji.streams.api.v1.domain.UserRegistration;
import dev.faaji.streams.api.v1.response.RoomRecommendationResponse;
import dev.faaji.streams.model.FaajiRoom;
import dev.faaji.streams.util.KeyUtils;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.*;
import java.util.function.Function;

import static dev.faaji.streams.service.bindings.MaterialBinding.USER_ROOM_STORE;
import static dev.faaji.streams.service.bindings.StreamBindings.ROOM_RECOMMENDER;

@Component
public class RoomRecommender {
    private static final Logger LOG = LoggerFactory.getLogger(RoomRecommender.class);
    public static final String RECOMMENDER_TABLE_NAME = "user-rooms";
    private final Serde<RoomRecommendationResponse> roomSerde;
    private final WebClient webClient;

    public RoomRecommender(Serde<RoomRecommendationResponse> roomSerde, WebClient webClient) {
        this.roomSerde = roomSerde;
        this.webClient = webClient;
    }

    @Bean(ROOM_RECOMMENDER)
    public Function<KStream<String, UserRegistration>, KTable<String, RoomRecommendationResponse>> recommendRoom() {
        return userStream -> userStream.map((key, userRegistration) -> {
            LOG.info("recommending room for user %s".formatted(userRegistration.userId()));
            String room = recommendRoom(userRegistration.interests(), getRoomsForEvent());
            String updatedKey = KeyUtils.merge(userRegistration.eventId(), userRegistration.userId());
            return new KeyValue<>(updatedKey, new RoomRecommendationResponse(
                    userRegistration.userId(),
                    userRegistration.eventId(),
                    room
            ));
        }).toTable(Named.as(RECOMMENDER_TABLE_NAME), Materialized.<String, RoomRecommendationResponse, KeyValueStore<Bytes, byte[]>>as(USER_ROOM_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(roomSerde));
    }

    private String recommendRoom(String[] userInterests, FaajiRoom[] rooms) {
        Set<String> userInterest = new HashSet<>(Set.of(userInterests));

        String recommended = "NONE";
        int previousTotal = 0;
        for (FaajiRoom room : rooms) {
            int totalCommon = 0;
            for (String interest : room.interests()) {
                if (userInterest.contains(interest)) totalCommon++;
            }

            if (totalCommon > previousTotal) {
                previousTotal = totalCommon;
                recommended = room.roomId();
            }
        }

        if (recommended.equals("NONE")) {
            int index = new Random().nextInt(rooms.length);
            recommended = rooms[index].roomId();
        }

        return recommended;
    }

    private FaajiRoom[] getRoomsForEvent() {
        return webClient.get()
                .uri("/interests")
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<FaajiRoom[]>() {})
                .onErrorReturn(new FaajiRoom[0])
                .block();
    }
}