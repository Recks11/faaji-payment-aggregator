package dev.faaji.streams.service;

import dev.faaji.streams.api.v1.domain.User;
import dev.faaji.streams.api.v1.domain.UserMatch;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Component
public class UserMatchStreamProcessor {

    private final Map<String, Integer> roundMap = new ConcurrentHashMap<>();
    private final Map<String, SortedSet<UserMatch>> eventMap = new HashMap<>();


    public Map<String, SortedSet<UserMatch>> getMatchSet(String id) {
        return eventMap;
    }

    public List<UserMatch> getRound(List<User> availableUsers, String eventId, int round) {
        List<UserMatch> result = new ArrayList<>();
        while (round > 0) {
            try {
                result = createMatchQueue(availableUsers, eventId);
                round--;
            } catch (Exception e) {
                break;
            }
        }
        return result;
    }

    // get next round
    public List<UserMatch> createMatchQueue(List<User> availableUsers, String eventId) {
        Queue<User> available = new ArrayDeque<>(List.copyOf(availableUsers));
        Set<String> availableIds = List.copyOf(availableUsers)
                .stream().map(User::id)
                .collect(Collectors.toSet());

        List<UserMatch> result = new ArrayList<>();
        while (available.peek() != null) {
            var user = available.poll(); // getting the next user
            var userId = createIdForEvent(user.id(), eventId);
            if (!availableIds.contains(user.id())) continue;

            SortedSet<UserMatch> userMatch = eventMap.get(userId);

            for (UserMatch match : userMatch) {
                var matchId = createIdForEvent(match.userId(), eventId);
                if (availableIds.contains(match.userId())) {
                    availableIds.remove(match.userId());
                    availableIds.remove(user.id());
                    result.add(new UserMatch(user.id(), match.userId(), 0));
                    result.add(new UserMatch(match.userId(), user.id(), 0));
                    userMatch.remove(match);
                    eventMap.replace(userId, userMatch);
                    var otherSet = eventMap.get(matchId).stream()
                            .filter(m -> !m.userId().equals(user.id()))
                            .collect(Collectors.toCollection(TreeSet::new));
                    eventMap.replace(matchId, otherSet);
                    break;
                }
            }
        }

        return result;
    }

    // initialise the pool
    public void setMatchMatrix(List<User> users, String eventId) {
        Map<String, SortedSet<UserMatch>> matchDictionary = new HashMap<>();
        Map<String, Set<String>> matchHistory = new ConcurrentHashMap<>(users.size());
        List<User> males = users.stream().filter(user -> user.gender().equalsIgnoreCase("male")).toList();
        List<User> female = users.stream().filter(user -> user.gender().equalsIgnoreCase("female")).toList();

        for (User user : users) {
            var userId = createIdForEvent(user.id(), eventId);
            Set<String> userMatches = matchHistory.computeIfAbsent(userId, o -> new HashSet<>());
            SortedSet<UserMatch> userDict = matchDictionary.computeIfAbsent(userId, o -> new TreeSet<>());
            var interests = convertArrayTISet(user.interests());
            var gender = user.gender() == null ? "non-binary" : user.gender().toLowerCase(Locale.ROOT);
            var match = switch (gender) {
                case "male" -> female;
                case "female" -> males;
                default -> users;
            };

            for (User other : match) {
                if (user.equals(other)) continue;
                var otherId = createIdForEvent(other.id(), eventId);
                Set<String> otherUserMatches = matchHistory.computeIfAbsent(otherId, o -> new HashSet<>());
                SortedSet<UserMatch> otherDict = matchDictionary.computeIfAbsent(otherId, o -> new TreeSet<>());
                if (otherUserMatches.contains(userId)) continue;
                SortedSet<String> otherInterests = convertArrayTISet(other.interests());
                otherInterests.retainAll(interests);
                int interestInCommon = otherInterests.size();

                var otherUserMatch = new UserMatch(user.id(), other.id(), interestInCommon);
                var userMatch = new UserMatch(other.id(), user.id(), interestInCommon);

                userDict.add(userMatch);
                otherDict.add(otherUserMatch);

                userMatches.add(other.id());
                otherUserMatches.add(user.id());
            }
        }

        this.eventMap.putAll(matchDictionary);
    }

    private static <T> SortedSet<T> convertArrayTISet(T[] data) {
        return new TreeSet<>(List.of(data));
    }

    private static String createIdForEvent(String id, String event) {
        return "%s:%s".formatted(event, id);
    }
}
